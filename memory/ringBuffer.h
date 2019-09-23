#pragma once
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

#include "util/barrier.h"
#include "util/likely.h"
#include <atomic>
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
#include <set>
class ringBuffer {
private:
	static constexpr uint64_t DEFAULT_NODE_SIZE = 32 * 1024 * 1024;
	static constexpr uint64_t NODE_LOCK_MASK = 0x8000000000000000ULL;
	static constexpr uint64_t NODE_USED_OUT_MASK = 0x4000000000000000ULL;
	static constexpr uint64_t NODE_FREED_MASK = 0x2000000000000000ULL;
	static constexpr uint64_t NODE_LOW_MASK = ~(NODE_LOCK_MASK|NODE_USED_OUT_MASK|NODE_FREED_MASK);
	struct ringBufferNode {
		std::atomic<ringBufferNode*> next;
		void* startPos;
		std::atomic<uint64_t> begin;
		std::atomic<uint64_t> end;
		uint64_t size;
		ringBufferNode(uint64_t size = DEFAULT_NODE_SIZE-2*sizeof(uint64_t))
		{
			next.store(nullptr,std::memory_order_relaxed);
			this->size = (size+ 2*sizeof(uint64_t)) > DEFAULT_NODE_SIZE ? (size + 2*sizeof(uint64_t)) : DEFAULT_NODE_SIZE;
			startPos = malloc(this->size+sizeof(uint64_t));
			begin.store(ALIGN((uint64_t)startPos, 8) - (uint64_t)startPos, std::memory_order_relaxed);
			end.store(begin.load(std::memory_order_relaxed), std::memory_order_relaxed);
			*(uint64_t*)(((char*)startPos)+begin.load(std::memory_order_relaxed)) = 0ul;
		}
		~ringBufferNode()
		{
			::free(startPos);
		}
		inline void reset()
		{
			begin.store(ALIGN((uint64_t)startPos, 8) - (uint64_t)startPos, std::memory_order_relaxed);
			end.store(begin.load(std::memory_order_relaxed), std::memory_order_relaxed);
		}
	};
	std::atomic<ringBufferNode*> m_head;
	std::atomic<ringBufferNode*> m_tail;
public:
	ringBuffer()
	{
		ringBufferNode* node = new  ringBufferNode();
		node->next.store(node,std::memory_order_relaxed);
		m_head.store(node, std::memory_order_relaxed);
		m_tail.store(node, std::memory_order_relaxed);
	}
	~ringBuffer()
	{
		ringBufferNode* node = m_head.load(std::memory_order_relaxed);
		do {
			ringBufferNode* next = node->next.load(std::memory_order_relaxed);
			delete node;
			node = next;
		} while (node != m_head.load(std::memory_order_relaxed));
	}
	void* alloc(size_t size) 
	{
		ringBufferNode* head;
		void* mem;
		uint64_t end;
		size = ALIGN(size,8);
		do {
			head = m_head.load(std::memory_order_relaxed);
			if (unlikely(((uint64_t)head) & NODE_LOCK_MASK))
			{
				barrier;
				continue;
			}
			end = head->end.load(std::memory_order_relaxed);//aba risk
			if (unlikely(end & NODE_LOCK_MASK))//this node has used out by other thread,and a new head node has been created
			{
				barrier;
				continue;
			}
			if (likely(end + size + sizeof(uint64_t) < head->size))
			{
				mem = ((char*)head->startPos) + end;
				if (likely(head->end.compare_exchange_weak(end, end+size+sizeof(uint64_t), std::memory_order_relaxed, std::memory_order_relaxed)))
				{
					*(uint64_t*)mem = size;
					barrier;
					return ((int8_t*)mem) + sizeof(uint64_t);
				}
			}
			else
			{
				if (unlikely(!head->end.compare_exchange_weak(end, end | NODE_LOCK_MASK, std::memory_order_relaxed, std::memory_order_relaxed)))
					continue;
				barrier;
				ringBufferNode* node = head->next.load(std::memory_order_relaxed);
				if (node == (ringBufferNode*)(((uint64_t)m_tail.load(std::memory_order_relaxed)) & NODE_LOW_MASK) )
				{
					ringBufferNode * newNode = new ringBufferNode(size);
					newNode->next.store(head->next.load(std::memory_order_relaxed), std::memory_order_relaxed);
					if (likely(head->next.compare_exchange_strong(node, newNode, std::memory_order_relaxed, std::memory_order_relaxed)))
					{
						if (unlikely(!m_head.compare_exchange_strong(head, newNode, std::memory_order_relaxed, std::memory_order_relaxed)))//set this node used out
						{
							*(uint64_t*)((char*)newNode->startPos+newNode->begin.load(std::memory_order_relaxed)) = NODE_FREED_MASK;
							newNode->end.store(newNode->end.load(std::memory_order_relaxed) | NODE_USED_OUT_MASK, std::memory_order_relaxed);
						}
					}
					else
					{
						delete newNode;
					}
				}
				else
				{
					while (node->next.load(std::memory_order_relaxed) != (ringBufferNode*)(((uint64_t)m_tail.load(std::memory_order_relaxed)) & NODE_LOW_MASK))
					{
						ringBufferNode* tmp = node->next.load(std::memory_order_relaxed);
						if (m_head.compare_exchange_strong(head, (ringBufferNode*)(((uint64_t)head) | NODE_LOCK_MASK)))
						{
							node->next.store(tmp->next.load(std::memory_order_relaxed), std::memory_order_relaxed);
							delete tmp;
						}
						else
							break;
					}
					m_head.store(node,std::memory_order_relaxed);
				}
				*(uint64_t*)((char*)head->startPos+end) = NODE_FREED_MASK;
				head->end.store((end&NODE_LOW_MASK)|NODE_USED_OUT_MASK);
			}
		} while (true);
	}
	//thread not safe
	void freeMem(void* mem)
	{
		mem = (void*)(((int8_t*)mem) - sizeof(uint64_t));
		*(uint64_t*)mem = (*(uint64_t*)mem) | NODE_FREED_MASK;
		ringBufferNode* node = m_tail.load(std::memory_order_relaxed);
		if (unlikely(*(uint64_t*)node) & NODE_LOCK_MASK)
			return;
		mem = (void*)(((int8_t*)node->startPos) + node->begin.load(std::memory_order_relaxed));//aba risk
		while ((*(uint64_t*)mem) & NODE_FREED_MASK)
		{
			if (!m_tail.compare_exchange_strong(node, (ringBufferNode*)(((uint64_t)node) | NODE_LOCK_MASK)))
				return;
			node = (ringBufferNode*)(((uint64_t)node) & NODE_LOW_MASK);
			do{
				if (mem != ((char*)node->startPos) + node->begin.load(std::memory_order_relaxed))
				{
					m_tail.store(node, std::memory_order_relaxed);
					return;
				}
				assert((char*)mem >=  (char*)node->startPos);
				if((uint32_t)((char*)mem - (char*)node->startPos) == (node->end.load(std::memory_order_relaxed)&NODE_LOW_MASK))
				{
					if (node->end.load(std::memory_order_relaxed) & NODE_USED_OUT_MASK)
					{
						node->reset();
						ringBufferNode* next = node->next.load(std::memory_order_relaxed);
						m_tail.store(next, std::memory_order_relaxed);
						node = next;
						mem = (void*)(((int8_t*)node->startPos) + node->begin.load(std::memory_order_relaxed));
						break;
					}
					else
					{
						m_tail.store(node, std::memory_order_relaxed);
						return;
					}
				}
				if(((*(uint64_t*)mem) & NODE_FREED_MASK)==0)
				{
					m_tail.store(node, std::memory_order_relaxed);
					return;
				}
				mem = (char*)mem+((*(uint64_t*)mem) & NODE_LOW_MASK) + sizeof(uint64_t);
				node->begin.store((char*)mem - (char*)node->startPos, std::memory_order_relaxed);
			}while (1);
			m_tail.store(node, std::memory_order_relaxed);
		}
	}
};
