#pragma once
#include "channel.h"
#include "stdint.h"
#include "thread/barrier.h"
#include "memory/bufferPool.h"
#include <mutex>
#include <atomic>
#include <stdio.h>
namespace BUS {
	class memChannel :public channel {
	private:
		enum class nodeStatus {
			FREE,
			APPENDING,
			FULL
		};
		struct memNode {
			int id;
			uint32_t volumn;
			uint32_t head;
			uint32_t tail;
			nodeStatus status;
			memNode* next;
			char buf[1];
		};
		memNode* m_head;
		memNode* m_tail;
		std::atomic<int> m_nodeCount;
		std::mutex m_condLock;
		std::condition_variable m_cond;
		bufferPool* m_pool;
		constexpr static uint32_t memNodeSize = 1024 * 32;
		constexpr static uint32_t maxNodeCountPerChannel = 8;
	private:
		inline memNode* allocNode()
		{
			memNode* node;
			if (m_pool == nullptr)
				node = (memNode*)malloc(sizeof(memNode) + memNodeSize - 1);
			else
				node = (memNode*)m_pool->alloc(sizeof(memNode) + memNodeSize - 1);
			node->head = node->tail = 0;
			node->next = nullptr;
			node->status = nodeStatus::APPENDING;
			node->id = m_nodeCount.load(std::memory_order_relaxed);
			return node;
		}
		inline memNode* tryAddNode()
		{
			if (m_nodeCount.load(std::memory_order_relaxed) < maxNodeCountPerChannel)
			{
				m_nodeCount.fetch_add(1, std::memory_order_relaxed);
				return allocNode();
			}
			else
				return nullptr;
		}
		inline void notify()
		{
			std::unique_lock <std::mutex> lock(m_condLock);
			m_cond.notify_all();
		}
		inline void wait()
		{
			std::unique_lock <std::mutex> lock(m_condLock);
			m_cond.wait_for(lock, std::chrono::microseconds(100));
		}
		inline bool updateHeadNode(int& outtimeMs)
		{
			do {
				if (m_head->next->status != nodeStatus::FREE)
				{
					memNode* next = tryAddNode();
					if (next == nullptr)
					{
						if (outtimeMs <= 0)
							break;
						outtimeMs -= 100;
						wait();
					}
					else
					{
						next->next = m_head->next;
						memNode* head = m_head;
						m_head->next = next;
						wmb();
						m_head = next;
						head->status = nodeStatus::FULL;
						return true;
					}
				}
				else
				{
					memNode* head = m_head;
					m_head = m_head->next;
					m_head->status = nodeStatus::APPENDING;
					wmb();
					head->status = nodeStatus::FULL;
					return true;
				}
			} while (true);
			return false;
		}
		inline void tryAttachToNextNode()
		{
			assert(m_tail->status != nodeStatus::FREE);
			rmb();
			if (m_tail->tail == m_tail->head)
			{
				m_tail->head = m_tail->tail = 0;
				wmb();
				m_tail->status = nodeStatus::FREE;
				m_tail = m_tail->next;
				notify();
			}
		}
	public:
		memChannel(bufferPool* pool = nullptr) :m_pool(pool), m_nodeCount(1)
		{
			m_head = allocNode();
			m_head->next = m_head;
			m_tail = m_head;
		}
		~memChannel()
		{
			for (memNode* node = m_head;;)
			{
				memNode* next = node->next;
				if (m_pool != nullptr)
					m_pool->free(node);
				else
					::free(node);
				if (node == m_tail)
					break;
				else
					node = next;
			}
		}

		int32_t send(const char* data, uint32_t size, int outtimeMs)
		{
			int32_t sendSize = 0, nSize;
			do {
				if ((nSize = memNodeSize - m_head->head) > size)
					nSize = size;
				memcpy(&m_head->buf[m_head->head], data + sendSize, nSize);
				sendSize += nSize;
				wmb();
				m_head->head += nSize;
				notify();
				if (likely((size -= nSize) == 0))
					break;
				if (!updateHeadNode(outtimeMs))
					break;
			} while (true);
			return sendSize;
		}
		int32_t recv(char* data, uint32_t size, int outtimeMs)
		{
			int32_t recvSize = 0, nSize;
			do {
				if ((nSize = m_tail->head - m_tail->tail) > size)
					nSize = size;
				rmb();
				memcpy(data + recvSize, &m_tail->buf[m_tail->tail], nSize);
				recvSize += nSize;
				if ((m_tail->tail += nSize) == m_tail->head)
				{
					if (m_tail->status == nodeStatus::FULL)
						tryAttachToNextNode();
				}

				if (likely(0 == (size -= nSize)))
					break;

				if (m_tail->head == m_tail->tail)
				{
					if (m_tail->status == nodeStatus::APPENDING)
					{
						if (outtimeMs > 0)
						{
							outtimeMs -= 100;
							wait();
						}
						else
							break;
					}
					else
					{
						tryAttachToNextNode();
					}
				}
			} while (true);
			return recvSize;
		}

	};
}