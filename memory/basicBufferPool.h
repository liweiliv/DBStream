#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "../util/nonBlockStack.h"
#include "../util/spinlock.h"
#include "../util/dualLinkList.h"
#include "../util/linkList.h"
#include "../util/threadLocal.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
class basicBufferPool
{
private:
	struct block;
	struct basicBlock {
		block * _block;
		basicBlock *next;
		char mem[1];
	};
	struct nonBlockStackWrap {
		nonBlockStackWrap *next;
		nonBlockStack<basicBlock> blocks;
	};
	struct block {
		block * next;//for nonBlockLinkList
		basicBufferPool * pool;
		dualLinkListNode dlNode;
		char * startPos;
		char * alignedStartPos;
		spinlock lock;
		nonBlockStack<basicBlock> basicBlocks;
		uint64_t basicBlockSize;
		uint32_t basicBlockCount;
		uint64_t blockSize;
		block(uint64_t _basicBlockSize, uint32_t _basicBlockCount, uint64_t _blockSize):basicBlockSize(_basicBlockSize+sizeof(basicBlock)-1),
			basicBlockCount(_basicBlockCount), blockSize(_blockSize)
		{
			startPos = (char*)malloc(blockSize + 8);
			alignedStartPos = (char*)ALIGN((uint64_t)startPos, 8);
			char * p = alignedStartPos;
			while (p < alignedStartPos + blockSize)
			{
				((basicBlock*)p)->_block = this;
				basicBlocks.pushFast((basicBlock*)p);
			}
		}
		~block()
		{
			free(startPos);
		}
		inline basicBlock* getBasicBlock()
		{
			return basicBlocks.popAll();
		}
		inline void freeBasicBlock(basicBlock* block)
		{
			basicBlocks.push(block);
		}
	};
	/*local cache*/
	threadLocal<nonBlockStack<basicBlock>> m_cache1;
	threadLocal<nonBlockStack<basicBlock>> m_cache2;
	/*global cache*/
	linkList<nonBlockStackWrap> m_globalCache;
	dualLinkList m_activeBlocks;
	dualLinkList m_freeBlocks;
	dualLinkList m_usedOutBlocks;
	std::mutex m_lock;
	uint64_t basicBlockSize;
	uint32_t basicBlockCount;
	uint64_t blockSize;
	uint64_t maxMem;
	uint64_t maxBlocks;
	std::atomic<int32_t> blockCount;
	std::atomic<int32_t> starvation;
public:
	basicBufferPool(uint64_t _basicBlockSize, uint64_t _maxMem) :basicBlockSize(_basicBlockSize), maxMem(_maxMem), blockCount(0), starvation(0)
	{
		if (basicBlockSize <= 4096)
		{
			blockSize = 1024 * 1024;
			basicBlockCount = blockSize / basicBlockSize;
		}
		else if (basicBlockSize <= 64 * 1024)
		{
			basicBlockCount = 128;
			blockSize = basicBlockCount * basicBlockSize;
		}
		else if (basicBlockSize <= 512 * 1024)
		{
			basicBlockCount = 48;
			blockSize = basicBlockCount * basicBlockSize;
		}
		maxBlocks = maxMem / blockSize;
	}
	~basicBufferPool()
	{
		dualLinkListNode * n;
		while (nullptr != (n = m_freeBlocks.popLast()))
		{
			block * b = container_of(n, block, dlNode);
			delete b;
		}	
		while (nullptr != (n = m_activeBlocks.popLast()))
		{
			block * b = container_of(n, block, dlNode);
			delete b;
		}
		while (nullptr != (n = m_usedOutBlocks.popLast()))
		{
			block * b = container_of(n, block, dlNode);
			delete b;
		}
	}
private:
	void fillCache(basicBlock* basic)
	{
		nonBlockStack<basicBlock>* cache = m_cache1.get();
		while (basic != nullptr && (cache->push(basic), cache->m_count.load(std::memory_order_relaxed) < 32))
			basic = basic->next;
		cache = m_cache2.get();
		while (basic != nullptr && (cache->push(basic), cache->m_count.load(std::memory_order_relaxed) < 32))
			basic = basic->next;
		if (basic != nullptr)
		{
			nonBlockStackWrap * wrap = new nonBlockStackWrap;
			wrap->blocks.push(basic);
			m_globalCache.push(wrap);
		}
	}
	inline basicBlock *getMemFromExistBlock(dualLinkList& list)
	{
		dualLinkListNode * n = list.popLastAndHandleLock();
		if (n == nullptr)
			return nullptr;
		block * b = container_of(n, block, dlNode);
		if (b != nullptr)
		{
			basicBlock* basic = b->getBasicBlock();
			assert(basic != nullptr);
			if (basic->next != nullptr)
				fillCache(basic->next);
			m_usedOutBlocks.insertForHandleLock(&b->dlNode);
			return basic;
		}
		else
			return nullptr;
	}
	void cleanCache(nonBlockStack<basicBlock> *cache)
	{
		basicBlock * basic = cache->popAll();
		nonBlockStackWrap * wrap = new nonBlockStackWrap;
		wrap->blocks.push(basic);
		m_globalCache.push(wrap);
		while (m_globalCache.getCount() > 5)
		{
			if (!m_lock.try_lock())
				return;
			wrap = m_globalCache.pop();
			if (wrap == nullptr)
			{
				m_lock.unlock();
				return;
			}
			while (nullptr != (basic = wrap->blocks.pop()))
			{
				block *b = basic->_block;
				b->dlNode.lock.lock();
				b->basicBlocks.push(basic);
				if (b->basicBlocks.m_count.load(std::memory_order_release) == 1)//used out
				{
					b->dlNode.lock.unlock();
					m_usedOutBlocks.eraseWithHandleLock(&b->dlNode);
					m_activeBlocks.insertForHandleLock(&b->dlNode);
				}
				else if (b->basicBlocks.m_count.load(std::memory_order_release) == (int32_t)basicBlockCount)//no use
				{
					while (!m_activeBlocks.tryEraseForHandleLock(&b->dlNode));
					m_freeBlocks.insertForHandleLock(&b->dlNode);
					while (m_freeBlocks.count.load(std::memory_order_relaxed) > 3) {
						dualLinkListNode *n = m_freeBlocks.popLast();
						if (n != nullptr)
						{
							b = container_of(n, block, dlNode);
							delete b;
						}
						else
							break;
					}
				}
				else
				{
					b->dlNode.lock.unlock();
				}
			}
		}
	}
public:
	inline void* alloc()
	{
		basicBlock * basic;
		if (nullptr != (basic = m_cache1.get()->popFast()))
			return &basic->mem[0];
		if (nullptr != (basic = m_cache2.get()->popFast()))
			return &basic->mem[0];
		bool isStarvation = false;
		do
		{
			if ((basic = getMemFromExistBlock(m_activeBlocks)) != nullptr || (basic = getMemFromExistBlock(m_freeBlocks)) != nullptr)
			{
				if (isStarvation)
					starvation--;
				return basic;
			}
			if (blockCount.load(std::memory_order_relaxed) >= (int32_t)maxBlocks)
			{
				if (!isStarvation)
				{
					starvation++;
					isStarvation = true;
				}
				std::this_thread::sleep_for(std::chrono::nanoseconds(100));
			}
			else
			{
				block * b = new block(basicBlockSize, basicBlockCount, blockSize);
				b->pool = this;
				basic = b->getBasicBlock();
				fillCache(basic->next);
				assert(basic != nullptr);
				m_usedOutBlocks.insert(&b->dlNode);
				if (isStarvation)
					starvation--;
				return basic;
			}
		} while (1);

	}

	inline void freeMem(void *_block)
	{
		basicBlock * basic = (basicBlock*)(((char*)_block) - offsetof(basicBlock, mem));
		nonBlockStack<basicBlock> * localCache = m_cache2.get();
		if (localCache->m_count.load(std::memory_order_relaxed) < (int32_t)basicBlockCount * 2)
		{
			localCache->push(basic);
			goto END;
		}
		localCache = m_cache1.get();
		if (localCache->m_count.load(std::memory_order_relaxed) < (int32_t)basicBlockCount * 2)
		{
			localCache->push(basic);
			goto END;
		}
		cleanCache(localCache);
		localCache->push(basic);
		return;
	END:
		if (starvation.load(std::memory_order_relaxed) > 0)
		{
			if (m_cache1.get()->m_count.load(std::memory_order_relaxed)==0)
				cleanCache(m_cache2.get());
			else
				cleanCache(m_cache1.get());
		}
	}
	static inline void free(void * _block)
	{
		basicBlock * basic = (basicBlock*)(((char*)_block) - offsetof(basicBlock, mem));
		basic->_block->pool->freeMem(_block);
	}
};
