#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "../util/nonBlockStack.h"
#include "../util/spinlock.h"
#include "../util/dualLinkList.h"
#include "../util/linkList.h"
#include "../util/threadLocal.h"
#define getCache(c,tc) if(unlikely(nullptr==(((c)=(tc).get())))){(tc).set((c)=new cache(this));}
#define getCacheWithDeclare(c,tc) cache *c ;getCache(c,tc)
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
	struct cache {
		basicBufferPool* pool;
		nonBlockStack<basicBlock> caches;
		cache(basicBufferPool* pool) :pool(pool) {}
		~cache()
		{
			pool->cleanCache(this);
		}
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
		block(uint64_t _basicBlockSize, uint32_t _basicBlockCount, uint64_t _blockSize);
		~block();
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
	threadLocal<cache> m_cache1;
	threadLocal<cache> m_cache2;
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
	basicBufferPool(uint64_t _basicBlockSize, uint64_t _maxMem);
	~basicBufferPool();
private:
	void fillCache(basicBlock* basic);
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
	void cleanCache(cache* c);
public:
	inline void* alloc()
	{
		basicBlock * basic;
		getCacheWithDeclare(c, m_cache1);
		if (nullptr != (basic = c->caches.popFast()))
			return &basic->mem[0];
		getCache(c, m_cache2);
		if (nullptr != (basic = c->caches.popFast()))
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
	inline uint64_t memUsed()
	{
		return (blockCount.load(std::memory_order_relaxed)) * blockSize;
	}
	inline void freeMem(void *_block)
	{
		basicBlock * basic = (basicBlock*)(((char*)_block) - offsetof(basicBlock, mem));
		getCacheWithDeclare(localCache, m_cache2);
		if (localCache->caches.m_count.load(std::memory_order_relaxed) < (int32_t)basicBlockCount * 2)
		{
			localCache->caches.push(basic);
			goto END;
		}
		getCache(localCache, m_cache1);
		if (localCache->caches.m_count.load(std::memory_order_relaxed) < (int32_t)basicBlockCount * 2)
		{
			localCache->caches.push(basic);
			goto END;
		}
		cleanCache(localCache);
		localCache->caches.push(basic);
		return;
	END:
		if (starvation.load(std::memory_order_relaxed) > 0)
		{
			if (m_cache1.get()->caches.m_count.load(std::memory_order_relaxed)==0)
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

