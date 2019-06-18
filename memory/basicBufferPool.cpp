#include "basicBufferPool.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
DLL_EXPORT basicBufferPool::block::block(basicBufferPool * pool,uint64_t _basicBlockSize, uint32_t _basicBlockCount, uint64_t _blockSize) :next(nullptr),pool(pool),basicBlockSize(_basicBlockSize + sizeof(basicBlock) - 1),
	basicBlockCount(_basicBlockCount), blockSize(_blockSize)
{
	startPos = (char*)malloc(blockSize + 8);
	alignedStartPos = (char*)ALIGN((uint64_t)startPos, 8);
	char* p = alignedStartPos;
	while (p+basicBlockSize < startPos + blockSize)
	{
		basicBlock * b = (basicBlock*)p;
		b->_block = this;
		b->next = nullptr;
		basicBlocks.pushFast(b);
		p += basicBlockSize;
		p = (char*)ALIGN((uint64_t)p,8);
	}
}
DLL_EXPORT basicBufferPool::block::~block()
{
	::free(startPos);
}
DLL_EXPORT basicBufferPool::basicBufferPool(uint64_t _basicBlockSize, uint64_t _maxMem) :basicBlockSize(_basicBlockSize+sizeof(basicBlock)-1), maxMem(_maxMem), blockCount(0), starvation(0)
{
	if (basicBlockSize <= 4096)
	{
		blockSize = 1024;
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
DLL_EXPORT basicBufferPool::~basicBufferPool()
{
	dualLinkListNode* n;
	do {
		nonBlockStackWrap* wrap = m_globalCache.pop();
		if (wrap == nullptr)
			break;
		delete wrap;
	} while (true);
	while (nullptr != (n = m_freeBlocks.popLast()))
	{
		block* b = container_of(n, block, dlNode);
		delete b;
	}
	while (nullptr != (n = m_activeBlocks.popLast()))
	{
		block* b = container_of(n, block, dlNode);
		delete b;
	}
	while (nullptr != (n = m_usedOutBlocks.popLast()))
	{
		block* b = container_of(n, block, dlNode);
		delete b;
	}
}
DLL_EXPORT void basicBufferPool::fillCache(basicBlock* basic)
{
	if (basic != nullptr)
	{
		basic = m_cache1.get()->caches.pushFastUtilCount(basic, 32);
	}
	if (basic != nullptr)
	{
		basic = m_cache2.get()->caches.pushFastUtilCount(basic, 32);
	}
	if (basic != nullptr)
	{
		nonBlockStackWrap* wrap = new nonBlockStackWrap;
		wrap->blocks.pushFast(basic);
		m_globalCache.push(wrap);
	}
}
DLL_EXPORT void basicBufferPool::cleanCache(cache * c)
{
	basicBlock* basic = c->caches.popAll();
	if(basic == nullptr)
		return ;
	nonBlockStackWrap* wrap = new nonBlockStackWrap;
	wrap->blocks.push(basic);
	m_globalCache.push(wrap);
	while (m_globalCache.getCount() > 5)
	{
		if (!m_lock.try_lock())
			return;
		wrap = m_globalCache.pop();
		m_lock.unlock();
		if (wrap == nullptr)
			return;
		while (nullptr != (basic = wrap->blocks.popFast()))
		{
			block* b = basic->_block;
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
				while (m_freeBlocks.count.load(std::memory_order_relaxed) > 3)
				{
					dualLinkListNode* n = m_freeBlocks.popLast();
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
		delete wrap;
	}
}
DLL_EXPORT void * basicBufferPool::allocNewMem()
{
	basicBlock * basic;
	do {
		nonBlockStackWrap* wrap = m_globalCache.pop();
		if (wrap == nullptr)
			break;
		basic = wrap->blocks.popAllFast();
		delete wrap;
		if (basic != nullptr)
		{
			fillCache(basic->next);
			basic->next = nullptr;
			return &basic->mem[0];
		}
	} while (true);
	bool isStarvation = false;
	do
	{
		if ((basic = getMemFromExistBlock(m_activeBlocks)) != nullptr || (basic = getMemFromExistBlock(m_freeBlocks)) != nullptr)
		{
			if (isStarvation)
				starvation--;
			return &basic->mem[0];
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
			block * b = new block(this,basicBlockSize, basicBlockCount, blockSize);
			b->pool = this;
			basic = b->getBasicBlock();
			fillCache(basic->next);
			basic->next = nullptr;
			assert(basic != nullptr);
			m_usedOutBlocks.insert(&b->dlNode);
			if (isStarvation)
				starvation--;
			return &basic->mem[0];
		}
	} while (1);
}
