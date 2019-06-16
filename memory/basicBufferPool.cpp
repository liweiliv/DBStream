#include "basicBufferPool.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
basicBufferPool::block::block(basicBufferPool * pool,uint64_t _basicBlockSize, uint32_t _basicBlockCount, uint64_t _blockSize) :next(nullptr),pool(pool),basicBlockSize(_basicBlockSize + sizeof(basicBlock) - 1),
	basicBlockCount(_basicBlockCount), blockSize(_blockSize)
{
	startPos = (char*)malloc(blockSize + 8);
	alignedStartPos = (char*)ALIGN((uint64_t)startPos, 8);
	char* p = alignedStartPos;
	while (p < alignedStartPos + blockSize)
	{
		basicBlock * b = (basicBlock*)malloc(basicBlockSize);
		b->_block = this;
		b->next = nullptr;
		((basicBlock*)p)->_block = this;
		((basicBlock*)p)->next = nullptr;
		//basicBlocks.pushFast((basicBlock*)p);
		basicBlocks.pushFast(b);
		printf("new %lx in %lx \n",b,this);
		p = (char*)ALIGN(((uint64_t)p)+basicBlockSize,8);
	}
}
basicBufferPool::block::~block()
{
	::free(startPos);
}
basicBufferPool::basicBufferPool(uint64_t _basicBlockSize, uint64_t _maxMem) :basicBlockSize(_basicBlockSize), maxMem(_maxMem), blockCount(0), starvation(0)
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
basicBufferPool::~basicBufferPool()
{
	dualLinkListNode* n;
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
void basicBufferPool::fillCache(basicBlock* basic)
{
	basicBlock * i = basic;
	while(i!=nullptr)
	{
		printf("fill cache :%lx\n",i);
		i=i->next;
	}
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
void basicBufferPool::cleanCache(cache * c)
{
	basicBlock* basic = c->caches.popAll();
	nonBlockStackWrap* wrap = new nonBlockStackWrap;
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
	}
}

