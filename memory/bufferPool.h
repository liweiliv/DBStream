#pragma once
#include<stdint.h>
#include<stdlib.h>
#include "basicBufferPool.h"
#include <atomic>
static constexpr uint32_t pageSizeList[] = { 128,256,512,4*1024,8*1024, 12*1024,16*1024,32*1024,64*1024,128*1024,512*1024,(512*1024)+((512*1024)/255)+16};
class bufferPool {
private:
	basicBufferPool *m_pools[sizeof(pageSizeList) / sizeof(uint32_t)];
	std::atomic_int m_ref;
public:
	bufferPool():m_ref(0)
	{
		for (uint8_t i = 0; i < sizeof(pageSizeList) / sizeof(uint32_t); i++)
		{
			if(pageSizeList[i]<4096)
				m_pools[i] = new basicBufferPool(pageSizeList[i], pageSizeList[i] * 1024 * 64);
			else if (pageSizeList[i] < 4096*16)
				m_pools[i] = new basicBufferPool(pageSizeList[i], pageSizeList[i] * 1024 * 16);
			else
				m_pools[i] = new basicBufferPool(pageSizeList[i], 1024ull*1024ull*1024ull*4ull);
		}
	}
	~bufferPool()
	{
		for (uint8_t i = 0; i < sizeof(pageSizeList) / sizeof(uint32_t); i++)
			delete m_pools[i];
	}
	static inline uint8_t calculateLevel(uint64_t size)
	{
		for (uint8_t i = 0; i < sizeof(pageSizeList) / sizeof(uint32_t); i++)
		{
			if (pageSizeList[i] >= size)
				return i;
		}
		return sizeof(pageSizeList) / sizeof(uint32_t);
	}
	inline uint64_t maxSize()
	{
		return pageSizeList[sizeof(pageSizeList) / sizeof(uint32_t) - 1];
	}
	inline void * allocByLevel(uint8_t level)
	{
		if (level >= sizeof(pageSizeList) / sizeof(uint32_t))
			return nullptr;
		return m_pools[level]->alloc();
	}
	inline void * alloc(uint64_t size)
	{
		uint8_t level = calculateLevel(size);
		return allocByLevel(level);
	}
	static inline void free(void* data)
	{
		basicBufferPool::free(data);
	}

};
