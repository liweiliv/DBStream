#pragma once
#include <stdint.h>
#include "memory/bufferPool.h"
#include "thread/threadLocal.h"
#include "util/ref.h"
#include "util/barrier.h"
namespace DATABASE {
#pragma pack(1)
	struct page {
		uint32_t pageId;
		uint32_t pageUsedSize;
		uint32_t pageSize;
		uint32_t crc;
		uint64_t lastAccessTime;
		::ref _ref;
		char* pageData;

		inline void use()
		{
			for (uint8_t i = 0; i < 10; i++)
			{
				if (_ref.use() > 0)
					return;
			}
			for (; !_ref.use(); std::this_thread::sleep_for(std::chrono::nanoseconds(100)));
		}
		inline void unuse()
		{
			if (_ref.unuse() < 0)
			{
				if (likely(pageData != nullptr))
				{
					char* data = pageData;
					pageData = nullptr;
					_ref.reset();
					barrier;
					basicBufferPool::free(data);
				}
				else
					_ref.reset();
			}
		}
		inline bool freeWhenNoUser()
		{
			if (_ref.tryUnuseIfZero())
			{
				char* data = pageData;
				pageData = nullptr;
				_ref.reset();
				barrier;
				basicBufferPool::free(data);
				return true;
			}
			else
				return false;
		}
	};
#pragma pack()

}
