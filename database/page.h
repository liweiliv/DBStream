#pragma once
#include <stdint.h>
#include "memory/bufferPool.h"
#include "thread/threadLocal.h"
#include "util/ref.h"
#include "util/barrier.h"
namespace DATABASE {
	struct page {
		uint64_t pageId;
		uint64_t pageUsedSize;
		uint64_t pageSize;
		uint32_t crc;
		::ref _ref;
		dualLinkListNode lruNode;
		char *pageData;
		
		inline void use()
		{
			for (uint8_t i = 0; i < 10; i++)
			{
				if (_ref.use())
					return;
			}
			for(;!_ref.use();std::this_thread::sleep_for(std::chrono::nanoseconds(100)));
		}
		inline void unuse()
		{
			if (_ref.unuse())
			{
				char* data = pageData;
				pageData = nullptr;
				_ref.reset();
				barrier;
				basicBufferPool::free(data);
			}
		}
	};

}
