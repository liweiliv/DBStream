#pragma once
#include <stdint.h>
#include "../memory/bufferPool.h"
#include "../util/threadLocal.h"
namespace STORE {
	struct page {
		uint64_t pageId;
		uint64_t pageUsedSize;
		uint64_t pageSize;
		uint32_t crc;
		std::atomic_int ref;
		dualLinkListNode lruNode;
		char *pageData;
		inline bool touch()
		{
			int _ref = ref.load(std::memory_order_relaxed);
			do {
				if (_ref >= 0)
				{
					if (ref.compare_exchange_weak(_ref, _ref + 1, std::memory_order_release))
					{
						return true;
					}
				}
				else
				{
					return false;
				}
			} while (1);
		}
	};

}
