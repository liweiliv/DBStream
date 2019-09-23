#pragma once
#include <stdint.h>
#include "memory/bufferPool.h"
#include "util/threadLocal.h"
#include "util/ref.h"
namespace STORE {
	struct page {
		uint64_t pageId;
		uint64_t pageUsedSize;
		uint64_t pageSize;
		uint32_t crc;
		ref _ref;
		dualLinkListNode lruNode;
		char *pageData;
	};

}
