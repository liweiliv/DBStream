#pragma once
#include <stdint.h>
#include "memory/bufferPool.h"
#include "thread/threadLocal.h"
#include "util/ref.h"
#include "thread/barrier.h"
#include "util/dualLinkList.h"
#include "thread/yield.h"
#include "thread/spinlock.h"
namespace DATABASE {
#pragma pack(1)
	struct page {
		uint32_t pageId;
		uint32_t pageUsedSize;
		uint32_t pageSize;
		uint32_t crc;
		globalLockDualLinkListNode lruNode;
		globalLockDualLinkList* lru;
		::ref _ref;
		char* pageData;

		inline void use()
		{
			int ref = 0;
			for (uint8_t i = 0; i < 10; i++)
			{
				if ((ref = _ref.use()) > 0)
					goto LRU;
			}
			for (; !(ref = _ref.use()); yield());
		LRU:
			if (ref == 1 && lru != nullptr)
			{
				if (lruNode.next != nullptr)
				{
					lru->erase(&lruNode);
				}
			}
		}
		inline void unuse()
		{
			if (lru != nullptr)
			{
				int ref = _ref.unuseForLru();
				if (ref == 0)
				{
					lru->insert(&lruNode);
					_ref.reset();
				}
				else if (ref < 0)
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
			else
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
