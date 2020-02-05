#pragma once
#include <stdint.h>
#include <mutex>
#include "buffer.h"
#include "basicBufferPool.h"
#include "util/likely.h"
#include "util/bitUtil.h"
struct buddyBuffer :public bufferBase {
	buddyBuffer* next;
	buddyBuffer* prev;

	buddyBuffer* bnext;
	buddyBuffer* bprev;

	uint32_t mask;
	uint8_t level;
	bool isFree;
};
struct topBuddyBuffer :public buddyBuffer {
	uint32_t id;
	bool isBackUp;
};
class buddySystem :public bufferBaseAllocer
{
private:
	topBuddyBuffer** m_topLevelHeads;
	topBuddyBuffer** m_bakTopLevelHeads;
	uint32_t m_topBuddyCount;
	uint32_t m_topBuddyVolumn;
	uint32_t m_bakTopSize;
	uint32_t m_topBuddyUsedCount;
	uint32_t m_allTopBuddyCount;
	uint32_t m_freeTopCount;
	uint32_t m_bakTopLevelRemind;

	buddyBuffer** m_levelHeads;
	uint8_t m_levels;
	uint32_t m_baseLevelSize;
	uint8_t m_baseLevelOff;
	uint64_t m_allocedMem;
	uint16_t m_usage;//percent%
	defaultBufferBaseAllocer m_buddyStructAllocer;
	basicBufferPool m_buddyStructPool;

	uint16_t m_highUsage;
	uint16_t m_lowUsage;

	void (*m_highUsageCallback)(void* handle,uint16_t usage);
	void (*m_lowUsageCallback)(void* handle, uint16_t usage);

	void* m_callbackHandle;



	std::mutex m_lock;
	inline void removeFromList(buddyBuffer* b)
	{
		b->bnext->bprev = b->bprev;
		b->bprev->bnext = b->bnext;
		if (m_levelHeads[b->level] == b)
			m_levelHeads[b->level] = nullptr;
	}
	inline buddyBuffer* getBuddyFromHead(uint8_t level)
	{
		assert(m_levelHeads[level] != nullptr);
		buddyBuffer* buffer = m_levelHeads[level]->bprev;
		removeFromList(buffer);
		return buffer;
	}
	inline void addToLevelCache(buddyBuffer* b)
	{
		buddyBuffer* head = m_levelHeads[b->level];
		if (head == nullptr)
		{
			m_levelHeads[b->level] = b;
			b->bprev = b;
			b->bnext = b;
		}
		else
		{
			head->bprev->bnext = b;
			b->bnext = head;
			b->bprev = head->bprev;
			head->bprev = b;
		}
	}
	void topIsFree(topBuddyBuffer* top)
	{
		m_topBuddyUsedCount--;
		if (top->isBackUp)
		{
			::free(top->buffer);
			m_buddyStructPool.free(top);
			for (uint32_t i = 0; i < m_bakTopSize; i++)
			{
				if (m_bakTopLevelHeads[i] == top)
				{
					m_bakTopLevelHeads[i] = nullptr;
					break;
				}
			}
			if (--m_bakTopLevelRemind == 0)
			{
				::free(m_bakTopLevelHeads);
				m_bakTopLevelHeads = nullptr;
				m_bakTopSize = 0;
			}
		}
		else
		{
			m_freeTopCount++;
			if (m_freeTopCount > 1)
			{
				assert(top == m_topLevelHeads[top->id]);
				m_topLevelHeads[top->id] = nullptr;
				::free(top->buffer);
				m_buddyStructPool.free(top);
				m_topBuddyCount--;
			}
		}
	}
	inline void merge(buddyBuffer* b)
	{
		buddyBuffer* right, * left;
		std::lock_guard<std::mutex> guard(m_lock);
		m_allocedMem -= size(b->level);
		do
		{
			if (b->mask & (((uint32_t)1) << b->level))//right
			{
				assert(nullptr != (left = b->prev));
				if (left->level != b->level || !left->isFree)
					return;
				right = b;
			}
			else//left
			{
				assert(nullptr != (right = b->next));
				if (right->level != b->level || !right->isFree)
					return;
				left = b;
			}
			removeFromList(right);
			removeFromList(left);

			left->next = right->next;
			right->next->prev = left;

			m_buddyStructPool.free(right);
			left->level++;
			addToLevelCache(left);
			b = left;
		} while (b->level < m_levels);
		//now it is top 
		topIsFree(static_cast<topBuddyBuffer*>(b));
	}
	inline uint32_t size(uint8_t level)
	{
		return m_baseLevelSize << level;
	}
	inline void split(buddyBuffer* b)
	{
		b->level--;
		buddyBuffer* next = (buddyBuffer*)m_buddyStructPool.alloc();
		next->prev = b;
		next->next = b->next;
		b->next->prev = next;
		b->next = next;

		next->buffer = b->buffer + size(b->level);
		next->mask = b->mask | (((uint32_t)1) << b->level);
		next->level = b->level;
		next->isFree = true;

		next->bprev = b;
		b->bnext = next;

		buddyBuffer* head = m_levelHeads[b->level];
		if (head == nullptr)
		{
			m_levelHeads[b->level] = b;
			b->bprev = next;
			next->bnext = b;
		}
		else
		{
			head->prev->bnext = b;
			next->bnext = head;
			b->prev = head->prev;
			head->bprev = next;
		}
	}
	inline bool allocTopLevel()
	{
		if (m_topBuddyCount < m_topBuddyVolumn)
		{
			for (uint32_t i = 0; i < m_topBuddyVolumn; i++)
			{
				if (m_topLevelHeads[i] == nullptr)
				{
					topBuddyBuffer* node = (topBuddyBuffer*)m_buddyStructPool.alloc();
					node->buffer = (char*)malloc(m_baseLevelSize << m_levels);
					node->isFree = true;
					node->mask = 1 << m_levels;
					node->level = m_levels;
					node->prev = node->next = node;
					node->isBackUp = false;
					node->id = i;
					m_topLevelHeads[i] = node;
					m_allTopBuddyCount++;
					m_topBuddyUsedCount++;
					m_topBuddyCount++;
					addToLevelCache(node);
					return true;
				}
			}
		}
		return false;
	}
	inline bool allocNewLevel(uint8_t level)
	{
		uint8_t cl = level + 1;
		while (cl < m_levels)
		{
			if (m_levelHeads[cl] == nullptr)
				cl++;
			else
				goto GET;
		}
		if (!allocTopLevel())
			return false;

	GET:
		do
		{
			split(getBuddyFromHead(cl));
		} while (--cl > level);
		return true;
	}
	buddyBuffer* allocLevel(uint8_t level)
	{
		m_lock.lock();
		if (unlikely(m_levelHeads[level] == nullptr))
		{
			if (unlikely(level >= m_levels) || unlikely(!allocNewLevel(level)))
			{
				m_lock.unlock();
				return nullptr;
			}
		}
		buddyBuffer* b = getBuddyFromHead(level);
		m_allocedMem += size(level);
		m_lock.unlock();
		b->isFree = false;
		m_usage = (m_allocedMem * 100) / (m_topBuddyVolumn * size(m_levels));
		if (m_highUsageCallback != nullptr && m_usage > m_highUsage)
			m_highUsageCallback(m_callbackHandle,m_usage);
		return b;
	}
public:
	buddySystem(uint64_t maxMemory, uint32_t baseSize, uint32_t level,
		uint16_t highUsage = 90, uint16_t lowUsage = 10, void (*highUsageCallback)(void*,uint16_t) = nullptr, void (*lowUsageCallback)(void*, uint16_t) = nullptr, void* callbackHandle = nullptr) :
		m_levels(level), m_topBuddyCount(0), m_buddyStructPool(&m_buddyStructAllocer, sizeof(topBuddyBuffer), 256 * 1024 * 1024),
		m_highUsage(highUsage), m_lowUsage(lowUsage), m_highUsageCallback(highUsageCallback), m_lowUsageCallback(lowUsageCallback), m_callbackHandle(callbackHandle)
	{
		m_baseLevelOff = 0;
		for (m_baseLevelSize = 1; m_baseLevelSize < baseSize; m_baseLevelSize <<= 1, m_baseLevelOff++);
		uint32_t topNodeVolumn = m_baseLevelSize << level;
		m_topBuddyVolumn = maxMemory / topNodeVolumn + ((maxMemory % topNodeVolumn > 0) ? 1 : 0);
		m_topBuddyUsedCount = 0;
		m_bakTopLevelRemind = 0;
		m_bakTopSize = 0;
		m_bakTopLevelHeads = nullptr;
		m_topLevelHeads = (topBuddyBuffer**)malloc(sizeof(buddyBuffer*) * (m_topBuddyVolumn + 1));
		memset(m_topLevelHeads, 0, sizeof(buddyBuffer*) * (m_topBuddyVolumn + 1));
		m_levelHeads = (buddyBuffer**)malloc(sizeof(buddyBuffer*) * (1 + level));
		memset(m_levelHeads, 0, sizeof(buddyBuffer*) * (1 + level));
	}
	~buddySystem()
	{
		for (uint32_t i = 0; i < m_topBuddyVolumn; i++)
		{
			if (m_topLevelHeads[i] != nullptr)
			{
				::free(m_topLevelHeads[i]->buffer);
				m_topLevelHeads[i] = nullptr;
			}
		}
	}
	void resetMaxMemLimit(uint64_t maxMemory)
	{
		uint32_t topNodeVolumn = m_baseLevelSize << m_levels;
		std::lock_guard<std::mutex> guard(m_lock);
		uint32_t bakTopSize = m_topBuddyCount, bakTopVolumn = m_topBuddyVolumn, topBuddyUsedCount = 0;
		m_topBuddyVolumn = maxMemory / topNodeVolumn + ((maxMemory % topNodeVolumn > 0) ? 1 : 0);
		if (bakTopVolumn == m_topBuddyVolumn)
			return;
		topBuddyBuffer** topLevelHeads = (topBuddyBuffer**)malloc(sizeof(topBuddyBuffer*) * (m_topBuddyVolumn + 1));
		memset(topLevelHeads, 0, sizeof(topBuddyBuffer*) * (m_topBuddyVolumn + 1));

		uint32_t bid = 0, nid = 0, bbid = 0;
		for (; nid < m_topBuddyVolumn; nid++)
		{
			while (m_topLevelHeads[bid] == nullptr && bid < bakTopVolumn)
				bid++;
			if (bid >= bakTopVolumn)
				break;
			topLevelHeads[nid] = m_topLevelHeads[bid];
			if (!topLevelHeads[nid]->isFree)
				topBuddyUsedCount++;
			bid++;
			m_topBuddyCount--;
		}
		if (m_bakTopLevelHeads != nullptr)
		{
			for (; nid < m_topBuddyVolumn; nid++)
			{
				while (m_bakTopLevelHeads[bbid] == nullptr && bbid < m_bakTopSize)
					bbid++;
				if (bbid >= m_bakTopSize)
					break;
				topLevelHeads[nid] = m_bakTopLevelHeads[bbid];
				topLevelHeads[nid]->isBackUp = false;
				if (!topLevelHeads[nid]->isFree)
					topBuddyUsedCount++;
				bbid++;
				m_bakTopLevelRemind--;
			}
			if (m_bakTopLevelRemind == 0)
			{
				::free(m_bakTopLevelHeads);
				m_bakTopLevelHeads = nullptr;
				m_bakTopSize = 0;
			}
		}
		if (nid < m_topBuddyVolumn)
		{
			assert(bid >= bakTopSize);
			assert(m_bakTopLevelHeads == nullptr);
		}
		else
		{
			if (m_topBuddyCount + m_bakTopLevelRemind > 0)
			{
				uint32_t bnid = 0, _bakTopSize = m_topBuddyCount + m_bakTopLevelRemind;
				topBuddyBuffer** bakTopLevelHeads = (topBuddyBuffer**)malloc(sizeof(topBuddyBuffer*) * (_bakTopSize));
				for (; bid < bakTopVolumn; bid++)
				{
					if (m_topLevelHeads[bid] != nullptr)
					{
						bakTopLevelHeads[bnid] = m_topLevelHeads[bid];
						bakTopLevelHeads[bnid++]->isBackUp = true;
					}
				}
				if (m_bakTopLevelHeads != nullptr)
				{
					for (; bbid < m_bakTopSize; bbid++)
					{
						if (m_bakTopLevelHeads[bbid] != nullptr)
							bakTopLevelHeads[bnid++] = m_bakTopLevelHeads[bbid];
					}
					::free(m_bakTopLevelHeads);
				}
				m_bakTopLevelHeads = bakTopLevelHeads;
				m_bakTopLevelRemind = m_bakTopSize = _bakTopSize;
			}

		}
		::free(m_topLevelHeads);
		m_topLevelHeads = topLevelHeads;
		m_topBuddyCount = nid;
		m_topBuddyUsedCount = topBuddyUsedCount;
	}

	bufferBase* alloc(uint32_t size)
	{
		uint8_t level = highPosOfUint(size >> m_baseLevelOff);
		return allocLevel(((m_baseLevelSize << (level - 1)) == size) ? level - 1 : level);
	}

	void free(bufferBase* b)
	{
		static_cast<buddyBuffer*>(b)->isFree = true;
		merge(static_cast<buddyBuffer*>(b));
		m_usage = (m_allocedMem * 100) / (m_topBuddyVolumn * size(m_levels));
		if (m_lowUsageCallback != nullptr && m_usage < m_lowUsage)
			m_lowUsageCallback(m_callbackHandle,m_usage);
	}
	inline uint32_t usage()
	{
		return m_usage;
	}
};
