#pragma once
#include <stdint.h>
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
class buddySystem :public bufferBaseAllocer
{
private:
	buddyBuffer** m_topLevelHeads;
	uint32_t m_topSize;
	uint32_t m_topUsedSize;
	buddyBuffer** m_levelHeads;
	uint8_t m_levels;
	uint32_t m_baseLevelSize;
	uint8_t m_baseLevelOff;
	defaultBufferBaseAllocer m_buddyStructAllocer;
	basicBufferPool m_buddyStructPool;
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
			b->prev = head->bprev;
			head->bprev = b;
		}
	}
	inline void merge(buddyBuffer* b)
	{
		buddyBuffer* right, * left;
		do
		{
			if (b->mask & (((uint32_t)1) << b->level))//right
			{
				assert(nullptr != (left = b->prev));
				if (!left->isFree)
					return;
				right = b;
			}
			else//left
			{
				assert(nullptr != (right = b->next));
				if (!right->isFree)
					return;
				left = b;
			}
			removeFromList(right);
			removeFromList(left);
			left->mask &= (~(((uint32_t)1) << b->level));
			left->level++;

			left->next = right->next;
			right->next->prev = left;

			m_buddyStructPool.free(right);
			addToLevelCache(left);
			b = left;
		} while (b->level < m_levels);
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
		if (m_topSize > m_topUsedSize)
		{
			for (uint32_t i = 0; i < m_topSize; i++)
			{
				if (m_topLevelHeads[i] == nullptr)
				{
					buddyBuffer* node = (buddyBuffer*)m_buddyStructPool.alloc();
					node->buffer = (char*)malloc(m_baseLevelSize << m_levels);
					node->isFree = true;
					node->mask = 1 << m_levels;
					node->level = m_levels;
					node->prev = node->next = node;
					m_topLevelHeads[i] = node;
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
		if (unlikely(m_levelHeads[level] == nullptr))
		{
			if (unlikely(level >= m_levels))
				return nullptr;
			if (unlikely(!allocNewLevel(level)))
				return nullptr;
		}
		buddyBuffer* b = getBuddyFromHead(level);
		b->isFree = false;
		return b;
	}
public:
	buddySystem(uint64_t maxMemory, uint32_t baseSize, uint32_t level) :m_levels(level), m_buddyStructPool(&m_buddyStructAllocer, sizeof(buddyBuffer), 256 * 1024 * 1024)
	{
		m_baseLevelOff = 0;
		for (m_baseLevelSize = 1; m_baseLevelSize < baseSize; m_baseLevelSize <<= 1, m_baseLevelOff++);
		uint32_t topNodeVolumn = m_baseLevelSize << level;
		m_topSize = maxMemory / topNodeVolumn + ((maxMemory % topNodeVolumn > 0) ? 1 : 0);
		m_topUsedSize = 0;
		m_topLevelHeads = (buddyBuffer**)malloc(sizeof(buddyBuffer*) * (m_topSize + 1));
		memset(m_topLevelHeads, 0, sizeof(buddyBuffer*) * (m_topSize + 1));
		m_levelHeads = (buddyBuffer**)malloc(sizeof(buddyBuffer*) * (1 + level));
		memset(m_levelHeads, 0, sizeof(buddyBuffer*) * (1 + level));
	}
	~buddySystem()
	{
		for (uint32_t i = 0; i < m_topSize; i++)
		{
			if (m_topLevelHeads[i] != nullptr)
			{
				::free(m_topLevelHeads[i]->buffer);
				m_topLevelHeads[i] = nullptr;
			}
		}
	}

	/*only used in little endian*/
	bufferBase* alloc(uint32_t size)
	{
		uint8_t level = highPosOfUint(size>> m_baseLevelOff);
		return allocLevel(((m_baseLevelSize << (level - 1)) == size) ? level - 1 : level);
	}

	void free(bufferBase* b)
	{
		buddyBuffer* buffer = static_cast<buddyBuffer*>(b);
		buffer->isFree = true;
		merge(buffer);
	}
	bool check()
	{
		for (uint32_t i = 0; i < m_topSize; i++)
		{
			if (m_topLevelHeads[i] != nullptr)
			{
				buddyBuffer* buffer = m_topLevelHeads[i];
				do {
					if (buffer->next != m_topLevelHeads[i] && buffer->next->buffer < buffer->buffer)
					{
						abort();
					}
					buffer = buffer->next;
				} while (buffer != m_topLevelHeads[i]);
			}
		}
	}
};
