#pragma once
#include <stdint.h>
#include "buffer.h"
#include "basicBufferPool.h"
#include "util/likely.h"
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
	void** m_top;
	uint32_t m_topSize;
	uint32_t m_topUsedSize;
	buddyBuffer** m_levelHeads;
	uint8_t m_levels;
	uint32_t m_baseLevelSize;
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
		removeFromList(m_levelHeads[level]->bprev);
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
				assert(nullptr != (right = b->prev));
				if (!right->isFree)
					return;
				left = b;
			}
			else//right
			{
				assert(nullptr != (left = b->prev));
				if (!left->isFree)
					return;
				right = b;
			}
			removeFromList(right);
			removeFromList(left);
			left->mask |= (~(((uint32_t)1) << b->level));
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
			for (int i = 0; i < m_topSize; i++)
			{
				if (m_top[i] != nullptr)
				{
					buddyBuffer* node = (buddyBuffer*)m_buddyStructPool.alloc();
					m_top[i] = malloc(m_baseLevelSize << m_levels);
					node->buffer = (char*)m_top[i];
					node->isFree = true;
					node->mask = 1 << m_levels;
					node->level = m_levels;
					node->prev = node->next = nullptr;
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
		if(!allocTopLevel())
			return false;
	GET:
		while (--cl > level)
			split(getBuddyFromHead(cl));
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
		for (m_baseLevelSize = 1; m_baseLevelSize < baseSize; m_baseLevelSize <<= 1);
		uint32_t topNodeVolumn = m_baseLevelSize << level;
		m_topSize = maxMemory / topNodeVolumn + (maxMemory % topNodeVolumn > 0) ? 1 : 0;
		m_topUsedSize = 0;
		m_top = (void**)malloc(sizeof(void*) * m_topSize);
		memset(m_top, 0, sizeof(void*) * m_topSize);
		m_levelHeads = (buddyBuffer**)malloc(sizeof(buddyBuffer*) * level);
		memset(m_levelHeads, 0, sizeof(buddyBuffer*) * level);
	}
	~buddySystem()
	{
		for (uint32_t i = 0; i < m_topSize; i++)
		{
			if (m_top[i] != nullptr)
			{
				::free(m_top[i]);
				m_top[i] = nullptr;
			}
		}
	}
	inline uint8_t highPosOfUchar(uint8_t c)
	{
		if (c & 0x80 == 0x80)
			return 8;
		if (c & 0x40 == 0x40)
			return 7;
		if (c & 0x20 == 0x20)
			return 6;
		if (c & 0x10 == 0x10)
			return 5;
		if (c & 0x8 == 0x8)
			return 4;
		if (c & 0x4 == 0x4)
			return 3;
		if (c & 0x2 == 0x2)
			return 2;
		if (c & 0x1 == 0x1)
			return 1;
		return 0;
	}
	bufferBase* alloc(uint32_t size)
	{
		const uint8_t* array = (const uint8_t*)&size;
		uint8_t level;
		if (array[0] != 0)
			level = 24 + highPosOfUchar(array[0]);
		else if (array[1] != 0)
			level = 16 + highPosOfUchar(array[1]);
		else if (array[2] != 0)
			level = 8 + highPosOfUchar(array[2]);
		else if (array[3] != 0)
			level = highPosOfUchar(array[3]);
		return allocLevel(((1 << level) == size) ? level : level + 1);
	}

	void free(bufferBase* b)
	{
		buddyBuffer* buffer = static_cast<buddyBuffer*>(b);
		buffer->isFree = true;
		uint8_t level = buffer->level;
		merge(buffer);
		if (buffer->level != level)
			addToLevelCache(buffer);
	}
};
