/*
 * pageTable.h
 *
 *  Created on: 2019年1月22日
 *      Author: liwei
 */

#ifndef PAGETABLE_H_
#define PAGETABLE_H_
#define PT_HIGN_OFFSET 8
#define PT_HIGN_MASK 0xff000000
#define PT_MID_OFFSET 12
#define PT_MID_MASK  0x00fff000
#define PT_LOW_OFFSET 12
#define PT_LOW_MASK  0x00000fff

#define PT_HIGH(id) ((id)>>(PT_MID_OFFSET+PT_LOW_OFFSET))
#define PT_MID(id) (((id)&PT_MID_MASK)>>PT_LOW_OFFSET)
#define PT_LOW(id) ((id)&PT_LOW_MASK)
#include <atomic>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "barrier.h"

template<class T>
class pageTable
{
private:
	struct lowNode
	{
		std::atomic<T*> child[PT_LOW(0xffffffffu) + 1];
		lowNode()
		{
			for (uint16_t idx = 0; idx < PT_LOW(0xffffffffu) + 1; idx++)
				child[idx].store(nullptr, std::memory_order_relaxed);
			//	barrier
			;
		}

	};
	struct midNode
	{
		std::atomic<lowNode*> child[PT_MID(0xffffffffu) + 1];
		midNode()
		{
			for (uint16_t idx = 0; idx < PT_MID(0xffffffffu) + 1; idx++)
				child[idx].store(nullptr, std::memory_order_relaxed);
			//	barrier
			;
		}
	};
	struct highNode
	{
		std::atomic<midNode*> child[PT_HIGH(0xffffffffu) + 1];
		highNode()
		{
			for (uint16_t idx = 0; idx < PT_HIGH(0xffffffffu) + 1; idx++)
				child[idx].store(nullptr, std::memory_order_relaxed);
			//barrier
			;
		}
	};
	highNode root;
	std::atomic<uint32_t> min;
	std::atomic<uint32_t> max;
	int (*destoryValueFunc)(T* data);
public:
	pageTable(int (*_destoryValueFunc)(T*) = nullptr) :
		destoryValueFunc(_destoryValueFunc)
	{
		min.store(0, std::memory_order_relaxed);
		max.store(0, std::memory_order_relaxed);
	}
	~pageTable()
	{
		clear();
	}
	void clear()
	{
		min.store(0, std::memory_order_relaxed);
		max.store(0, std::memory_order_relaxed);
		for (uint16_t i = 0; i <= PT_HIGH(0xffffffffu); i++)
		{
			midNode* mid;
			if ((mid = root.child[i].load(std::memory_order_relaxed)) != NULL)
			{
				for (uint16_t j = 0; j <= PT_MID(0xffffffffu); j++)
				{
					lowNode* low;
					if ((low = mid->child[j].load(std::memory_order_relaxed))
						!= NULL)
					{
						for (uint16_t m = 0; m <= PT_LOW(0xffffffffu); m++)
						{
							T* data;
							if ((data = low->child[m].load(
								std::memory_order_relaxed)) != NULL)
							{
								if (destoryValueFunc)
									destoryValueFunc(data);
							}
						}
						delete low;
					}
				}
				delete mid;
			}
			else
				root.child[i].store(nullptr, std::memory_order_relaxed);
		}
	}
	inline T* get(uint32_t id)
	{
		if (min.load(std::memory_order_relaxed) > id
			|| max.load(std::memory_order_relaxed) < id)
			return nullptr;
		uint8_t highId = PT_HIGH(id);
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		if (mid == nullptr)
			return nullptr;
		uint16_t midId = PT_MID(id);
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
			return nullptr;
		uint32_t lowId = PT_LOW(id);
		return low->child[lowId].load(std::memory_order_relaxed);
	}
	inline bool update(uint32_t id, T* data)
	{
		if (min.load(std::memory_order_relaxed) > id
			|| max.load(std::memory_order_relaxed) < id)
			return false;
		uint8_t highId = PT_HIGH(id);
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		if (mid == nullptr)
			return false;
		uint16_t midId = PT_MID(id);
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
			return false;
		uint32_t lowId = PT_LOW(id);
		if (nullptr == low->child[lowId].load(std::memory_order_relaxed))
			return false;
		low->child[lowId].store(data, std::memory_order_acquire);
		return true;
	}
	inline bool updateCas(uint32_t id, T* data,T *& expect)
	{
		if (min.load(std::memory_order_relaxed) > id
			|| max.load(std::memory_order_relaxed) < id)
		{
			expect = nullptr;
			return false;
		}
		uint8_t highId = PT_HIGH(id);
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		if (mid == nullptr)
		{
			expect = nullptr;
			return false;
		}		
		uint16_t midId = PT_MID(id);
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
		{
			expect = nullptr;
			return false;
		}
		return low->child[PT_LOW(id)].compare_exchange_strong(expect,data, std::memory_order_acquire);
	}
	inline T* operator[](uint32_t id)
	{
		return get(id);
	}
	inline T* begin()
	{
		do
		{
			T* data = nullptr;
			uint32_t id = min.load(std::memory_order_relaxed);
			if (nullptr == (data = get(id)))
			{
				if (id == min.load(std::memory_order_relaxed))
					return nullptr;
			}
			else
				return data;
		} while (1);
	}
	inline T* end()
	{
		do
		{
			T* data = nullptr;
			uint32_t id = max.load(std::memory_order_relaxed);
			if (nullptr == (data = get(id)))
			{
				if (id == max.load(std::memory_order_relaxed))
					return nullptr;
			}
			else
				return data;
		} while (1);
	}
	inline T* set(uint32_t id, T* data)
	{
		uint8_t highId = PT_HIGH(id);
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		bool newMid = false, newLow = false;
		if (mid == nullptr)
		{
			mid = new midNode;
			midNode* nullData = nullptr;
			while (!root.child[highId].compare_exchange_weak(nullData, mid, std::memory_order_relaxed, std::memory_order_relaxed))
			{
				if (nullData != nullptr)
				{
					delete mid;
					mid = nullData;
					break;
				}
			}

		}
		uint16_t midId = PT_MID(id);
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
		{
			low = new lowNode;
			lowNode* nullData = nullptr;
			while (!mid->child[midId].compare_exchange_weak(nullData, low, std::memory_order_relaxed, std::memory_order_relaxed))
			{
				if (nullData != nullptr)
				{
					delete low;
					low = nullData;
					break;
				}
			}

		}
		uint16_t lowId = PT_LOW(id);
		T* tmpData = nullptr;
		if ((tmpData = low->child[lowId].load(std::memory_order_relaxed))
			!= nullptr)
			return tmpData;
		while (!low->child[lowId].compare_exchange_weak(tmpData, data, std::memory_order_relaxed, std::memory_order_relaxed))
		{
			if (tmpData != nullptr)
				return tmpData;
		}
		uint32_t _min;
		do
		{
			_min = min.load(std::memory_order_relaxed);
			if (_min <= id)
				break;
			if (min.compare_exchange_weak(_min, id, std::memory_order_relaxed,
				std::memory_order_relaxed))
				break;
		} while (1);
		uint32_t _max;
		do
		{
			_max = max.load(std::memory_order_relaxed);
			if (_max >= id)
				break;
			if (max.compare_exchange_weak(_max, id, std::memory_order_relaxed,
				std::memory_order_relaxed))
				break;
		} while (1);
		return data;
	}
	inline void erase(uint32_t id)
	{
		if (min.load(std::memory_order_relaxed) > id
			|| max.load(std::memory_order_relaxed) < id)
			return;
		uint8_t highId = PT_HIGH(id);
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		if (mid == nullptr)
			return;
		uint16_t midId = PT_MID(id);
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
			return;
		uint16_t lowId = PT_LOW(id);
		T* data = low->child[lowId].load(std::memory_order_relaxed);
		if (data == nullptr)
			return;
		do
		{
			T* nulldata = nullptr;
			if (low->child[lowId].compare_exchange_weak(data, nulldata,
				std::memory_order_acq_rel, std::memory_order_acq_rel))
				break;
			assert(low->child[lowId].load(std::memory_order_relaxed) == data);
		} while (1);
		if (destoryValueFunc)
			destoryValueFunc(data);
	}
	inline void purge(uint32_t id)
	{
		if (min.load(std::memory_order_relaxed) > id
			|| max.load(std::memory_order_relaxed) < id)
			return;
		uint32_t _min;
		do
		{
			_min = min.load(std::memory_order_relaxed);
			if (_min <= id)
				break;
			if (min.compare_exchange_weak(_min, id, std::memory_order_acq_rel,
				std::memory_order_acq_rel))
				break;
		} while (1);
		uint8_t highId = PT_HIGH(id);
		for (uint16_t i = 0; i < highId; i++)
		{
			midNode* mid;
			if ((mid = root.child[i].load(std::memory_order_relaxed)) != NULL)
			{
				for (uint16_t j = 0; j < PT_MID(0xffffffffu); j++)
				{
					lowNode* low;
					if ((low = mid->child[j].load(std::memory_order_relaxed))
						!= NULL)
					{
						for (uint16_t m = 0; m < PT_LOW(0xffffffffu); m++)
						{
							T* data;
							if ((data = low->child[m].load(
								std::memory_order_release)) != NULL)
							{
								if (destoryValueFunc)
									destoryValueFunc(data);
							}
						}
						delete low;
					}
				}
				delete mid;
			}
			else
				root.child[i].store(nullptr, std::memory_order_relaxed);
		}
		midNode* mid = root.child[highId].load(std::memory_order_relaxed);
		if (mid == nullptr)
			return;
		uint16_t midId = PT_MID(id);
		for (uint16_t j = 0; j < midId; j++)
		{
			lowNode* low;
			if ((low = mid->child[j].load(std::memory_order_relaxed)) != NULL)
			{
				mid->child[j].store(nullptr, std::memory_order_release);
				for (uint16_t m = 0; m < PT_LOW(0xffffffffu); m++)
				{
					T* data;
					if ((data = low->child[m].load(std::memory_order_relaxed))
						!= NULL)
					{
						if (destoryValueFunc)
							destoryValueFunc(data);
					}
				}
				delete low;
			}
		}
		lowNode* low = mid->child[midId].load(std::memory_order_relaxed);
		if (low == nullptr)
			return;
		uint16_t lowId = PT_LOW(id);
		for (uint16_t m = 0; m < lowId; m++)
		{
			T* data;
			if ((data = low->child[m].load(std::memory_order_relaxed)) != NULL)
			{
				low->child[m].store(nullptr, std::memory_order_release);
				if (destoryValueFunc)
					destoryValueFunc(data);
			}
		}
	}
};

#endif /* PAGETABLE_H_ */
