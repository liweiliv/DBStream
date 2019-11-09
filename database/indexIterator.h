#pragma once
#include <stdint.h>
#include <assert.h>
#include "meta/columnType.h"
namespace DATABASE
{
	template<class INDEX_TYPE>
	class indexIterator
	{
	protected:
		uint32_t flag;
		INDEX_TYPE* index;
		META::COLUMN_TYPE type;
		const uint32_t* recordIds;
		uint32_t idChildCount;
		uint32_t innerIndexId;
	public:
		indexIterator(uint32_t flag,INDEX_TYPE* index ,META::COLUMN_TYPE type) :flag(flag),index(index), type(type),recordIds(nullptr), idChildCount(0), innerIndexId(0)
		{
		}
		indexIterator(const indexIterator& iter) :flag(iter.flag),index(iter.index), type(iter.type),recordIds(iter.recordIds), idChildCount(iter.idChildCount), innerIndexId(iter.innerIndexId) {}
		indexIterator& operator=(const indexIterator& iter)
		{
			key = iter.key;
			index = iter.index;
			innerIndexId = iter.innerIndexId;
			return *this;
		}
		virtual ~indexIterator() {}
		virtual bool begin() = 0;
		virtual bool rbegin() = 0;
		virtual bool seek(const void * key) = 0;
		virtual inline bool valid()const
		{
			return recordIds != nullptr && innerIndexId < idChildCount;
		}
		virtual inline uint32_t value() const
		{
			return recordIds[innerIndexId];
		}
		virtual inline void toLastValueOfKey()
		{
			innerIndexId = idChildCount - 1;
		}
		virtual inline void toFirstValueOfKey()
		{
			innerIndexId = 0;
		}
		virtual inline const void* key()const = 0;
		virtual inline uint32_t currentVersionValue()
		{
			return recordIds[idChildCount - 1];
		}
		virtual inline bool next()
		{
			if (innerIndexId + 1 < idChildCount)
			{
				innerIndexId++;
				return true;
			}
			else
				return false;
		}
		virtual inline bool nextKey() = 0;
		virtual inline bool prev()
		{
			if (innerIndexId >= 1)
			{
				innerIndexId--;
				return true;
			}
			else
				return false;
		}
		virtual inline bool prevKey() = 0;
	};
}
