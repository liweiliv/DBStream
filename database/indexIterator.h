#pragma once
#include <stdint.h>
#include <assert.h>
#include "meta/columnType.h"
#include "iterator.h"
namespace DATABASE
{
	template<class INDEX_TYPE>
	class IndexIterator
	{
	protected:
		uint32_t flag;
		INDEX_TYPE* index;
		META::COLUMN_TYPE type;
		const uint32_t* recordIds;
		uint32_t idChildCount;
		uint32_t innerIndexId;
	public:
		IndexIterator(uint32_t flag, INDEX_TYPE* index, META::COLUMN_TYPE type) :flag(flag), index(index), type(type), recordIds(nullptr), idChildCount(0), innerIndexId(0)
		{
		}
		IndexIterator(const IndexIterator& iter) :flag(iter.flag), index(iter.index), type(iter.type), recordIds(iter.recordIds), idChildCount(iter.idChildCount), innerIndexId(iter.innerIndexId) {}
		IndexIterator& operator=(const IndexIterator& iter)
		{
			key = iter.key;
			index = iter.index;
			innerIndexId = iter.innerIndexId;
			return *this;
		}
		virtual ~IndexIterator() {}
		virtual bool begin() = 0;
		virtual bool rbegin() = 0;
		virtual bool seek(const void* key) = 0;
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
		virtual const void* key()const = 0;
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
		virtual bool nextKey() = 0;
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
		virtual bool prevKey() = 0;
	};
}
