#pragma once
#include <stdint.h>
#include "meta/columnType.h"
#include "indexInfo.h"
#include "indexIterator.h"
#include "util/likely.h"
namespace DATABASE {
	struct varSolidIndex {
		const char * rawIndexData;
		uint32_t m_keyCount;
		varSolidIndex(const char * data):rawIndexData(data),m_keyCount(*(uint32_t*)data) {}
		template<class T>
		int find(const T & d, bool equalOrGreater)
		{
			abort();//not use this
		}
		template<>
		int find(const binaryType & d,bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char * idx = rawIndexData + ((uint32_t*)(rawIndexData + sizeof(uint32_t)))[m];
				binaryType _m(idx+sizeof(uint16_t),*(uint16_t*)idx);
				int c = d.compare(_m);
				if (c > 0)
				{
					s = m + 1;
				}
				else if (c < 0)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if(equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		template<>
		int find(const unionKey & d, bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				unionKey _m;
				_m.key = rawIndexData + ((uint32_t*)(rawIndexData + sizeof(uint32_t)))[m];
				_m.meta = d.meta;//todo 
				int c = d.compare(_m);
				if (c > 0)
				{
					s = m + 1;
				}
				else if (c < 0)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if (equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		inline const void* key(uint32_t idx)
		{
			return rawIndexData + ((uint32_t*)(rawIndexData + sizeof(uint32_t)))[idx];
		}
		inline void getRecordIdByIndex(uint32_t idx, const uint32_t *& recordIds, uint32_t &count)
		{
			const char * indexData = rawIndexData + ((uint32_t*)(rawIndexData + sizeof(uint32_t)))[idx];
			recordIds = (const uint32_t*)(indexData + *(uint16_t*)indexData + sizeof(uint16_t) + sizeof(uint32_t));
			count = *(uint32_t*)(((const char*)recordIds) - sizeof(uint32_t));
		}
		template<class T>
		inline bool find(const T & d, const uint32_t *& recordIds, uint32_t &count, bool equalOrGreater)
		{
			int idx = find(d, equalOrGreater);
			if (idx < 0)
				return false;
			getRecordIdByIndex(idx, recordIds, count);
			return true;
		}
		bool begin(const uint32_t*& recordIds, uint32_t& count)
		{
			if (count == 0)
				return false;
			getRecordIdByIndex(0, recordIds, count);
			return true;
		}
		bool rbegin(const uint32_t*& recordIds, uint32_t& count)
		{
			if (count == 0)
				return false;
			getRecordIdByIndex(this->m_keyCount - 1, recordIds, count);
			return true;
		}
		inline uint32_t getKeyCount()
		{
			return m_keyCount;
		}
		class iterator :public indexIterator
		{
		private:
			const uint32_t* recordIds;
			uint32_t idChildCount;
			varSolidIndex* index;
			uint32_t indexId;
			uint32_t innerIndexId;
			friend class varSolidIndex;
		public:
			iterator(varSolidIndex* index = nullptr) :recordIds(nullptr), idChildCount(0), key(nullptr), index(index), indexId(0), innerIndexId(0)
			{
				if (index != nullptr && index->begin(recordIds, idChildCount))
					innerIndexId = idChildCount - 1;
			}
			iterator(const iterator& iter) :recordIds(iter.recordIds), idChildCount(iter.idChildCount), key(iter.key), index(iter.index), indexId(iter.indexId), innerIndexId(iter.innerIndexId) {}
			iterator& operator=(const iterator& iter)
			{
				key = iter.key;
				index = iter.index;
				indexId = iter.indexId;
				innerIndexId = iter.innerIndexId;
				return *this
			}
			~iterator() {}
			inline bool valid()
			{
				return recordIds != nullptr && innerIndexId < idChildCount;
			}
			inline uint32_t value()
			{
				return recordIds[innerIndexId];
			}
			inline const void* key()
			{
				return index->getKey(indexId);
			}
			inline uint32_t currentVersionValue()
			{
				return recordIds[idChildCount - 1];
			}
			inline bool next()
			{
				if (innerIndexId + 1 < idChildCount)
				{
					innerIndexId++;
					return true;
				}
				else if (indexId + 1 < this->index->m_keyCount)
				{
					indexId++;
					innerIndexId = 0;
					index->getRecordIdByIndex(indexId, recordIds, idChildCount);
					return true;
				}
				else
					return false;
			}
			inline bool nextKey()
			{
				if (indexId + 1 < this->index->m_keyCount)
				{
					indexId++;
					index->getRecordIdByIndex(indexId, recordIds, idChildCount);
					innerIndexId = idChildCount - 1;
					return true;
				}
				else
					return false;
			}
			inline bool prev()
			{
				if (innerIndexId >= 1)
				{
					innerIndexId--;
					return true;
				}
				else if (indexId >= 1)
				{
					indexId--;
					index->getRecordIdByIndex(indexId, recordIds, idChildCount);
					innerIndexId = idChildCount - 1;
					return true;
				}
				else
					return false;
			}
			inline bool prevKey()
			{
				if (indexId >= 1)
				{
					indexId--;
					index->getRecordIdByIndex(indexId, recordIds, idChildCount);
					innerIndexId = idChildCount - 1;
					return true;
				}
				else
					return false;
			}
		};
	};
	struct fixedSolidIndex
	{
		const char * rawIndexData;
		uint16_t meta;
		uint32_t m_keyCount;
		fixedSolidIndex(const char * data) :rawIndexData(data), meta(*(uint16_t*)data), m_keyCount(*(uint32_t*)(data + sizeof(uint16_t)))
		{
		}
		template<class T>
		int find(const T & d, bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char * idx = rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t))*m;
				T _m = *(T*)idx;
				if (d > _m )
				{
					s = m + 1;
				}
				else if (d < _m)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if (equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		template<>
		int find(const unionKey & d, bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char * idx = rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t))*m;
				unionKey _m;
				_m.key = idx;
				_m.meta = d.meta;//todo
				int c = d.compare(_m);
				if (c > 0)
				{
					s = m + 1;
				}
				else if (c < 0)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if (equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		template<>
		int find(const float & d, bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char * idx = rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t))*m;
				float _m = *(float*)idx;
				if (d-_m > 0.00001f)
				{
					s = m + 1;
				}
				else if (_m - d > 0.00001f)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if (equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		template<>
		int find(const double & d, bool equalOrGreater)
		{
			int32_t s = 0, e = m_keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char * idx = rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t))*m;
				double _m = *(double*)idx;
				if (d - _m > 0.0000000001f)
				{
					s = m + 1;
				}
				else if (_m - d > 0.0000000001f)
				{
					e = m - 1;
				}
				else
				{
					return m;
				}
			}
			if (equalOrGreater)
				return -1;
			if (e < 0)
				return 0;
			if (s < m_keyCount)
				return s;
		}
		inline uint32_t getKeyCount()
		{
			return m_keyCount;
		}
		inline void getRecordIdByIndex(uint32_t idx, const uint32_t *& recordIds, uint32_t &count)
		{
			const char * indexData = rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t))*idx + meta;
			uint32_t idInfo = *(uint32_t*)indexData;
			if ((idInfo & 0xff000000) == 0)
			{
				recordIds = (const uint32_t*)indexData;
				count = 1;
			}
			else if ((idInfo & 0x80000000) == 0)
			{
				count = idInfo >> 24;
				recordIds = (const uint32_t*)(rawIndexData + (idInfo & 0x00ffffff));
			}
			else
			{
				count = *(const uint32_t*)(rawIndexData + (idInfo & 0x00ffffff));
				recordIds = (const uint32_t*)(rawIndexData + (idInfo & 0x00ffffff)+sizeof(uint32_t));
			}
		}
		template<class T>
		inline bool find(const T & d, const uint32_t *& recordIds, uint32_t &count, bool equalOrGreater)
		{
			int idx = find(d, equalOrGreater);
			if (idx < 0)
				return false;
			getRecordIdByIndex(idx, recordIds, count);
			return true;
		}
		bool begin(const uint32_t*& recordIds, uint32_t& count)
		{
			if (count == 0)
				return false;
			getRecordIdByIndex(0, recordIds, count);
			return true;
		}
		bool rbegin(const uint32_t*& recordIds, uint32_t& count)
		{
			if (count == 0)
				return false;
			getRecordIdByIndex(this->m_keyCount-1, recordIds, count);
			return true;
		}
		inline const void* getKey(uint32_t id)
		{
			return  rawIndexData + sizeof(uint16_t) + sizeof(uint32_t) + (meta + sizeof(uint32_t)) * m;
		}
	};
	template<class T,class INDEX_TYPE>
	class solidIndexIterator:public indexIterator<T>
	{
	private:
		uint32_t indexId;
	public:
		solidIndexIterator(INDEX_TYPE* index, META::COLUMN_TYPE type, const unionKeyMeta* ukMeta = nullptr) :indexIterator<INDEX_TYPE>(index, type, ukMeta), indexId(0)
		{
		}
		virtual ~solidIndexIterator() {}
		virtual bool begin()
		{
			if (!index->begin(recordIds, idChildCount))
				return fasle;
			indexId = 0;
			innerIndexId = idChildCount - 1;
			return true;
		}
		virtual bool rbegin()
		{
			if (index->getKeyCount() == 0)
				return false;
			indexId = index->getKeyCount() - 1;
			getRecordIdByIndex(indexId, recordIds, idChildCount);
			innerIndexId = idChildCount - 1;
			return true;
		}
		virtual bool seek(const void* key)
		{
			
		}
		virtual inline bool valid()const
		{
			return recordIds != nullptr && innerIndexId < idChildCount;
		}
		virtual inline uint32_t value() const
		{
			return recordIds[innerIndexId];
		}
		virtual inline const void* key()const
		{
			return index->getKey(indexId);
		}
		virtual inline bool nextKey()
		{
			if (indexId + 1 < this->index->getKeyCount())
			{
				indexId++;
				index->getRecordIdByIndex(indexId, recordIds, idChildCount);
				innerIndexId = idChildCount - 1;
				return true;
			}
			else
				return false;
		}
		virtual inline bool prevKey()
		{
			if (indexId >= 1)
			{
				indexId--;
				index->getRecordIdByIndex(indexId, recordIds, idChildCount);
				innerIndexId = idChildCount - 1;
				return true;
			}
			else
				return false;
		}
	};
}
