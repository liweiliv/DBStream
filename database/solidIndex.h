#pragma once
#include <stdint.h>
#include "meta/columnType.h"
#include "indexIterator.h"
#include "util/likely.h"
namespace DATABASE {
#define SOLID_INDEX_FLAG_FIXED 0x01
#pragma pack(1)
	struct solidIndexHead {
		uint32_t keyCount;
		int8_t type;
		int8_t flag;
		int16_t length;
	};
#pragma pack()
	struct varSolidIndex {
		solidIndexHead* head;
		varSolidIndex(const char* data) :head((solidIndexHead*)data) {}
		varSolidIndex(const varSolidIndex& index) :head(index.head) {}
		varSolidIndex& operator=(const varSolidIndex& index) { head = index.head; return *this; }
		template<class T>
		int find(const T& d, bool equalOrGreater)const
		{
			abort();//not use this
		}
		template<>
		int find(const META::binaryType& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[m];
				META::binaryType _m(idx + sizeof(uint16_t), *(uint16_t*)idx);
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
			if (s < head->keyCount)
				return s;
		}
		template<>
		int find(const META::unionKey& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				META::unionKey _m;
				_m.key = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[m];
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
			if (s < head->keyCount)
				return s;
		}
		inline const void* key(uint32_t idx)const
		{
			return ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(uint32_t)))[idx];
		}
		inline void getRecordIdByIndex(uint32_t idx, const uint32_t*& recordIds, uint32_t& count)const
		{
			const char* indexData = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[idx];
			recordIds = (const uint32_t*)(indexData + *(uint16_t*)indexData + sizeof(uint16_t) + sizeof(uint32_t));
			count = *(uint32_t*)(((const char*)recordIds) - sizeof(uint32_t));
		}
		template<class T>
		inline bool find(const T& d, const uint32_t*& recordIds, uint32_t& count, bool equalOrGreater)const
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
			getRecordIdByIndex(this->head->keyCount - 1, recordIds, count);
			return true;
		}
		inline uint32_t getKeyCount()const
		{
			return head->keyCount;
		}
		inline const void* getKey(uint32_t id)const
		{
			return ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[id];
		}
		inline META::COLUMN_TYPE getType()const
		{
			return static_cast<META::COLUMN_TYPE>(head->type);
		}
	};
	struct fixedSolidIndex
	{
		solidIndexHead* head;
		fixedSolidIndex(const char* data) : head((solidIndexHead*)data) {}
		fixedSolidIndex(const fixedSolidIndex& index) :head(index.head) {}
		fixedSolidIndex& operator=(const fixedSolidIndex& index) { head = index.head; return *this; }
		template<class T>
		int find(const T& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
				T _m = *(T*)idx;
				if (d > _m)
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
			if (s < (int)head->keyCount)
				return s;
		}
		template<>
		int find(const META::unionKey& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
				META::unionKey _m;
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
			if (s < head->keyCount)
				return s;
		}
		template<>
		int find(const float& d, bool equalOrGreater)const 
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
				float _m = *(float*)idx;
				if (d - _m > 0.00001f)
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
			if (s < head->keyCount)
				return s;
		}
		template<>
		int find(const double& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
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
			if (s < head->keyCount)
				return s;
		}
		inline uint32_t getKeyCount()const
		{
			return head->keyCount;
		}
		inline void getRecordIdByIndex(uint32_t idx, const uint32_t*& recordIds, uint32_t& count)const
		{
			const char* indexData = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * idx + head->length;
			uint32_t idInfo = *(uint32_t*)indexData;
			if ((idInfo & 0xff000000) == 0)
			{
				recordIds = (const uint32_t*)indexData;
				count = 1;
			}
			else if ((idInfo & 0x80000000) == 0)
			{
				count = idInfo >> 24;
				recordIds = (const uint32_t*)(((const char*)head) + (idInfo & 0x00ffffff));
			}
			else
			{
				count = *(const uint32_t*)(((const char*)head) + (idInfo & 0x00ffffff));
				recordIds = (const uint32_t*)(((const char*)head) + (idInfo & 0x00ffffff) + sizeof(uint32_t));
			}
		}
		template<class T>
		inline bool find(const T& d, const uint32_t*& recordIds, uint32_t& count, bool equalOrGreater)const
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
			getRecordIdByIndex(this->head->keyCount - 1, recordIds, count);
			return true;
		}
		inline const void* getKey(uint32_t id)const
		{
			return  ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * id;
		}
		inline META::COLUMN_TYPE getType()const
		{
			return static_cast<META::COLUMN_TYPE>(head->type);
		}

	};
	template<class T, class INDEX_TYPE>
	class solidIndexIterator :public indexIterator<INDEX_TYPE>
	{
	private:
		uint32_t indexId;
	public:
		solidIndexIterator(uint32_t flag,INDEX_TYPE index) :indexIterator<INDEX_TYPE>(flag,index, static_cast<META::COLUMN_TYPE>(index.head->type)), indexId(0)
		{
		}
		virtual ~solidIndexIterator() {}
		virtual bool begin()
		{
			if (!index.begin(recordIds, idChildCount))
				return false;
			indexId = 0;
			innerIndexId = idChildCount - 1;
			return true;
		}
		virtual bool rbegin()
		{
			if (index.getKeyCount() == 0)
				return false;
			indexId = index.getKeyCount() - 1;
			index.getRecordIdByIndex(indexId, recordIds, idChildCount);
			innerIndexId = idChildCount - 1;
			return true;
		}
		virtual bool seek(const void* key)
		{
			int _indexId = index.find(*static_cast<const T*>(key), true);
			if (_indexId < 0)
			{
				if (!(flag & ITER_FLAG_DESC))
					return false;
				else
				{
					if (0 == (_indexId = index.getKeyCount()))
						return false;
					_indexId --;
					const void* _key = index.getKey(_indexId);
					if (*(const T*)(_key) > * (const T*)(key))
						return false;
				}
			}
			else
			{
				const void* _key = index.getKey(_indexId);
				if ((flag & ITER_FLAG_DESC)&&*(const T*)(_key) > * (const T*)(key))
				{
					if (_indexId == 0)
						return false;
					else
						_indexId--;
				}
			}
			indexId = _indexId;
			index.getRecordIdByIndex(indexId, recordIds, idChildCount);
			toLastValueOfKey();
			return true;
		}
		virtual inline const void* key()const
		{
			return index.getKey(indexId);
		}
		virtual inline bool nextKey()
		{
			if (indexId + 1 < this->index.getKeyCount())
			{
				indexId++;
				index.getRecordIdByIndex(indexId, recordIds, idChildCount);
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
				index.getRecordIdByIndex(indexId, recordIds, idChildCount);
				innerIndexId = idChildCount - 1;
				return true;
			}
			else
				return false;
		}
	};
}
