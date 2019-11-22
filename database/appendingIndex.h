/*
 * appendingIndex.h
 *
 *  Created on: 2019年2月21日
 *      Author: liwei
 */

#ifndef APPENDINGINDEX_H_
#define APPENDINGINDEX_H_
#include <stdint.h>
#include <string.h>
#include "util/skiplist.h"
#include "meta/metaData.h"
#include "message/record.h"
#include "iterator.h"
#include "meta/columnType.h"
#include "indexIterator.h"
#include "solidIndex.h"
namespace DATABASE {
	/*every key may have multi record in one block,use keyChildInfo to save those records*/
	struct keyChildInfo {
		uint32_t *subArray;
		uint32_t arraySize; //volumn
		uint32_t count;     //current record count
	};

	template <typename T>

	struct KeyTemplate
	{
		T key;
		keyChildInfo child;
	};

	template <typename T>
	struct keyComparator
	{
		keyComparator(){}
		inline int operator()(const KeyTemplate<T> * a, const KeyTemplate<T> * b) const
		{
			if (a->key < b->key)
				return -1;
			else if (a->key > b->key)
				return +1;
			else
				return 0;
		}
	};

	template <>
	struct keyComparator<double>
	{
		keyComparator(){}
		inline int operator()(const KeyTemplate<double> * a, const KeyTemplate<double> * b) const
		{
			if (a->key < 0.000000001f + b->key)
				return -1;
			else if (a->key > b->key + 0.000000001f)
				return +1;
			else
				return 0;
		}
	};
	template <>
	struct keyComparator<float>
	{
		keyComparator(){}
		inline int operator()(const KeyTemplate<float> * a, const KeyTemplate<float> * b) const
		{
			if (a->key < 0.000001f + b->key)
				return -1;
			else if (a->key > b->key + 0.000001f)
				return +1;
			else
				return 0;
		}
	};
	template <>
	struct keyComparator<META::unionKey>
	{
		keyComparator(){}
		inline int operator()(const KeyTemplate<META::unionKey>* a, const KeyTemplate<META::unionKey>* b) const
		{
			return a->key.compare(b->key);
		}
	};
	template <>
	struct keyComparator<META::binaryType>
	{
		keyComparator(){}
		inline int operator()(const KeyTemplate<META::binaryType>* a, const KeyTemplate<META::binaryType>* b) const
		{
			return a->key.compare(b->key);
		}
	};
	class appendingIndex {
	private:
		META::COLUMN_TYPE m_type;
		void * m_index;
		const META::tableMeta* m_meta;
		const META::unionKeyMeta *m_ukMeta;
		leveldb::Arena *m_arena;
		bool m_localArena;
		uint32_t m_allCount;
		uint32_t m_keyCount;
		uint32_t m_varSize;
		typedef void(*appendIndexFunc) (appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		template <typename T>
		static inline bool appendIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, KeyTemplate<T> *c, uint32_t id, bool keyUpdated = false)
		{
			bool newKey = true;
			KeyTemplate<T> *k = nullptr;
			typename leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >::Iterator iter(static_cast<leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> > *>(index->m_index));
			iter.Seek(c);
			if (!iter.Valid() || iter.key()->key > c->key)
			{
				k = (KeyTemplate<T> *)index->m_arena->AllocateAligned(sizeof(KeyTemplate<T>));
				k->key = c->key;
				k->child.subArray = (uint32_t*)index->m_arena->AllocateAligned(sizeof(uint32_t)*(k->child.arraySize = 1));
				k->child.count = 0;
				static_cast<leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> > *>(index->m_index)->Insert(k);
				index->m_keyCount++;
			}
			else
			{
				assert(c->key == iter.key()->key);
				k = (KeyTemplate<T> *)iter.key();
				newKey = false;
			}
			if (k->child.count >= k->child.arraySize)
			{
				/*do not free alloced memory ,only copy*/
				uint32_t * tmp = (uint32_t*)index->m_arena->AllocateAligned(sizeof(uint32_t)*(k->child.arraySize * 2));
				memcpy(tmp, k->child.subArray, sizeof(uint32_t)*k->child.arraySize);
				k->child.arraySize *= 2;
				k->child.subArray = tmp;
			}
			k->child.subArray[k->child.count] = id;
			barrier;
			k->child.count++;
			return newKey;
		}

		static inline void appendUint8Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendInt8Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendUint16Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendInt16Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendUint32Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendInt32Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendUint64Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendInt64Index(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendFloatIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendDoubleIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendBinaryIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		static inline void appendUnionIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
	public:
		static appendIndexFunc m_appendIndexFuncs[];
	public:
		DLL_EXPORT appendingIndex(const META::unionKeyMeta * ukMeta, const META::tableMeta * meta, leveldb::Arena *arena = nullptr);
		DLL_EXPORT ~appendingIndex();
		inline uint32_t getKeyCount()
		{
			return m_keyCount;
		}
		inline META::COLUMN_TYPE getType()
		{
			return m_type;
		}
		inline  const META::unionKeyMeta* getUkMeta() {
			return m_ukMeta;
		}
		inline const META::tableMeta* getMeta() {
			return m_meta;
		}
		template<typename T>
		inline int32_t find(const void * key)
		{
			KeyTemplate<T> k;
			k.key = *static_cast<const T*>(key);
			typename leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >::Iterator iter(static_cast<leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >*>(m_index));
			iter.Seek(&k);
			if (!iter.Valid() || iter.key()->key > * static_cast<const T*>(key))
				return -1;
			else
				k = *(KeyTemplate<T>*)iter.key();
			int count = k.child.count;
			return k.child.subArray[count-1];
		}
		DLL_EXPORT void append(const DATABASE_INCREASE::DMLRecord  * r, uint32_t id);
		template <typename T>
		class iterator:public indexIterator<appendingIndex> {
		private:
			typename leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >::Iterator m_iter;
		public:
			iterator(uint32_t flag,appendingIndex* index) :indexIterator<appendingIndex>(flag,index, index->m_type), m_iter(static_cast<leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >*>(index->m_index))
			{
			}
			inline bool begin()
			{
				m_iter.SeekToFirst();
				if (!m_iter.Valid())
					return false;
				if (0 == m_iter.key()->child.count)
					return false;
				recordIds = m_iter.key()->child.subArray;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline bool rbegin()
			{
				m_iter.SeekToLast();
				if (!m_iter.Valid())
					return false;
				if (0 == m_iter.key()->child.count)
					return false;
				recordIds = m_iter.key()->child.subArray;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline bool valid()
			{
				return m_iter.Valid();
			}
			inline bool seekIncrease(KeyTemplate<T>& k)
			{
				m_iter.Seek(&k);
				if (!m_iter.Valid())
					return false;
				if (0 == m_iter.key()->child.count)
					return false;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline bool seekDecrease(KeyTemplate<T>& k)
			{
				m_iter.Seek(&k);
				if (!m_iter.Valid() )
				{
					m_iter.SeekToLast();
					if (!m_iter.Valid())
						return false;
					if (m_iter.key()->key > k.key)
						return false;
				}
				else if (m_iter.key()->key > k.key)
				{
					m_iter.Prev();
					if (!m_iter.Valid())
						return false;
					if (m_iter.key()->key > k.key)
						return false;
				}
				if (0 == m_iter.key()->child.count)
					return false;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline bool seek(const void* key)
			{
				KeyTemplate<T> k;
				k.key = *static_cast<const T*>(key);
				if (likely(!(flag & ITER_FLAG_DESC)))
					return seekIncrease(k);
				else
					return seekDecrease(k);
			}
			inline bool nextKey()
			{
				m_iter.Next();
				if (!m_iter.Valid())
					return false;
				recordIds = m_iter.key()->child.subArray;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline bool prevKey()
			{
				m_iter.Prev();
				if (!m_iter.Valid())
					return false;
				recordIds = m_iter.key()->child.subArray;
				idChildCount = m_iter.key()->child.count;
				innerIndexId = idChildCount - 1;
				return true;
			}
			inline const void* key() const
			{
				return  &m_iter.key()->key;
			}
			inline const keyChildInfo* keyDetail()
			{
				return  &m_iter.key()->child;
			}
		};
		/*
		indexPos: every index length is [key size]+[4byte]
		format [key1][if record count =1: record id][key 2 if record count >1 &&<0x7f :[[1 byte record count][3byte record array offset]]]
		[key 3 if record count >=0x80 [0x80][3byte record array offset]]
		extern pos:
		[if record count of key <=0x7f,[record id 1][record id 2]...[record id n]]...[if record count of key >=0x80,[4 byte record count][record id 1][record id 2]...[record id n]]]
		*/
		template<typename T>
		void createFixedSolidIndex(char * data, iterator<T> &iter, uint16_t keySize)
		{
			char* indexPos = data + sizeof(struct solidIndexHead), * externCurretPos = indexPos + (keySize +sizeof(uint32_t)) * (m_keyCount+1);
			((solidIndexHead*)(data))->flag = SOLID_INDEX_FLAG_FIXED;
			((solidIndexHead*)(data))->length = keySize;
			((solidIndexHead*)(data))->keyCount = m_keyCount;
			((solidIndexHead*)(data))->type = static_cast<int8_t>(m_type);
			do
			{
				const keyChildInfo * k = iter.keyDetail();
				memcpy(indexPos, iter.key(), keySize);
				if (k->count == 1)
				{
					*(uint32_t*)(indexPos + keySize) = k->subArray[0];
				}
				else
				{
					if (k->count < 0x7f)
					{
						*(uint32_t*)(indexPos + keySize) = (k->count << 24) | (externCurretPos - data);
					}
					else
					{
						*(uint32_t*)(indexPos + keySize) = (0x80u << 24) | (externCurretPos - data);
						*(uint32_t*)externCurretPos = k->count;
						externCurretPos += sizeof(uint32_t);
					}
					memcpy(externCurretPos, k->subArray, sizeof(uint32_t)*k->count);
					externCurretPos += sizeof(uint32_t)*k->count;
				}
				indexPos += keySize + sizeof(uint32_t);
			} while (iter.nextKey());
			((solidIndexHead*)(data))->size = externCurretPos-data;
		}

		template<typename T>
		void createVarSolidIndex(char * data, iterator<T> &iter)
		{
			abort();
		}
		inline uint32_t toSolidIndexSize()
		{
			uint32_t size = 0;
			uint16_t keySize = 0;
			bool fixed = false;
			if (m_ukMeta->columnCount == 1)
			{
				keySize = META::columnInfos[m_ukMeta->columnInfo[0].type].columnTypeSize;
				fixed = META::columnInfos[m_ukMeta->columnInfo[0].type].fixed;
			}
			else
			{
				keySize = m_ukMeta->size;
				fixed = m_ukMeta->fixed;
			}
			if (fixed)
				size = sizeof(solidIndexHead) + (keySize + sizeof(uint32_t)) * (1+m_keyCount) + sizeof(uint32_t) * (m_allCount) + ((m_allCount-m_keyCount)/0x7f)*sizeof(uint32_t);
			else
				size = sizeof(solidIndexHead) + (keySize + 2*sizeof(uint32_t)) * (1+m_keyCount) + (sizeof(uint16_t)+sizeof(uint32_t)) * (m_allCount) + m_varSize;
			return size;
		}
		template<typename T>
		const char *toString(char * data = nullptr)
		{
			uint16_t keySize = 0;
			bool fixed = false;
			if (m_ukMeta->columnCount == 1)
			{
				keySize = META::columnInfos[m_ukMeta->columnInfo[0].type].columnTypeSize;
				fixed = META::columnInfos[m_ukMeta->columnInfo[0].type].fixed;
			}
			else
			{
				keySize = m_ukMeta->size;
				fixed = m_ukMeta->fixed;
			}
			iterator<T> iter(0,this);
			if (!iter.begin() || !iter.valid())
				return nullptr;//no data
			if (data == nullptr)
				data = (char*)basicBufferPool::allocDirect(toSolidIndexSize());
			if (fixed)
				createFixedSolidIndex(data, iter, keySize);
			else
				createVarSolidIndex<T>(data, iter);
			return data;
		}
	};
	template<>
	DLL_EXPORT void appendingIndex::createFixedSolidIndex<META::unionKey>(char * data, appendingIndex::iterator<META::unionKey> &iter, uint16_t keySize);
	template<>
	DLL_EXPORT void appendingIndex::createVarSolidIndex<META::unionKey>(char * data, appendingIndex::iterator<META::unionKey> &iter);
	template<>
	DLL_EXPORT void appendingIndex::createVarSolidIndex<META::binaryType>(char * data, appendingIndex::iterator<META::binaryType> &iter);
}
#endif /* APPENDINGINDEX_H_ */

