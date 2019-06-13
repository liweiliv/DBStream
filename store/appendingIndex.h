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
#include "indexInfo.h"
#include "../util/skiplist.h"
#include "../meta/metaData.h"
#include "../message/record.h"
#include "iterator.h"
#include "../meta/columnType.h"
namespace STORE {
	struct keyChildInfo {
		uint32_t *subArray;
		uint32_t arraySize;
		uint32_t count;
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
	class appendingIndex {
	private:
		uint8_t m_type;
		void * m_index;
		uint16_t *m_columnIdxs;
		uint16_t m_columnCount;
		META::tableMeta* m_meta;
		unionKeyMeta  m_ukMeta;
		leveldb::Arena *m_arena;
		bool m_localArena;
		void* m_comp;
		uint32_t m_allCount;
		uint32_t m_keyCount;
		uint32_t m_varSize;
		typedef void(*appendIndexFunc) (appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, uint32_t id);
		template <typename T>
		static inline void appendIndex(appendingIndex * index, const DATABASE_INCREASE::DMLRecord * r, KeyTemplate<T> *c, uint32_t id, bool keyUpdated = false)
		{
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
				k = (KeyTemplate<T> *)iter.key();
			if (k->child.count >= k->child.arraySize)
			{
				uint32_t * tmp = (uint32_t*)index->m_arena->AllocateAligned(sizeof(uint32_t)*(k->child.arraySize * 2));
				memcpy(tmp, k->child.subArray, sizeof(uint32_t)*k->child.arraySize);
				k->child.arraySize *= 2;
				k->child.subArray = tmp;
			}
			k->child.subArray[k->child.count] = id;
			barrier;
			k->child.count++;
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
		appendingIndex(uint16_t *columnIdxs, uint16_t columnCount, META::tableMeta * meta, leveldb::Arena *arena = nullptr);
		~appendingIndex();
		void append(const DATABASE_INCREASE::DMLRecord  * r, uint32_t id);
		template <typename T>
		class iterator {
		private:
			appendingIndex * m_index;
			typename leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> >::Iterator m_iter;
			uint32_t m_childIdx;
			const keyChildInfo * m_key;
		public:
			iterator(appendingIndex * index) :m_index(index), m_iter(static_cast<leveldb::SkipList< KeyTemplate<T>*, keyComparator<T> > *>(index->m_index)), m_childIdx(0), m_key(nullptr)
			{
			}
			inline bool begin()
			{
				m_iter.SeekToFirst();
				if (!m_iter.Valid())
					return false;
				if (0 == m_iter.key()->child.count)
					return false;
				m_key = &m_iter.key()->child;
				m_childIdx = 0;
				return true;
			}
			inline bool valid()
			{
				return m_iter.Valid();
			}
			inline bool seek(const T & key)
			{
				KeyTemplate<T> k;
				k.key = key;
				m_iter.Seek(&k);
				if (!m_iter.Valid() || m_iter.key()->key > key)
					return false;
				if (0 == m_iter.key()->child.count)
					return false;
				m_key = &m_iter.key()->child;
				m_childIdx = 0;
				return true;
			}
			inline bool nextKey()
			{
				m_childIdx = 0;
				m_iter.Next();
				if (!m_iter.Valid())
					return false;
				m_key = &m_iter.key()->child;
				return true;
			}
			inline bool nextValueOfKey()
			{
				if (m_childIdx >= m_key->count)
					return false;
				else
					m_childIdx++;
				return true;
			}
			inline uint32_t value() const
			{
				return m_key->subArray[m_childIdx];
			}
			inline const T & key() const
			{
				return  m_iter.key()->key;
			}
			inline const keyChildInfo * keyDetail() const
			{
				return m_key;
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
			char * indexPos = data + sizeof(uint32_t), *externCurretPos = data + keySize * m_keyCount;
			*(uint32_t*)data = m_keyCount;
			do
			{
				const keyChildInfo * k = iter.keyDetail();
				memcpy(indexPos, &iter.key(), keySize);
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
				}
				indexPos += keySize + sizeof(uint32_t);
			} while (iter.nextKey());
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
			if (m_columnCount == 1)
			{
				keySize = columnInfos[m_columnIdxs[0]].columnTypeSize;
				fixed = columnInfos[m_columnIdxs[0]].fixed;
			}
			else
			{
				keySize = m_ukMeta.m_size;
				fixed = m_ukMeta.m_fixed;
			}
			if (fixed)
				size = (keySize + sizeof(uint32_t))*m_keyCount + sizeof(uint32_t)*(m_allCount - m_keyCount) * 2;
			else
				size = (keySize + sizeof(uint32_t))*m_keyCount + sizeof(uint32_t)*(m_allCount - m_keyCount) * 2 + m_varSize;
			return size;
		}
		template<typename T>
		const char *toString(char * data = nullptr)
		{
			uint16_t keySize = 0;
			bool fixed = false;
			if (m_columnCount == 1)
			{
				keySize = columnInfos[m_columnIdxs[0]].columnTypeSize;
				fixed = columnInfos[m_columnIdxs[0]].fixed;
			}
			else
			{
				keySize = m_ukMeta.m_size;
				fixed = m_ukMeta.m_fixed;
			}
			iterator<T> iter(this);
			if (!iter.begin() || !iter.valid())
				return nullptr;//no data
			if (data == nullptr)
				data = (char*)malloc(toSolidIndexSize());
			if (fixed)
				createFixedSolidIndex(data, iter, keySize);
			else
				createVarSolidIndex(data, iter);
			return data;
		}
	};
	template<>
	void appendingIndex::createFixedSolidIndex<unionKey>(char * data, appendingIndex::iterator<unionKey> &iter, uint16_t keySize);
	template<>
	void appendingIndex::createVarSolidIndex<unionKey>(char * data, appendingIndex::iterator<unionKey> &iter);
	template<>
	void appendingIndex::createVarSolidIndex<binaryType>(char * data, appendingIndex::iterator<binaryType> &iter);
}
#endif /* APPENDINGINDEX_H_ */

