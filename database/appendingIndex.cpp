/*
 * appendingIndex.cpp
 *
 *  Created on: 2019年3月12日m_ukMeta.m_size
 *      Author: liwei
 */
#include "appendingIndex.h"
#include "solidIndex.h"
namespace DATABASE {
	void appendingIndex::appendBinaryIndex(appendingIndex* index, const DATABASE_INCREASE::DMLRecord* r, uint32_t id,bool keyUpdate)
	{
		KeyTemplate<META::binaryType> c;
		c.key.data = r->column(index->m_ukMeta->columnInfo[0].columnId);
		c.key.size = r->varColumnSize(index->m_ukMeta->columnInfo[0].columnId);
		if(appendIndex(index, r, &c, id, false))
			index->m_varSize += c.key.size;
		if (keyUpdate)
		{
			c.key.data = r->oldColumnOfUpdateType(index->m_ukMeta->columnInfo[0].columnId);
			c.key.size = r->oldVarColumnSizeOfUpdateType(index->m_ukMeta->columnInfo[0].columnId, c.key.data);
			if(appendIndex(index, r, &c, id, true))
				index->m_varSize += c.key.size;
		}

	}
	void appendingIndex::appendUnionIndex(appendingIndex* index, const DATABASE_INCREASE::DMLRecord* r, uint32_t id,bool keyUpdate)
	{
		KeyTemplate<META::unionKey> c;
		uint16_t size = META::unionKey::memSize(r, index->m_ukMeta, false);
		c.key.key = index->m_arena->Allocate(size + (index->m_ukMeta->fixed?0:sizeof(uint16_t)));
		META::unionKey::initKey((char*)c.key.key, size, index->m_ukMeta, r, false);
		c.key.meta = index->m_ukMeta;
		if(appendIndex(index, r, &c, id, false)&&!index->m_ukMeta->fixed)
		{
			index->m_varSize += *(uint16_t*)c.key.key - index->m_ukMeta->size;
		}
		if(keyUpdate)
		{
			size = META::unionKey::memSize(r, index->m_ukMeta, true);
			c.key.key = index->m_arena->Allocate(size);
			META::unionKey::initKey((char*)c.key.key, size, index->m_ukMeta, r, true);
			if(appendIndex(index, r, &c, id, true)&&!index->m_ukMeta->fixed)
				index->m_varSize += *(uint16_t*)c.key.key - index->m_ukMeta->size;
		}
	}
	DLL_EXPORT appendingIndex::appendingIndex(const META::unionKeyMeta* ukMeta, const META::tableMeta* meta, leveldb::Arena* arena) :
		m_meta(meta), m_ukMeta(ukMeta), m_arena(arena), m_localArena(arena == nullptr), m_allCount(0), m_keyCount(0), m_varSize(0)
	{
		if (m_arena == nullptr)
			m_arena = new leveldb::Arena();
		if (ukMeta->columnCount > 1)
			m_type = META::COLUMN_TYPE::T_UNION;
		else
			m_type = static_cast<META::COLUMN_TYPE>(ukMeta->columnInfo[0].type);
		switch (m_type)
		{
		case META::COLUMN_TYPE::T_INT8:
			m_index = new leveldb::SkipList< KeyTemplate<int8_t>*, keyComparator<int8_t> >(keyComparator<int8_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_UINT8:
			m_index = new leveldb::SkipList< KeyTemplate<uint8_t>*, keyComparator<uint8_t> >(keyComparator<uint8_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_INT16:
		case META::COLUMN_TYPE::T_YEAR:
			m_index = new leveldb::SkipList< KeyTemplate<int16_t>*, keyComparator<int16_t> >(keyComparator<int16_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_UINT16:
			m_index = new leveldb::SkipList< KeyTemplate<uint16_t>*, keyComparator<uint16_t> >(keyComparator<uint16_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_INT32:
		case META::COLUMN_TYPE::T_DATE:
			m_index = new leveldb::SkipList< KeyTemplate<int32_t>*, keyComparator<int32_t> >(keyComparator<int32_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_UINT32:
			m_index = new leveldb::SkipList< KeyTemplate<uint32_t>*, keyComparator<uint32_t> >(keyComparator<uint32_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_INT64:
		case META::COLUMN_TYPE::T_DATETIME:
		case META::COLUMN_TYPE::T_TIME:
			m_index = new leveldb::SkipList< KeyTemplate<int64_t>*, keyComparator<int64_t> >(keyComparator<int64_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_TIMESTAMP:
		case META::COLUMN_TYPE::T_UINT64:
			m_index = new leveldb::SkipList< KeyTemplate<uint64_t>*, keyComparator<uint64_t> >(keyComparator<uint64_t>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_FLOAT:
			m_index = new leveldb::SkipList< KeyTemplate<float>*, keyComparator<float> >(keyComparator<float>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_DOUBLE:
			m_index = new leveldb::SkipList< KeyTemplate<double>*, keyComparator<double> >(keyComparator<double>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_BLOB:
		case META::COLUMN_TYPE::T_STRING:
			m_index = new leveldb::SkipList< KeyTemplate<META::binaryType>*, keyComparator<META::binaryType> >(keyComparator<META::binaryType>(), m_arena);
			break;
		case META::COLUMN_TYPE::T_UNION:
			m_index = new leveldb::SkipList< KeyTemplate<META::unionKey>*, keyComparator<META::unionKey> >(keyComparator<META::unionKey>(), m_arena);
			break;
		default:
			abort();
		}

	};
	DLL_EXPORT appendingIndex::~appendingIndex()
	{
		if (m_index != nullptr)
		{
			switch (m_type)
			{
			case META::COLUMN_TYPE::T_INT8:
				delete static_cast<leveldb::SkipList< KeyTemplate<int8_t>*, keyComparator<int8_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				delete static_cast<leveldb::SkipList< KeyTemplate<uint8_t>*, keyComparator<uint8_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_INT16:
			case META::COLUMN_TYPE::T_YEAR:
				delete static_cast<leveldb::SkipList< KeyTemplate<int16_t>*, keyComparator<int16_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				delete static_cast<leveldb::SkipList< KeyTemplate<uint16_t>*, keyComparator<uint16_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_INT32:
			case META::COLUMN_TYPE::T_DATE:
				delete static_cast<leveldb::SkipList< KeyTemplate<int32_t>*, keyComparator<int32_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				delete static_cast<leveldb::SkipList< KeyTemplate<uint32_t>*, keyComparator<uint32_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_INT64:
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_TIME:
				delete static_cast<leveldb::SkipList< KeyTemplate<int64_t>*, keyComparator<int64_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				delete static_cast<leveldb::SkipList< KeyTemplate<uint64_t>*, keyComparator<uint64_t> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				delete static_cast<leveldb::SkipList< KeyTemplate<float>*, keyComparator<float> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				delete static_cast<leveldb::SkipList< KeyTemplate<double>*, keyComparator<double> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_STRING:
			case META::COLUMN_TYPE::T_BLOB:
				delete static_cast<leveldb::SkipList< KeyTemplate<META::binaryType>*, keyComparator<META::binaryType> >*>(m_index);
				break;
			case META::COLUMN_TYPE::T_UNION:
				delete static_cast<leveldb::SkipList< KeyTemplate<META::unionKey>*, keyComparator<META::unionKey> >*>(m_index);
				break;
			default:
				abort();
			}
		}
		if (m_localArena && m_arena != nullptr)
			delete m_arena;
	}
	typename appendingIndex::appendIndexFunc appendingIndex::m_appendIndexFuncs[] = {
		appendUnionIndex,//T_UNION
		appendIndexByType<uint8_t>,//T_UINT8
		appendIndexByType<int8_t>,//T_INT8
		appendIndexByType<uint16_t>,//T_UINT16
		appendIndexByType<int16_t>,//T_INT16
		appendIndexByType<uint32_t>,//T_UINT32
		appendIndexByType<int32_t>,//T_INT32
		appendIndexByType<uint64_t>,//T_UINT64
		appendIndexByType<int64_t>,//T_INT64
		nullptr,//T_BIG_NUMBER
		appendIndexByType<float>,//T_FLOAT
		appendIndexByType<double>,//T_DOUBLE
		nullptr,//T_DECIMAL
		appendIndexByType<uint64_t>,//T_TIMESTAMP
		appendIndexByType<int64_t>,//T_DATETIME
		appendIndexByType<int32_t>,//T_DATE
		appendIndexByType<int16_t>,//T_YEAR
		appendIndexByType<int64_t>,//T_TIME
		appendBinaryIndex ,//T_BLOB
		appendBinaryIndex,//T_STRING
		nullptr,//T_JSON
		nullptr,//T_XML
		nullptr,//T_GEOMETRY
		nullptr, //T_SET
		nullptr,//T_ENUM
		nullptr,//T_BYTE
		appendBinaryIndex,//T_BINARY
		appendBinaryIndex,//T_TEXT
		nullptr//T_BOOL
	};
	DLL_EXPORT void  appendingIndex::append(const DATABASE_INCREASE::DMLRecord* r, uint32_t id)
	{
		if ((r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_UPDATE) ||
				r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_REPLACE))&&r->isKeyUpdated(m_ukMeta)
				)
		{
			m_appendIndexFuncs[TID(m_type)](this, r, id,true);
			m_allCount += 2;
		}
		else
		{
			m_appendIndexFuncs[TID(m_type)](this, r, id,false);
			m_allCount++;
		}
	}
	template<>
	void appendingIndex::createFixedSolidIndex<META::unionKey>(char* data, appendingIndex::iterator<META::unionKey>& iter, uint16_t keySize)
	{
		char* indexPos = data + sizeof(struct solidIndexHead), * externCurretPos = indexPos + (keySize +sizeof(uint32_t)) * (m_keyCount+1);
		((solidIndexHead*)(data))->flag = SOLID_INDEX_FLAG_FIXED;
		((solidIndexHead*)(data))->length = keySize;
		((solidIndexHead*)(data))->keyCount = m_keyCount;
		((solidIndexHead*)(data))->type = static_cast<int8_t>(META::COLUMN_TYPE::T_UNION);
		do
		{
			const keyChildInfo* k = iter.keyDetail();
			memcpy(indexPos, static_cast<const META::unionKey*>(iter.key())->key, keySize);
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
				memcpy(externCurretPos, k->subArray, sizeof(uint32_t) * k->count);
				externCurretPos+=sizeof(uint32_t) * k->count;
			}
			indexPos += keySize + sizeof(uint32_t);
		} while (iter.nextKey());
		((solidIndexHead*)(data))->size = externCurretPos-data;
	}
	template<>
	DLL_EXPORT void appendingIndex::createVarSolidIndex<META::unionKey>(char* data, appendingIndex::iterator<META::unionKey>& iter)
	{
		char* indexPos = data + sizeof(struct solidIndexHead), * externCurretPos = indexPos + +sizeof(uint32_t) * (m_keyCount+1);
		((solidIndexHead*)(data))->flag = 0;
		((solidIndexHead*)(data))->length = sizeof(uint32_t);
		((solidIndexHead*)(data))->keyCount = m_keyCount;
		((solidIndexHead*)(data))->type = TID(META::COLUMN_TYPE::T_UNION);
		do
		{
			const keyChildInfo* k = iter.keyDetail();
			*(uint32_t*)indexPos = externCurretPos - data;
			indexPos += sizeof(uint32_t);
			memcpy(externCurretPos, static_cast<const META::unionKey*>(iter.key())->key, sizeof(uint16_t)+*(const uint16_t*)(static_cast<const META::unionKey*>(iter.key())->key));
			externCurretPos += sizeof(uint16_t) + *(uint16_t*)externCurretPos;
			*(uint32_t*)externCurretPos = k->count;
			memcpy(externCurretPos + sizeof(uint32_t), k->subArray, sizeof(uint32_t) * k->count);
			externCurretPos += sizeof(uint32_t) + sizeof(uint32_t) * k->count;
		} while (iter.nextKey());
		*(uint32_t*)indexPos = externCurretPos - data;
		((solidIndexHead*)(data))->size = externCurretPos-data;
	}
	template<>
	DLL_EXPORT void appendingIndex::createVarSolidIndex<META::binaryType>(char* data, appendingIndex::iterator<META::binaryType>& iter)
	{
		char* indexPos = data + sizeof(solidIndexHead), * externCurretPos = indexPos + sizeof(uint32_t) * (m_keyCount + 1);
		((solidIndexHead*)(data))->flag = 0;
		((solidIndexHead*)(data))->length = sizeof(uint32_t);
		((solidIndexHead*)(data))->keyCount = m_keyCount;
		((solidIndexHead*)(data))->type = TID(META::COLUMN_TYPE::T_BINARY);
		do
		{
			const keyChildInfo* k = iter.keyDetail();
			*(uint32_t*)indexPos = externCurretPos - data;
			indexPos += sizeof(uint32_t);
			*(uint16_t*)externCurretPos = static_cast<const META::binaryType*>(iter.key())->size;
			memcpy(externCurretPos + sizeof(uint16_t), static_cast<const META::binaryType*>(iter.key())->data, *(uint16_t*)externCurretPos);
			externCurretPos += static_cast<const META::binaryType*>(iter.key())->size + sizeof(uint16_t);
			*(uint32_t*)externCurretPos = k->count;
			memcpy(externCurretPos + sizeof(uint32_t), k->subArray, sizeof(uint32_t) * k->count);
			externCurretPos += sizeof(uint32_t) + sizeof(uint32_t) * k->count;
		} while (iter.nextKey());
		*(uint32_t*)indexPos = externCurretPos - data;
		((solidIndexHead*)(data))->size = externCurretPos-data;
	}


}

