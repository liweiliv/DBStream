#include<string.h>
#include<assert.h>
#include "indexInfo.h"
#include"../util/arena.h"
#include "../message/record.h"
#include "../meta/metaData.h"
namespace STORE {
	binaryType::binaryType() :size(0),data(nullptr){

	}
	binaryType::binaryType(const char* _data, uint16_t _size) :size(_size), data(_data){

	}
	binaryType::binaryType(const binaryType & dest) :size(dest.size), data(dest.data)
	{
	}

	binaryType binaryType::operator=(const binaryType & dest)
	{
		data = dest.data;
		size = dest.size;
		return *this;
	}
	int binaryType::compare(const binaryType & dest) const
	{
		if (size == dest.size)
			return memcmp(data, dest.data, size);
		else if (size > dest.size)
		{
			if (memcmp(data, dest.data, size) >= 0)
				return -1;
			else
				return 1;
		}
		else
		{
			if (memcmp(data, dest.data, dest.size) > 0)
				return 1;
			else
				return -1;
		}
	}
	unionKey::unionKey() :key(nullptr), meta(nullptr) {}
	unionKey::unionKey(const unionKey & dest) : key(dest.key), meta(dest.meta)
	{
	}
	bool unionKeyMeta::init(const uint16_t *columnIndexs, uint16_t columnCount, const META::tableMeta* meta)
	{
		if (m_types)
			delete[]m_types;
		m_types = new uint8_t[columnCount];
		m_fixed = true;
		for (uint16_t i = 0; i < columnCount; i++)
		{
			m_types[i] = meta->getColumn(columnIndexs[i])->m_columnType;
			if (!META::columnInfos[m_types[i]].asIndex)
			{
				delete[]m_types;
				m_types = nullptr;
				return false;
			}
			if (!META::columnInfos[m_types[i]].fixed)
			{
				if (m_fixed)
					m_fixed = false;
				m_size += sizeof(uint16_t);
			}
			else
				m_size += META::columnInfos[m_types[i]].columnTypeSize;
		}
		m_keyCount = columnCount;
		return true;
	}
	int unionKey::compare(const unionKey & dest) const
	{
		assert(meta == dest.meta);
		const char * srcKey = key, *destKey = dest.key;
		for (uint16_t i = 0; i < meta->m_keyCount; i++)
		{
			switch (meta->m_types[i])
			{
			case T_UINT8:
				if (*(uint8_t*)srcKey != *(uint8_t*)destKey)
					return *(uint8_t*)srcKey - *(uint8_t*)destKey;
				break;
			case T_INT8:
				if (*(int8_t*)srcKey != *(int8_t*)destKey)
					return *(int8_t*)srcKey - *(int8_t*)destKey;
				break;
			case T_UINT16:
				if (*(uint16_t*)srcKey != *(uint16_t*)destKey)
					return *(uint16_t*)srcKey - *(uint16_t*)destKey;
				break;
			case T_INT16:
				if (*(int16_t*)srcKey != *(int16_t*)destKey)
					return *(int16_t*)srcKey - *(int16_t*)destKey;
				break;
			case T_UINT32:
				if (*(uint32_t*)srcKey != *(uint32_t*)destKey)
					return *(uint32_t*)srcKey - *(uint32_t*)destKey;
				break;
			case T_INT32:
				if (*(int32_t*)srcKey != *(int32_t*)destKey)
					return *(int32_t*)srcKey - *(int32_t*)destKey;
				break;
			case T_UINT64:
				if (*(uint64_t*)srcKey != *(uint64_t*)destKey)
				{
					if (*(uint64_t*)srcKey > *(uint64_t*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case T_INT64:
				if (*(int64_t*)srcKey != *(int64_t*)destKey)
				{
					if (*(int64_t*)srcKey > *(int64_t*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case T_FLOAT:
				if (*(float*)srcKey - *(float*)destKey > 0.000001f || *(float*)srcKey - *(float*)destKey < -0.000001f)
				{
					if (*(float*)srcKey > *(float*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case T_DOUBLE:
				if (*(double*)srcKey - *(double*)destKey > 0.000001f || *(double*)srcKey - *(double*)destKey < -0.000001f)
				{
					if (*(double*)srcKey > *(double*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case T_STRING:
			case T_BLOB:
			{
				binaryType s(srcKey + sizeof(uint16_t), *(uint16_t*)srcKey), d(destKey + sizeof(uint16_t), *(uint16_t*)destKey);
				int c = s.compare(d);
				if (c != 0)
					return c;
				break;
			}
			default:
				abort();
			}
			if (META::columnInfos[meta->m_types[i]].fixed)
			{
				srcKey += META::columnInfos[meta->m_types[i]].columnTypeSize;
				destKey += META::columnInfos[meta->m_types[i]].columnTypeSize;
			}
			else
			{
				srcKey += sizeof(uint16_t) + *(uint16_t*)srcKey;
				destKey += sizeof(uint16_t) + *(uint16_t*)destKey;
			}
		}
		return 0;
	}
	/*
	*format :
	fixed:[column1][column2]...[column n]
	var:[16bit length][if(fixed) column 1][if(var) column 2 value offset in extern data]...[column n]
	*/
	const char* unionKey::initKey(leveldb::Arena * arena, unionKeyMeta * keyMeta, uint16_t *columnIdxs, uint16_t columnCount, const DATABASE_INCREASE::DMLRecord * r, bool keyUpdated)
	{
		uint32_t keySize = 0;
		char *  key = nullptr;
		char* ptr;
		if (!keyMeta->m_fixed)
		{
			for (uint16_t idx = 0; idx < keyMeta->m_keyCount; idx++)
			{
				if (!META::columnInfos[keyMeta->m_types[idx]].fixed)
				{
					if (keyUpdated)
						keySize += r->oldVarColumnSizeOfUpdateType(columnIdxs[idx], r->oldColumnOfUpdateType(columnIdxs[idx])) + sizeof(uint16_t);
					else
						keySize += r->varColumnSize(columnIdxs[idx]);
				}
			}
			keySize += keyMeta->m_size;
			key = arena->Allocate(keySize + sizeof(uint16_t));
			ptr = key + sizeof(uint16_t);//first 16bit is length
		}
		else
		{
			key = arena->Allocate(keyMeta->m_size);
			ptr = key;
		}
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE ||
			((r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE) && !keyUpdated))
		{
			for (uint16_t i = 0; i < keyMeta->m_keyCount; i++)
			{
				if (META::columnInfos[keyMeta->m_types[i]].fixed)
				{
					memcpy(ptr, r->column(columnIdxs[i]), META::columnInfos[keyMeta->m_types[i]].columnTypeSize);
					ptr += META::columnInfos[keyMeta->m_types[i]].columnTypeSize;
				}
				else
				{
					*(uint16_t*)ptr = r->varColumnSize(i);
					memcpy(ptr + sizeof(uint16_t), r->column(columnIdxs[i]), *(uint16_t*)ptr);
					ptr += sizeof(uint16_t)+ *(uint16_t*)ptr;
				}
			}
		}
		else
		{
			for (uint16_t i = 0; i < keyMeta->m_keyCount; i++)
			{
				if (META::columnInfos[keyMeta->m_types[i]].fixed)
				{
					memcpy(ptr, r->column(columnIdxs[i]), META::columnInfos[keyMeta->m_types[i]].columnTypeSize);
					ptr += META::columnInfos[keyMeta->m_types[i]].columnTypeSize;
				}
				else
				{
					const char* v = r->column(columnIdxs[i]);
					*(uint16_t*)ptr = r->oldVarColumnSizeOfUpdateType(i, v);
					memcpy(ptr + sizeof(uint16_t), v, *(uint16_t*)ptr);
					ptr += sizeof(uint16_t) + *(uint16_t*)ptr;
				}
			}
		}
		return key;
	}
}
