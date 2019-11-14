#include<string.h>
#include<assert.h>
#include "columnType.h"
#include"util/arena.h"
#include "message/record.h"
#include "meta/metaData.h"
namespace META {
	DLL_EXPORT binaryType::binaryType() :size(0), data(nullptr) {

	}
	DLL_EXPORT binaryType::binaryType(const char* _data, uint16_t _size) : size(_size), data(_data) {

	}
	DLL_EXPORT binaryType::binaryType(const binaryType& dest) : size(dest.size), data(dest.data)
	{
	}

	DLL_EXPORT binaryType binaryType::operator=(const binaryType& dest)
	{
		data = dest.data;
		size = dest.size;
		return *this;
	}
	DLL_EXPORT int binaryType::compare(const binaryType& dest) const
	{
		if (size == dest.size)
			return memcmp(data, dest.data, size);
		else if (size < dest.size)
		{
			if (memcmp(data, dest.data, size) > 0)
				return 1;
			else
				return -1;
		}
		else
		{
			if (memcmp(data, dest.data, dest.size) >= 0)
				return 1;
			else
				return -1;
		}
	}
	DLL_EXPORT unionKey::unionKey() :key(nullptr), meta(nullptr) {}
	DLL_EXPORT unionKey::unionKey(const unionKey& dest) : key(dest.key), meta(dest.meta)
	{
	}
	DLL_EXPORT int unionKey::compare(const unionKey& dest) const
	{
		assert(meta == dest.meta);
		const char* srcKey = key + (dest.meta->fixed?0:2), * destKey = dest.key + (dest.meta->fixed ? 0 : 2);
		for (uint16_t i = 0; i < meta->columnCount; i++)
		{
			switch (static_cast<META::COLUMN_TYPE>(meta->columnInfo[i].type))
			{
			case META::COLUMN_TYPE::T_UINT8:
				if (*(uint8_t*)srcKey != *(uint8_t*)destKey)
					return *(uint8_t*)srcKey - *(uint8_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_INT8:
				if (*(int8_t*)srcKey != *(int8_t*)destKey)
					return *(int8_t*)srcKey - *(int8_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_UINT16:
				if (*(uint16_t*)srcKey != *(uint16_t*)destKey)
					return *(uint16_t*)srcKey - *(uint16_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_INT16:
				if (*(int16_t*)srcKey != *(int16_t*)destKey)
					return *(int16_t*)srcKey - *(int16_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_UINT32:
				if (*(uint32_t*)srcKey != *(uint32_t*)destKey)
					return *(uint32_t*)srcKey - *(uint32_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_INT32:
				if (*(int32_t*)srcKey != *(int32_t*)destKey)
					return *(int32_t*)srcKey - *(int32_t*)destKey;
				break;
			case META::COLUMN_TYPE::T_UINT64:
				if (*(uint64_t*)srcKey != *(uint64_t*)destKey)
				{
					if (*(uint64_t*)srcKey > * (uint64_t*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case META::COLUMN_TYPE::T_INT64:
				if (*(int64_t*)srcKey != *(int64_t*)destKey)
				{
					if (*(int64_t*)srcKey > * (int64_t*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				if (*(float*)srcKey - *(float*)destKey > 0.000001f || *(float*)srcKey - *(float*)destKey < -0.000001f)
				{
					if (*(float*)srcKey > * (float*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				if (*(double*)srcKey - *(double*)destKey > 0.000001f || *(double*)srcKey - *(double*)destKey < -0.000001f)
				{
					if (*(double*)srcKey > * (double*)destKey)
						return 1;
					else
						return -1;
				}
				break;
			case META::COLUMN_TYPE::T_STRING:
			case META::COLUMN_TYPE::T_BLOB:
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
			if (META::columnInfos[TID(meta->columnInfo[i].type)].fixed)
			{
				srcKey += META::columnInfos[TID(meta->columnInfo[i].type)].columnTypeSize;
				destKey += META::columnInfos[TID(meta->columnInfo[i].type)].columnTypeSize;
			}
			else
			{
				srcKey += sizeof(uint16_t) + *(uint16_t*)srcKey;
				destKey += sizeof(uint16_t) + *(uint16_t*)destKey;
			}
		}
		return 0;
	}
	DLL_EXPORT uint16_t unionKey::memSize(const DATABASE_INCREASE::DMLRecord* r, const META::unionKeyMeta* meta, bool keyUpdated)
	{
		if (!meta->fixed)
		{
			uint16_t keySize = meta->size;
			for (uint16_t idx = 0; idx < meta->columnCount; idx++)
			{
				if (!META::columnInfos[TID(meta->columnInfo[idx].type)].fixed)
				{
					if (keyUpdated)
						keySize += r->oldVarColumnSizeOfUpdateType(meta->columnInfo[idx].columnId, r->oldColumnOfUpdateType(meta->columnInfo[idx].columnId));
					else
						keySize += r->varColumnSize(meta->columnInfo[idx].columnId);
				}
			}
			return keySize;
		}
		else
		{
			return meta->size;
		}
	}
	DLL_EXPORT void unionKey::initKey(char* key, uint16_t size, const META::unionKeyMeta* keyMeta, const DATABASE_INCREASE::DMLRecord* r, bool keyUpdated)
	{
		char* ptr = key;
		if (!keyMeta->fixed)
		{
			*(uint16_t*)key = size;
			ptr += sizeof(uint16_t);
		}
		if (r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_INSERT) ||
				r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE) ||
			((r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_UPDATE) ||
					r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_REPLACE)) && !keyUpdated))
		{
			for (uint16_t i = 0; i < keyMeta->columnCount; i++)
			{
				if (META::columnInfos[static_cast<int>(keyMeta->columnInfo[i].type)].fixed)
				{
					memcpy(ptr, r->column(keyMeta->columnInfo[i].columnId), META::columnInfos[keyMeta->columnInfo[i].type].columnTypeSize);
					ptr += META::columnInfos[keyMeta->columnInfo[i].type].columnTypeSize;
				}
				else
				{
					*(uint16_t*)ptr = r->varColumnSize(keyMeta->columnInfo[i].columnId);
					memcpy(ptr + sizeof(uint16_t), r->column(keyMeta->columnInfo[i].columnId), *(uint16_t*)ptr);
					ptr += sizeof(uint16_t) + *(uint16_t*)ptr;
				}
			}
		}
		else
		{
			for (uint16_t i = 0; i < keyMeta->columnCount; i++)
			{
				if (META::columnInfos[static_cast<int>(keyMeta->columnInfo[i].type)].fixed)
				{
					memcpy(ptr, r->oldColumnOfUpdateType(keyMeta->columnInfo[i].columnId), META::columnInfos[static_cast<int>(keyMeta->columnInfo[i].type)].columnTypeSize);
					ptr += META::columnInfos[static_cast<int>(keyMeta->columnInfo[i].type)].columnTypeSize;
				}
				else
				{
					const char* v = r->oldColumnOfUpdateType(keyMeta->columnInfo[i].columnId);
					*(uint16_t*)ptr = r->oldVarColumnSizeOfUpdateType(i, v);
					memcpy(ptr + sizeof(uint16_t), v, *(uint16_t*)ptr);
					ptr += sizeof(uint16_t) + *(uint16_t*)ptr;
				}
			}
		}
	}
}
