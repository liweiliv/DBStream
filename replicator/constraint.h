#pragma once
#include <stdint.h>
#include "meta/metaData.h"
#include "util/sparsepp/spp.h"
#include "message/record.h"
#include "util/itoaSse.h"
#include "util/dtoa.h"
#include "messageWrap.h"
namespace REPLICATOR {
	static constexpr auto HASH_SEED = 31; // 31 131 1313 13131 131313 etc.. 37
	static inline uint32_t bkdrHash(uint32_t hash, const char* key, uint32_t length)
	{
		const char* str = key;
		while (str - key < length)
			hash = hash * HASH_SEED + (*str++);
		return hash;
	}
	static inline uint32_t bkdrHash(uint32_t hash, const char* key)
	{
		const char* str = key;
		while (*(str) != '\0')
			hash = hash * HASH_SEED + (*str++);
		return hash;
	}
	static inline uint32_t getUnionKeyHash(DATABASE_INCREASE::DMLRecord* record, const META::unionKeyMeta* key, bool newOrOld)
	{
		uint32_t hash = 0;
		for (uint16_t idx = 0; idx < key->columnCount; idx++)
		{
			uint8_t type = static_cast<uint8_t>(record->meta->m_columns[key->columnInfo[idx].columnId].m_columnType);
			const char* v;
			if (likely(newOrOld))
				v = record->column(key->columnInfo[idx].columnId);
			else
				v = record->oldColumnOfUpdateType(key->columnInfo[idx].columnId);
			if (v != nullptr)
			{
				if (META::columnInfos[type].fixed)
					hash = bkdrHash(hash, v, META::columnInfos[type].columnTypeSize);
				else
					hash = bkdrHash(hash, v, record->varColumnSize(key->columnInfo[idx].columnId));
			}
		}
		return hash;
	}
	static inline uint32_t getHash(DATABASE_INCREASE::DMLRecord* record, const META::unionKeyMeta* key, bool newOrOld)
	{
		if (key->columnCount == 1)
		{
			const char* value;
			if (likely(newOrOld))
				value = record->column(key->columnInfo[0].columnId);
			else
				value = record->oldColumnOfUpdateType(key->columnInfo[0].columnId);
			if (value == nullptr)
				return 0;
			switch (static_cast<META::COLUMN_TYPE>(key->columnInfo[0].type))
			{
			case META::COLUMN_TYPE::T_STRING:
			case META::COLUMN_TYPE::T_TEXT:
			case META::COLUMN_TYPE::T_BINARY:
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_BYTE:
			case META::COLUMN_TYPE::T_GEOMETRY:
			case META::COLUMN_TYPE::T_JSON:
			case META::COLUMN_TYPE::T_XML:
			case META::COLUMN_TYPE::T_DECIMAL:
			case META::COLUMN_TYPE::T_BIG_NUMBER:
				return bkdrHash(0, value, record->varColumnSize(key->columnInfo[0].columnId));
			case META::COLUMN_TYPE::T_FLOAT:
			{
				char floatBuffer[256];
				my_fcvt(*(const float*)value, record->meta->m_columns[key->columnInfo[0].columnId].m_decimals, floatBuffer, nullptr);
				return bkdrHash(0, floatBuffer);
			}
			case META::COLUMN_TYPE::T_DOUBLE:
			{
				char floatBuffer[256];
				my_fcvt(*(const double*)value, record->meta->m_columns[key->columnInfo[0].columnId].m_decimals, floatBuffer, nullptr);
				return bkdrHash(0, floatBuffer);
			}
			default:
			{
				if (META::columnInfos[key->columnInfo[0].type].fixed)
				{
					return bkdrHash(0, value, META::columnInfos[static_cast<uint8_t>(record->meta->m_columns[key->columnInfo[0].columnId].m_columnType)].columnTypeSize);
				}
				else
				{
					return bkdrHash(0, value, record->varColumnSize(key->columnInfo[0].columnId));
				}
			}
			}
		}
		else
		{
			return getUnionKeyHash(record, key, newOrOld);
		}
	}
	static inline bool compareUnionKey(const META::tableMeta* meta, const META::unionKeyMeta* key, DATABASE_INCREASE::DMLRecord* src, DATABASE_INCREASE::DMLRecord* dest)
	{
		for (uint16_t idx = 0; idx < key->columnCount; idx++)
		{
			uint8_t type = static_cast<uint8_t>(meta->m_columns[key->columnInfo[idx].columnId].m_columnType);
			const char* srcValue = src->column(key->columnInfo[idx].columnId), * destValue = dest->column(key->columnInfo[idx].columnId);
			if (unlikely(src == nullptr && dest == nullptr))
				return true;
			else if (unlikely(src == nullptr || dest == nullptr))
				return false;
			if (META::columnInfos[type].fixed)
			{
				if (memcmp(srcValue, destValue, META::columnInfos[static_cast<uint8_t>(meta->m_columns[key->columnInfo[idx].columnId].m_columnType)].columnTypeSize) != 0)
					return false;
			}
			else
			{
				if (src->varColumnSize(key->columnInfo[idx].columnId) != dest->varColumnSize(key->columnInfo[idx].columnId))
					return false;
				if (memcmp(srcValue, destValue, src->varColumnSize(key->columnInfo[idx].columnId)) != 0)
					return false;
			}
		}
		return true;
	}
	static inline bool compareKey(const META::tableMeta* meta, const META::unionKeyMeta* key, DATABASE_INCREASE::DMLRecord* src, DATABASE_INCREASE::DMLRecord* dest)
	{
		if (key->columnCount == 1)
		{
			const char* srcValue = src->column(key->columnInfo[0].columnId), * destValue = dest->column(key->columnInfo[0].columnId);
			if (unlikely(src == nullptr && dest == nullptr))
				return true;
			else if (unlikely(src == nullptr || dest == nullptr))
				return false;
			switch (meta->m_columns[key->columnInfo[0].columnId].m_columnType)
			{
			case META::COLUMN_TYPE::T_STRING:
			case META::COLUMN_TYPE::T_TEXT:
			case META::COLUMN_TYPE::T_BINARY:
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_BYTE:
			case META::COLUMN_TYPE::T_GEOMETRY:
			case META::COLUMN_TYPE::T_JSON:
			case META::COLUMN_TYPE::T_XML:
			case META::COLUMN_TYPE::T_DECIMAL:
			case META::COLUMN_TYPE::T_BIG_NUMBER:
				if (src->varColumnSize(key->columnInfo[0].columnId) != dest->varColumnSize(key->columnInfo[0].columnId))
					return false;
				return memcmp(srcValue, destValue, src->varColumnSize(key->columnInfo[0].columnId)) == 0;
			case META::COLUMN_TYPE::T_FLOAT:
				return *(float*)srcValue == *(float*)destValue;
			case META::COLUMN_TYPE::T_DOUBLE:
				return *(double*)srcValue == *(double*)destValue;
			default:
				if (META::columnInfos[key->columnInfo[0].type].fixed)
					return memcmp(srcValue, destValue, META::columnInfos[key->columnInfo[0].type].columnTypeSize) == 0;
				else
				{
					if (src->varColumnSize(key->columnInfo[0].columnId) != dest->varColumnSize(key->columnInfo[0].columnId))
						return false;
					return memcmp(srcValue, destValue, src->varColumnSize(key->columnInfo[0].columnId)) == 0;
				}
			}
		}
		else
		{
			return compareUnionKey(meta, key, src, dest);
		}
	}
	static inline void* createBukcet(const META::tableMeta* meta, const META::unionKeyMeta* key)
	{
		if (key->columnCount == 1)
		{
			switch (meta->m_columns[key->columnInfo[0].columnId].m_columnType)
			{
			case META::COLUMN_TYPE::T_INT32:
				return new spp::sparse_hash_map<int32_t, blockListNode*>();
			case META::COLUMN_TYPE::T_UINT32:
				return new spp::sparse_hash_map<uint32_t, blockListNode*>();
			case META::COLUMN_TYPE::T_INT64:
				return new spp::sparse_hash_map<int64_t, blockListNode*>();
			case META::COLUMN_TYPE::T_UINT64:
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_TIME:
				return new spp::sparse_hash_map<uint64_t, blockListNode*>();
			case META::COLUMN_TYPE::T_INT16:
				return new spp::sparse_hash_map<int16_t, blockListNode*>();
			case META::COLUMN_TYPE::T_UINT16:
			case META::COLUMN_TYPE::T_YEAR:
			case META::COLUMN_TYPE::T_ENUM:
				return new spp::sparse_hash_map<uint16_t, blockListNode*>();
			case META::COLUMN_TYPE::T_INT8:
				return new spp::sparse_hash_map<int8_t, blockListNode*>();
			case META::COLUMN_TYPE::T_UINT8:
				return new spp::sparse_hash_map<uint8_t, blockListNode*>();
			case META::COLUMN_TYPE::T_SET:
				return new spp::sparse_hash_map<uint64_t, blockListNode*>();
			default:
				return new spp::sparse_hash_map<uint32_t, blockListNode*>();//use hash
			}
		}
		else
		{
			return new spp::sparse_hash_map<uint32_t, blockListNode*>();//use hash
		}
	}
	static inline void destroyBukcet(const META::tableMeta* meta, const META::unionKeyMeta* key, void* bucket)
	{
		if (key->columnCount == 1)
		{
			switch (meta->m_columns[key->columnInfo[0].columnId].m_columnType)
			{
			case META::COLUMN_TYPE::T_INT32:
				delete static_cast<spp::sparse_hash_map<int32_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				delete static_cast<spp::sparse_hash_map<uint32_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_INT64:
				delete static_cast<spp::sparse_hash_map<int64_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_UINT64:
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_TIME:
				delete static_cast<spp::sparse_hash_map<uint64_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_INT16:
				delete static_cast<spp::sparse_hash_map<int16_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_UINT16:
			case META::COLUMN_TYPE::T_YEAR:
			case META::COLUMN_TYPE::T_ENUM:
				delete static_cast<spp::sparse_hash_map<uint16_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_INT8:
				delete static_cast<spp::sparse_hash_map<int8_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				delete static_cast<spp::sparse_hash_map<uint8_t, blockListNode*>*>(bucket);
				break;
			case META::COLUMN_TYPE::T_SET:
				delete static_cast<spp::sparse_hash_map<uint64_t, blockListNode*>*>(bucket);
				break;
			default:
				delete static_cast<spp::sparse_hash_map<uint32_t, blockListNode*>*>(bucket);
				break;
			}
		}
		else
		{
			delete static_cast<spp::sparse_hash_map<uint32_t, blockListNode*>*>(bucket);
		}
	}
	
#define FIXED_INSERT(vtype,v,blockNode,bucket,prev) do{\
	(blockNode)->type = blockListNodeType::HEAD;\
	(blockNode)->value = (*(const vtype*)(v));\
	std::pair<spp::sparse_hash_map<vtype, blockListNode*>::iterator, bool> rtv = static_cast<spp::sparse_hash_map<vtype, blockListNode*>*>(bucket)->insert(std::pair<vtype, blockListNode*>((v)==nullptr?0:*(const vtype*)(v),(blockNode)));\
	if(!rtv.second){\
		(prev) = rtv.first->second;\
		(prev)->prev = (blockNode);\
		rtv.first->second = (blockNode);\
		(prev)->type = blockListNodeType::NORMAL;}\
	else{\
		(prev) = (blockNode); \
		(blockNode)->type = blockListNodeType::HEAD;\
	}\
	}while (0);

	static blockListNode* insertToBucket(void* bucket, DATABASE_INCREASE::DMLRecord* record, blockListNode* blockNode, const META::unionKeyMeta* key, bool newOrOld)
	{
		blockListNode* prev = nullptr;
		if (key->columnCount == 1)
		{
			const char* v;
			if (likely(newOrOld))
				v = record->column(key->columnInfo[0].columnId);
			else
				v = record->oldColumnOfUpdateType(key->columnInfo[0].columnId);
			switch (record->meta->m_columns[key->columnInfo[0].columnId].m_columnType)
			{
			case META::COLUMN_TYPE::T_INT32:
				FIXED_INSERT(int32_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				FIXED_INSERT(uint32_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_INT64:
				FIXED_INSERT(int64_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_UINT64:
				FIXED_INSERT(uint64_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_STRING:
			{
				uint32_t hash = getHash(record, key, newOrOld);
				FIXED_INSERT(uint32_t, &hash, blockNode, bucket, prev);
				break;
			}
			case META::COLUMN_TYPE::T_INT16:
				FIXED_INSERT(int16_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				FIXED_INSERT(uint16_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_INT8:
				FIXED_INSERT(int8_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				FIXED_INSERT(uint8_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_TIME:
				FIXED_INSERT(uint64_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_YEAR:
			case META::COLUMN_TYPE::T_ENUM:
				FIXED_INSERT(uint16_t, v, blockNode, bucket, prev);
				break;
			case META::COLUMN_TYPE::T_SET:
				FIXED_INSERT(uint64_t, v, blockNode, bucket, prev);
				break;
			default:
			{
				uint32_t hash = getHash(record, key, newOrOld);
				FIXED_INSERT(uint32_t, &hash, blockNode, bucket, prev);
				break;
			}
			}
		}
		else
		{
			uint32_t hash = getUnionKeyHash(record, key, newOrOld);
			FIXED_INSERT(uint32_t, &hash, blockNode, bucket, prev);
		}
		return prev;
	}
#define FIXED_ERASE(type,blockNode,bucket,empty) static_cast<spp::sparse_hash_map<type, blockListNode*>*>(bucket)->erase((type)blockNode->value);\
				(empty) = static_cast<spp::sparse_hash_map<type, blockListNode*>*>(bucket)->empty();

	static inline bool eraseKey(META::tableMeta* meta, void* bucket, blockListNode* blockNode, const META::unionKeyMeta* key)
	{
		bool empty;
		if (key->columnCount == 1)
		{
			switch (meta->m_columns[key->columnInfo[0].columnId].m_columnType)
			{
			case META::COLUMN_TYPE::T_INT32:
				FIXED_ERASE(int32_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				FIXED_ERASE(uint32_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_INT64:
				FIXED_ERASE(int64_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_UINT64:
				FIXED_ERASE(uint64_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_STRING:
				FIXED_ERASE(uint32_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_INT16:
				FIXED_ERASE(int16_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				FIXED_ERASE(uint16_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_INT8:
				FIXED_ERASE(int8_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				FIXED_ERASE(uint8_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_TIME:
				FIXED_ERASE(uint64_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_YEAR:
			case META::COLUMN_TYPE::T_ENUM:
				FIXED_ERASE(uint16_t, blockNode, bucket, empty);
				break;
			case META::COLUMN_TYPE::T_SET:
				FIXED_ERASE(uint64_t, blockNode, bucket, empty);
				break;
			default:
				FIXED_ERASE(uint32_t, blockNode, bucket, empty);
				break;
			}
		}
		else
		{
			FIXED_ERASE(uint32_t, blockNode, bucket, empty);
		}
		return empty;
	}

}
