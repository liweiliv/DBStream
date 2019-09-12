#pragma once
#include "../meta/metaData.h"
#include "../util/sparsepp/spp.h"
#include "..//message/record.h"
#include "../util/itoaSse.h"
#include "../util/dtoa.h"
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
		while (*(str) != '\0)
			hash = hash * HASH_SEED + (*str++);
		return hash;
	}
	static inline bool compareUnionKey(const META::tableMeta* meta, const META::keyInfo* key, DATABASE_INCREASE::DMLRecord* src, DATABASE_INCREASE::DMLRecord* dest)
	{
		for (uint16_t idx = 0; idx < key->count; idx++)
		{
			uint8_t type = meta->m_columns[key->keyIndexs[idx]].m_columnType;
			const char* srcValue = src->column(key->keyIndexs[idx]), * destValue = dest->column(key->keyIndexs[idx]);
			if (unlikely(src == nullptr && dest == nullptr))
				return true;
			else if (unlikely(src == nullptr || dest == nullptr))
				return false;
			if (META::columnInfos[type].fixed)
			{
				if (memcmp(srcValue, destValue, META::columnInfos[meta->m_columns[key->keyIndexs[idx]].m_columnType].columnTypeSize) != 0)
					return false;
			}
			else
			{
				if (src->varColumnSize(key->keyIndexs[idx]) != dest->varColumnSize(keyIndexs[idx]))
					return false;
				if (memcmp(srcValue, destValue, src->varColumnSize(key->keyIndexs[idx])) != 0)
					return false;
			}
		}
		return true;
	}
	static inline bool compareKey(const META::tableMeta* meta, const META::keyInfo* key, DATABASE_INCREASE::DMLRecord* src, DATABASE_INCREASE::DMLRecord* dest)
	{
		if (key->count == 1)
		{
			const char* srcValue = src->column(key->keyIndexs[0]), * destValue = dest->column(key->keyIndexs[0]);
			if (unlikely(src == nullptr && dest == nullptr))
				return true;
			else if (unlikely(src == nullptr || dest == nullptr))
				return false;
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_STRING:
			case META::T_TEXT:
			case META::T_BINARY:
			case META::T_BLOB:
			case META::T_BYTE:
			case META::T_GEOMETRY:
			case META::T_JSON:
			case META::T_XML:
			case META::T_DECIMAL:
			case META::T_BIG_NUMBER:
				if (src->varColumnSize(key->keyIndexs[0]) != dest->varColumnSize(keyIndexs[0]))
					return false;
				return memcmp(srcValue, destValue, src->varColumnSize(key->keyIndexs[0])) == 0;
			case META::T_FLOAT:
				return *(float*)srcValue == *(float*)destValue;
			case META::T_DOUBLE:
				return *(double*)srcValue == *(double*)destValue;
			default:
				if (META::columnInfos[meta->m_columns[key->keyIndexs[0]].m_columnType].fixed)
					return memcmp(srcValue, destValue, META::columnInfos[meta->m_columns[key->keyIndexs[0]].m_columnType].columnTypeSize) == 0;
				else
				{
					if (src->varColumnSize(key->keyIndexs[0]) != dest->varColumnSize(keyIndexs[0]))
						return false;
					return memcmp(srcValue, destValue, src->varColumnSize(key->keyIndexs[0])) == 0;
				}
			}
		}
		else
		{
			return compareUnionKey(meta, key, src, dest);
		}
	}
	static inline void* createBukcet(const META::tableMeta* meta, const META::keyInfo* key)
	{
		if (key->count == 1)
		{
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_INT32:
				return new spp::sparse_hash_map<int32_t, blockListNode*>();
			case META::T_UINT32:
				return new spp::sparse_hash_map<uint32_t, blockListNode*>();
			case META::T_INT64:
				return new spp::sparse_hash_map<int64_t, blockListNode*>();
			case META::T_UINT64:
			case META::T_DATETIME:
			case META::T_TIMESTAMP:
			case META::T_TIME:
				return new spp::sparse_hash_map<uint64_t, blockListNode*>();
			case META::T_INT16:
				return new spp::sparse_hash_map<int16_t, blockListNode*>();
			case META::T_UINT16:
			case META::T_YEAR:
			case META::T_ENUM:
				return new spp::sparse_hash_map<uint16_t, blockListNode*>();
			case META::T_INT8:
				return new spp::sparse_hash_map<int8_t, blockListNode*>();
			case META::T_UINT8:
				return new spp::sparse_hash_map<uint8_t, blockListNode*>();
			case META::T_SET:
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
	static inline void destroyBukcet(const META::tableMeta* meta, const META::keyInfo* key,void * bucket)
	{
		if (key->count == 1)
		{
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_INT32:
				delete static_cast<spp::sparse_hash_map<int32_t, blockListNode*>*>(bucket);
				break;
			case META::T_UINT32:
				delete static_cast<spp::sparse_hash_map<uint32_t, blockListNode*>*>(bucket);
				break;
			case META::T_INT64:
				delete static_cast<spp::sparse_hash_map<int64_t, blockListNode*>*>(bucket);
				break;
			case META::T_UINT64:
			case META::T_DATETIME:
			case META::T_TIMESTAMP:
			case META::T_TIME:
				delete static_cast<spp::sparse_hash_map<uint64_t, blockListNode*>*>(bucket);
				break;
			case META::T_INT16:
				delete static_cast<spp::sparse_hash_map<int16_t, blockListNode*>*>(bucket);
				break;
			case META::T_UINT16:
			case META::T_YEAR:
			case META::T_ENUM:
				delete static_cast<spp::sparse_hash_map<uint16_t, blockListNode*>*>(bucket);
				break;
			case META::T_INT8:
				delete static_cast<spp::sparse_hash_map<int8_t, blockListNode*>*>(bucket);
				break;
			case META::T_UINT8:
				delete static_cast<spp::sparse_hash_map<uint8_t, blockListNode*>*>(bucket);
				break;
			case META::T_SET:
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
	static inline uint32_t getUnionKeyHash(DATABASE_INCREASE::DMLRecord* record, const META::keyInfo* key, bool newOrOld)
	{
		uint32_t hash = 0;
		for (uint16_t idx = 0; idx < key->count; idx++)
		{
			uint8_t type = meta->m_columns[key->keyIndexs[idx]].m_columnType;
			const char* v;
			if (likely(newOrOld))
				v = record->column(key->keyIndexs[idx]);
			else
				v = record->oldColumnOfUpdateType(key->keyIndexs[idx]);
			if (v != null)
			{
				if (META::columnInfos[type].fixed)
					hash = bkdrHash(hash, v, META::columnInfos[type].columnTypeSize);
				else
					hash = bkdrHash(hash, v, record->varColumnSize(key->keyIndexs[idx]));
			}
		}
		return hash;
	}
#define FIXED_INSERT(type,v,blockNode,bucket,prev) do{\
	(blockNode)->head = true;\
	(blockNode)->value = (v);\
	std::pair<spp::sparse_hash_map<type, blockListNode*>::iterator, bool> rtv = static_cast<spp::sparse_hash_map<type, blockListNode*>*>(bucket)->insert(std::pair<type, blockListNode*>((v)==nullptr?0:*(const type*)(v),(blockNode)));\\
	if(!rtv.second){\
		(prev) = rtv.first->second;\
		(prev)->prev = (blockNode);\
		rtv.first->second = (blockNode);\
		(prev)->type = NORMAL;}\
	else{\
		(prev) = (blockNode); \
		(blockNode)->type = HEAD;\
	}\
	}while (0);
	
	static blockListNode* insertToBucket(void* bucket, DATABASE_INCREASE::record* record, blockListNode * blockNode, const META::keyInfo* key, bool newOrOld)
	{
		blockListNode* prev = nullptr;
		if (key->count == 1)
		{
			const char* v;
			if (likely(newOrOld))
				v = record->column(key->keyIndexs[0]);
			else
				v = record->oldColumnOfUpdateType(key->keyIndexs[0]);
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_INT32:
				FIXED_INSERT(int32_t, v, blockNode, bucket, prev);
				break;
			case META::T_UINT32:
				FIXED_INSERT(uint32_t, v, blockNode, bucket, prev);
				break;
			case META::T_INT64:
				FIXED_INSERT(int64_t, v, blockNode, bucket, prev);
				break;
			case META::T_UINT64:
				FIXED_INSERT(uint64_t, v, blockNode, bucket, prev);
				break;
			case META::T_STRING:
			{
				uint32_t hash = getHash(record, key, newOrOld);
				FIXED_INSERT(uint32_t, &hash, blockNode, bucket, prev);
				break;
			}
			case META::T_INT16:
				FIXED_INSERT(int16_t, v, blockNode, bucket, prev);
				break;
			case META::T_UINT16:
				FIXED_INSERT(uint16_t, v, blockNode, bucket, prev);
				break;
			case META::T_INT8:
				FIXED_INSERT(int8_t, v, blockNode, bucket, prev);
				break;
			case META::T_UINT8:
				FIXED_INSERT(uint8_t, v, blockNode, bucket, prev);
				break;
			case META::T_DATETIME:
			case META::T_TIMESTAMP:
			case META::T_TIME:
				FIXED_INSERT(uint64_t, v, blockNode, bucket, prev);
				break;
			case META::T_YEAR:
			case META::T_ENUM:
				FIXED_INSERT(uint16_t, v, blockNode, bucket, prev);
				break;
			case META::T_SET:
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
			FIXED_INSERT(uint32_t, &v, blockNode, bucket, prev);
		}
		return prev;
	}
#define FIXED_ERASE(type,blockNode,bucket,empty) static_cast<spp::sparse_hash_map<type, blockListNode*>*>(bucket)->erase((type)blockNode->value);\
				(empty) = static_cast<spp::sparse_hash_map<type, blockListNode*>*>(bucket)->empty();

	static inline bool eraseKey(void* bucket,blockListNode* blockNode, const META::keyInfo* key)
	{
		bool empty;
		if (key->count == 1)
		{
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_INT32:
				FIXED_ERASE(int32_t, blockNode, bucket);
				break;
			case META::T_UINT32:
				FIXED_ERASE(uint32_t, blockNode, bucket);
				break;
			case META::T_INT64:
				FIXED_ERASE(int64_t, blockNode, bucket);
				break;
			case META::T_UINT64:
				FIXED_ERASE(uint64_t, blockNode, bucket);
				break;
			case META::T_STRING:
				FIXED_ERASE(uint32_t, blockNode, bucket);
				break;
			case META::T_INT16:
				FIXED_ERASE(int16_t, blockNode, bucket);
				break;
			case META::T_UINT16:
				FIXED_ERASE(uint16_t, blockNode, bucket);
				break;
			case META::T_INT8:
				FIXED_ERASE(int8_t, blockNode, bucket);
				break;
			case META::T_UINT8:
				FIXED_ERASE(uint8_t, blockNode, bucket);
				break;
			case META::T_DATETIME:
			case META::T_TIMESTAMP:
			case META::T_TIME:
				FIXED_ERASE(uint64_t, blockNode, bucket);
				break;
			case META::T_YEAR:
			case META::T_ENUM:
				FIXED_ERASE(uint16_t, blockNode, bucket);
				break;
			case META::T_SET:
				FIXED_ERASE(uint64_t, blockNode, bucket);
				break;
			default:
				FIXED_ERASE(uint32_t, blockNode, bucket);
				break;
			}
		}
		else
		{
			FIXED_ERASE(uint32_t, blockNode, bucket);
		}
		return empty;
	}
	static inline uint32_t getHash(DATABASE_INCREASE::DMLRecord* record, const META::keyInfo* key, bool newOrOld)
	{
		if (key->count == 1)
		{
			const char* value;
			if (likely(newOrOld))
				value = record->column(key->keyIndexs[0]);
			else
				value = record->oldColumnOfUpdateType(key->keyIndexs[0]);
			if (value == nullptr)
				return 0;
			switch (meta->m_columns[key->keyIndexs[0]].m_columnType)
			{
			case META::T_STRING:
			case META::T_TEXT:
			case META::T_BINARY:
			case META::T_BLOB:
			case META::T_BYTE:
			case META::T_GEOMETRY:
			case META::T_JSON:
			case META::T_XML:
			case META::T_DECIMAL:
			case META::T_BIG_NUMBER:
				return bkdrHash(0, value, record->varColumnSize(key->keyIndexs[0]));
			case META::T_FLOAT:
			{
				char floatBuffer[256];
				my_fcvt(*(const float*)value, record->meta->m_columns[key->keyIndexs[0]].m_decimals, floatBuffer, NULL);
				return bkdrHash(0, floatBuffer);
			}
			case META::T_DOUBLE:
			{
				char floatBuffer[256];
				my_fcvt(*(const double*)value, record->meta->m_columns[key->keyIndexs[0]].m_decimals, floatBuffer, NULL);
				return bkdrHash(0, floatBuffer);
			}
			default:
			{
				if (META::columnInfos[record->meta->m_columns[key->keyIndexs[0]].m_columnType].fixed)
				{
					return bkdrHash(0, value, META::columnInfos[record->meta->m_columns[key->keyIndexs[0]].m_columnType].columnTypeSize);
				}
				else
				{
					return bkdrHash(0, value, record->varColumnSize(key->keyIndexs[0]));
				}
			}
			}
		}
		else
		{
			return getUnionKeyHash(record, key);
		}
	}
