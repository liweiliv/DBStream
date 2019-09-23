#pragma once
#include "meta/metaData.h"
#include "messageWrap.h"
#include "constraint.h"
#include "util/json.h"
#include "glog/logging.h"
#include "remoteMeta.h"
#include <mutex>
namespace REPLICATOR
{
	struct tableInfo {
		uint64_t id;
		bool empty;
		std::string tableName;
		void* primaryKey;
		void** uniqueKeys;
		uint16_t uniqueKeysCount;
		uint16_t keyCount;
		replicatorRecord* ddlRecord;
		replicatorRecord* ddlWaitListHead;

		const META::tableMeta* meta;
		const META::tableMeta* destMeta;
		uint16_t* columnMap;
		std::string escapedTableNmae;
		std::string* escapedColumnNames;
		tableInfo* next;
		tableInfo* prev;
		static std::mutex  escapLock;
		tableInfo(const META::tableMeta* meta, const META::tableMeta* destMeta) :id(meta->m_id), empty(true), tableName(meta->m_tableName), uniqueKeysCount(meta->m_uniqueKeysCount), primaryKey(nullptr), uniqueKeys(nullptr), keyCount(meta->m_uniqueKeysCount + (meta->m_primaryKey.count > 0?1 : 0)), ddlRecord(nullptr), ddlWaitListHead(nullptr), meta(meta),destMeta(destMeta), columnMap(nullptr),next(nullptr), prev(nullptr)
		{
			if (destMeta->m_primaryKey.count > 0)
				primaryKey = createBukcet(destMeta, &destMeta->m_primaryKey);
			if (destMeta->m_uniqueKeysCount > 0)
			{
				uniqueKeys = new void* [destMeta->m_uniqueKeysCount];
				for (int idx = 0; idx < destMeta->m_uniqueKeysCount; idx++)
					uniqueKeys[idx] = createBukcet(destMeta, &destMeta->m_uniqueKeys[idx]);
			}
			escapedColumnNames = new std::string[destMeta->m_columnsCount];
		}
		bool init(remoteMeta* remote, const jsonObject* columnMapInJson)
		{
			columnMap = new uint16_t[destMeta->m_columnsCount];
			if (remote->escapeTableName(destMeta, escapedTableNmae) != 0)
			{
				LOG(ERROR) << "escape table name of " << destMeta->m_dbName << "." << destMeta->m_tableName << " failed ";
				return false;
			}
			for (int idx = 0; idx < destMeta->m_columnsCount; idx++)
			{
				const jsonValue* mapedColumn = columnMapInJson->get(destMeta->m_columns[idx].m_columnName);
				if (mapedColumn == nullptr) 
				{
					const META::columnMeta* mapedColumnMeta = meta->getColumn(destMeta->m_columns[idx].m_columnName.c_str());
					if (mapedColumnMeta == nullptr)
					{
						LOG(ERROR) << "can not genrate table map info between src table:" << meta->m_dbName << "." << meta->m_tableName << " and " << destMeta->m_dbName << "." << destMeta->m_tableName << " for dest column " << destMeta->m_columns[idx].m_columnName << " is not exist in src table and column map";
						return false;
					}
					else
					{
						columnMap[idx] = mapedColumnMeta->m_columnIndex;
					}
				}
				else
				{
					if (mapedColumn->t != jsonValue::J_STRING)
					{
						LOG(ERROR) << "can not genrate table map info between src table:" << meta->m_dbName << "." << meta->m_tableName << " and " << destMeta->m_dbName << "." << destMeta->m_tableName << " for column map of dest column " << destMeta->m_columns[idx].m_columnName << " is not a string";
						return false;
					}
					const META::columnMeta* mapedColumnMeta = meta->getColumn(static_cast<const jsonString*>(mapedColumn)->m_value.c_str());
					if (mapedColumnMeta == nullptr)
					{
						LOG(ERROR) << "can not genrate table map info between src table:" << meta->m_dbName << "." << meta->m_tableName << " and " << destMeta->m_dbName << "." << destMeta->m_tableName << " for dest column " << destMeta->m_columns[idx].m_columnName << " is not exist in src table and column map";
						return false;
					}
					else
					{
						columnMap[idx] = mapedColumnMeta->m_columnIndex;
					}
				}
				if (remote->escapeColumnName(&destMeta->m_columns[idx], escapedColumnNames[idx]) != 0)
				{
					LOG(ERROR) << "escape column name of " << destMeta->m_dbName << "." << destMeta->m_tableName <<"."<< destMeta->m_columns[idx].m_columnName<< " failed ";
					return false;
				}
			}
			return true;
		}
		~tableInfo()
		{
			if (primaryKey != nullptr)
				destroyBukcet(meta, &meta->m_primaryKey, primaryKey);
			if (uniqueKeys != nullptr)
			{

				for (int idx = 0; idx < uniqueKeysCount; idx++)
					destroyBukcet(meta, &meta->m_uniqueKeys[idx], uniqueKeys[idx]);
				delete[] uniqueKeys;
			}
			if (columnMap != nullptr)
				delete[] columnMap;
			if (escapedColumnNames != nullptr)
				delete[]escapedColumnNames;
		}
	};
}