#pragma once
#include "../memory/bufferPool.h"
#include "../meta/metaDataCollection.h"
#include "../meta/metaDataTimeLine.h"
#include "../sqlParser/sqlParser.h"
#include "../message/record.h"
#include "../util/unorderMapUtil.h"
#include "replicator.h"
namespace REPLICATOR {
	struct tableInfo {
		uint64_t id;
		std::string tableName;
		void* primaryKey;
		void** uniqueKeys;
		uint16_t uniqueKeysCount;
		META::tableMeta* meta;
	};
	typedef std::unordered_map<const char*, META::MetaTimeline<tableInfo>*, StrHash, StrCompare> tableTree;
	struct databaseInfo {
		tableTree tables;
		uint64_t id;
		std::string dbName;
	};
	typedef std::unordered_map<const char*, META::MetaTimeline<databaseInfo>*, StrHash, StrCompare> dbTree;
	typedef std::unordered_map<uint64_t, tableInfo *> tableIdTree;

	class bucketReplicator :public replicator {
	private:
		tableIdTree m_tables;
		dbTree m_databases;
		uint64_t m_currentTxnId;
	public:
		bucketReplicator(config* conf, META::metaDataCollection* metaDataCollection) :replicator(conf, metaDataCollection)
		{
		}
		int commit()
		{

		}
		void* createKeyInfo(const META::tableMeta* meta, const META::keyInfo* key)
		{
			if (key->count > 0)
			{
				return new 
			}
		}
		inline tableInfo* getTable(DATABASE_INCREASE::DMLRecord* record)
		{
			tableIdTree::const_iterator iter = m_tables.find(record->tableMetaID);
			if (iter == m_tables.end())
			{
				tableInfo* table = new tableInfo();
				table->id = record->tableMetaID;
				table->meta = record->meta;
				table->tableName = record->meta->m_tableName;
				if (table->meta->m_primaryKey.count > 0)
				{
					if (table->meta->m_primaryKey.count == 1 && META::columnInfos[table->meta->m_columns[table->meta->m_primaryKey.keyIndexs[0]].m_columnType].fixed)
					{

					}
				}
				else
				{
					table->primaryKey = nullptr;
				}

			}
		}
		int putDML(DATABASE_INCREASE::DMLRecord* record)
		{


		}
		int put(DATABASE_INCREASE::record* record)
		{
			if (record->head->txnId != m_currentTxnId)
			{
				if (0 != commit())
				{
					return -1;
				}
				m_currentTxnId = record->head->txnId;
			}
			switch (record->head->type)
			{
			case DATABASE_INCREASE::R_INSERT:
			case DATABASE_INCREASE::R_DELETE:
			case DATABASE_INCREASE::R_UPDATE:
			case DATABASE_INCREASE::R_REPLACE:
				return putDML(static_cast<DATABASE_INCREASE::record>(record));
			default:
				break;
			}
		}
	};
}
