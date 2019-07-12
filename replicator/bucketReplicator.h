#pragma once
#include "../memory/bufferPool.h"
#include "../meta/metaDataCollection.h"
#include "../meta/metaDataTimeLine.h"
#include "../sqlParser/sqlParser.h"
#include "../message/record.h"
#include "../util/unorderMapUtil.h"
#include "replicator.h"
#include "messageWrap.h"
#include "constraint.h"
namespace REPLICATOR {
	struct tableInfo {
		uint64_t id;
		std::string tableName;
		void* primaryKey;
		void** uniqueKeys;
		uint16_t uniqueKeysCount;
		uint16_t keyCount;
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
		transaction* m_currentTxn;
		bufferPool m_pool;
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
		int putDML(tableInfo * table,DATABASE_INCREASE::DMLRecord* record)
		{
			replicatorRecord* r;
			if (record->head->type == DATABASE_INCREASE::R_UPDATE || record->head->type == DATABASE_INCREASE::R_REPLACE)
				r = (replicatorRecord*)m_pool.alloc(sizeof(replicatorRecord*) * 2 * table->keyCount + sizeof(replicatorRecord));
			else
				r = (replicatorRecord*)m_pool.alloc(sizeof(replicatorRecord*)  * table->keyCount + sizeof(replicatorRecord));
			r->nextInTrans = nullptr;
			if (m_currentTxn->lastRecord == nullptr)
				m_currentTxn->lastRecord = m_currentTxn->firstRecord = r;
			else
			{
				m_currentTxn->lastRecord->nextInTrans = r;
				m_currentTxn->lastRecord = r;
			}
			uint16_t keyIdx = 0;
			if (record->meta->m_primaryKey.count > 0)
			{
				r->blocks[keyIdx].prev = nullptr;
				r->blocks[keyIdx].record = r;
				blockListNode* cur = &r->blocks[keyIdx],* prev = insertToBucket(table->primaryKey, record, cur, &record->meta->m_primaryKey, true);
				if (unlikely(prev != cur&&prev->record->trans != m_currentTxn))
					m_currentTxn->blockCount++;
				keyIdx++;
			}
			for(u)
		}
		int put(DATABASE_INCREASE::record* record)
		{
			if (record->head->txnId != m_currentTxnId)
			{
				if (0 != commit())
				{
					return -1;
				}
				m_currentTxn = (transaction*)m_pool.alloc(sizeof(transaction));
				m_currentTxn->blockCount = 0;
				m_currentTxn->finished = false;
				m_currentTxn->recordCount = 1;
				m_currentTxnId = record->head->txnId;
			}
			if (likely(record->head->type <= DATABASE_INCREASE::R_REPLACE))
				return putDML(static_cast<DATABASE_INCREASE::record>(record));
		}
	};
}

