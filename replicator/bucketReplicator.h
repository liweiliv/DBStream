#pragma once
#include "../memory/bufferPool.h"
#include "../meta/metaDataCollection.h"
#include "../meta/metaDataTimeLine.h"
#include "../sqlParser/sqlParser.h"
#include "../message/record.h"
#include "../util/unorderMapUtil.h"
#include "../util/ringFixedQueue.h"
#include "replicator.h"
#include "messageWrap.h"
#include "constraint.h"
#include "applier.h"
namespace REPLICATOR {
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
		META::tableMeta* meta;
		tableInfo *next;
		tableInfo *prev;
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
		transaction m_safeCheckPointHead;
		transaction m_preCommitHead;

		transaction* m_currentTxn;
		bufferPool m_pool;
		ringFixedQueue<transaction*>* m_preCommitTxnQueues;
		uint8_t m_preCommitTxnQueueCount;
	public:
		bucketReplicator(config* conf, META::metaDataCollection* metaDataCollection) :replicator(conf, metaDataCollection)
		{
		}
		inline int commit(transaction* trans)
		{
			if (trans->blockCount > 0)
				return 0;
			trans->preCommitNext = &m_preCommitHead;
			trans->ppreCommitPrevrev = m_preCommitHead.preCommitPrev;
			m_preCommitHead.preCommitPrev->preCommitNext = trans;
			m_preCommitHead.preCommitPrev = trans;
			if (unlikely(!m_preCommitTxnQueues[m_currentTxnId & (m_preCommitTxnQueueCount - 1)].push(trans, 1000)))
			{
				do
				{
					for (uint8_t idx = 0; idx < m_preCommitTxnQueueCount, idx++)
					{
						if (likely(m_preCommitTxnQueues[idx].push(trans, 1000)))
							return 0;
					}
				} while (m_running);
				return -1;
			}
			else
				return 0;
		}
		int handleFault(applier* ap)
		{
			if (m_ignoreErrno.find(ap->getErrno) != m_ignoreErrno.end())
				return 1;
			if (m_reconnectErrno.find(ap->getErrno) != m_reconnectErrno.end())
			{
				if(0!=ap->reconnect())
			}

		}
		void executeTransactionThread(int id)
		{
			applier* ap = nullptr;
			do {
				transaction* txn = nullptr;
				if (!m_preCommitTxnQueues[id & (m_preCommitTxnQueueCount - 1)].pop(txn, 1000))
				{
					for (uint8_t idx = 0; idx < m_preCommitTxnQueueCount; idx++)
					{
						if (m_preCommitTxnQueues[(id + idx) & (m_preCommitTxnQueueCount - 1)].pop(txn, 1000))
							break;
					}
					if (txn == nullptr)
						continue;
				}
				if (unlikely(ap->apply(txn) != 0))
				{
					if (m_ignoreErrno.find(ap->getErrno) != m_ignoreErrno.end())
					{
						bool success = false;
						while (m_running)
						{
							if (handleFault(ap) == 0)
							{
								if (success = (ap->apply(txn) == 0))
									break;
							}
						}
						if (!success&&m_ignoreErrno.find(ap->getErrno) == m_ignoreErrno.end())
						{
							m_running = false;
							return;
						}
					}
				}
				txn->committed = true;
			} while (likely(m_running));
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
		void processConstrint(replicatorRecord* r, uint16_t &keyIdx,bool newOrOld)
		{
			META::tableMeta* meta = static_cast<DATABASE_INCREASE::DMLRecord*>(r->record)->meta;
			if (static_cast<DATABASE_INCREASE::DMLRecord*>(r->record)->meta->m_primaryKey.count > 0)
			{
				if (likely(newOrOld || static_cast<DATABASE_INCREASE::DMLRecord*>(r->record)->isKeyUpdated(meta->m_primaryKey.keyIndexs, meta->m_primaryKey.count)))
				{
					r->blocks[keyIdx].prev = nullptr;
					r->blocks[keyIdx].record = r;
					blockListNode* cur = &r->blocks[keyIdx], * prev = insertToBucket(table->primaryKey, r->record, cur, &meta->m_primaryKey, newOrOld);
					if (unlikely(prev != cur && prev->record->trans != m_currentTxn))
						m_currentTxn->blockCount++;
					keyIdx++;
				}
			}
			for (uint16_t idx = 0; idx < record->meta->m_uniqueKeysCount; idx++)
			{
				if (likely(newOrOld || static_cast<DATABASE_INCREASE::DMLRecord*>(r->record)->isKeyUpdated(meta->m_uniqueKeys[idx].keyIndexs, meta->m_uniqueKeys[idx].count)))
				{
					r->blocks[keyIdx].prev = nullptr;
					r->blocks[keyIdx].record = r;
					blockListNode* cur = &r->blocks[keyIdx], * prev = insertToBucket(table->uniqueKeys[i], r->record, cur, &meta->m_uniqueKeys[idx], newOrOld);
					if (unlikely(prev != cur && prev->record->trans != m_currentTxn))
						m_currentTxn->blockCount++;
				}
				else
				{
					r->blocks[keyIdx].type = UNUSED;
				}
				keyIdx++;

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
			r->tableInfo = table;
			r->trans = m_currentTxn;
			if (m_currentTxn->lastRecord == nullptr)
				m_currentTxn->lastRecord = m_currentTxn->firstRecord = r;
			else
			{
				m_currentTxn->lastRecord->nextInTrans = r;
				m_currentTxn->lastRecord = r;
			}
			uint16_t keyIdx = 0;
			processConstrint(r, keyIdx, true);
			if (record->head->type == DATABASE_INCREASE::R_UPDATE || record->head->type == DATABASE_INCREASE::R_REPLACE)
				processConstrint(r, keyIdx, false);
			if (unlikely(table->ddlRunning))
			{
				if (table->ddlWaitListHead == nullptr)
					r->nextWaitForDDL = nullptr;
				else
					r->nextWaitForDDL = table->ddlWaitListHead;
				table->ddlWaitListHead = r;
				m_currentTxn->blockCount++;
			}
			return 0;
		}
		int putDDL(DATABASE_INCREASE::DDLRecord* record)
		{
			SQL_PARSER::handle* handle = nullptr;
			m_sqlParser->parse(handle, record->ddl);
		}
		inline int releaseRecord(replicatorRecord* record)
		{
			tableInfo* table = static_cast<tableInfo*>(record->tableInfo);
			if (record->record->head->type <= DATABASE_INCREASE::R_REPLACE)
			{
				bool allEmpty = true;
				for (uint16_t idx = 0; idx < record->prevRecordCount; idx++)
				{
					if (record->blocks[idx].type == NORMAL)
					{
						if (--record->blocks[idx].prev->record->trans->blockCount == 0)
						{
							if (unlikely(0 != commit(record->blocks[idx].prev->record->trans)))
								return -1;
						}
						allEmpty = false;
					}
					else if (record->blocks[idx].type == HEAD)
					{
						uint16_t realIdx = idx;
						if (realIdx >= table->keyCount)
							realIdx = idx % table->keyCount;
						void* keyMap;
						keyInfo* keyDef;
						if (table->primaryKey != nullptr)
						{
							if (realIdx == 0)
							{
								keyMap = table->primaryKey;
								keyDef = &table->meta->m_primaryKey;
							}
							else
							{
								keyMap = table->uniqueKeys[realIdx - 1];
								keyDef = &table->meta->m_uniqueKeys[realIdx - 1];
							}
						}
						else
						{
							keyMap = table->uniqueKeys[realIdx];
							keyDef = &table->meta->m_uniqueKeys[realIdx];
						}
						allEmpty |= eraseKey(keyMap, &record->blocks[idx], keyDef);
					}
				}
				if (allEmpty)
					table->empty = true;
			}
			else if (record->record->head->type == DATABASE_INCREASE::R_DDL)
			{
				replicatorRecord* ddlWait = table->ddlWaitListHead;
				while (ddlWait!=nullpt)
				{
					if (--ddlWait->trans->blockCount == 0)
					{
						if (unlikely(0 != commit(ddlWait->trans)))
							return -1;
					}
					ddlWait = ddlWait->nextWaitForDDL;
				}
				table->ddlWaitListHead = nullptr;
				table->ddlRecord = nullptr;
			}
			bufferPool::free(record->record);
			bufferPool::free(record);
			if (unlikely(table->next != nullptr))
			{
				if (table->empty) 
				{
					if (--table->next->ddlRecord->trans->blockCount == 0)
					{
						if (unlikely(0 != commit(ddlWait->trans)))
							return -1;
					}
					table->next->prev = nullptr;
					delete table;
				}
			}
			return 0;
		}
		inline void checkCommittedTxn()
		{
			uint8_t failedCount = 0;
			transaction* txn = m_preCommitHead.preCommitNext;
			if (txn == nullptr)
				return;
			do {
				if (!txn->committed)
				{
					failedCount++;
				}
				else
				{
					txn->preCommitNext->preCommitPrev = txn->preCommitPrev;
					txn->preCommitPrev->preCommitNext = txn->preCommitNext;

					txn->next->prev = txn->prev;
					txn->prev->next = txn->next;

					if (txn->prev == &m_safeCheckPointHead)
					{
						m_safeLogOffset = txn->lastRecord->record->head->logOffset;
						m_safeLogOffset = txn->lastRecord->record->head->timestamp;
					}
					replicatorRecord* record = txn->firstRecord;
					do {
						releaseRecord(record);
						record = record->nextInTrans;
					} while (record != txn->firstRecord);
					transaction* next = txn->preCommitNext;
					bufferPool::free(txn);
					txn = next;
				}
			} while (txn!=nullptr&&failedCount < m_preCommitTxnQueueCount);
		}
		int createNewTxn(DATABASE_INCREASE::record* record)
		{
			transaction* newTxn = (transaction*)m_pool.alloc(sizeof(transaction));
			newTxn->blockCount = 0;
			newTxn->recordCount = 1;
			newTxn->committed = false;
			newTxn = record->head->txnId;

			newTxn->next = &m_safeCheckPointHead;
			newTxn->prev = m_safeCheckPointHead.prev;
			m_safeCheckPointHead.prev->next = newTxn;
			m_safeCheckPointHead.prev = newTxn;

			if (likely(m_currentTxn!=nullptr)&&0 != commit(m_currentTxn))
			{
				return -1;
			}
			m_currentTxn = newTxn;
		}
		int put(DATABASE_INCREASE::record* record)
		{
			if(unlikely(checkCommittedTxn()<0)
				return -1;
			if (record->head->txnId != m_currentTxnId)
				createNewTxn(record);
			if (likely(record->head->type <= DATABASE_INCREASE::R_REPLACE))
				return putDML(static_cast<DATABASE_INCREASE::DMLRecord>(record));
			else if(record->head->type == DATABASE_INCREASE::R_DDL)
				return putDDL(static_cast<DATABASE_INCREASE::DDLRecord>(record));
			else if (record->head->type == DATABASE_INCREASE::R_HEARTBEAT)
			{
				createNewTxn(record);
			}
			else
			{

			}

		}
	};
}
