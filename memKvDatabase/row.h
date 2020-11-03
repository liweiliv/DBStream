#pragma once
#include <glog/logging.h>
#include "memory/bufferPool.h"
#include "message/record.h"
#include "util/timer.h"
#include "util/status.h"
#include "thread/barrier.h"
#include "meta/columnType.h"
#include "errorCode.h"
#include "clientHandel.h"
namespace KVDB
{
	constexpr static int MAX_COLUMN_COUNT = 1024;
	struct columnChange {
		uint16_t columnId;
		uint32_t size;
		const char* newValue;
	};
	struct rowChange {
		uint16_t count;
		uint32_t varColumnSize;
		columnChange* columnChanges;
	};
	struct version
	{
		version* prev;
		version* next;
		DATABASE_INCREASE::DMLRecord data;
		static dsStatus& allocForInsert(version*& v, bufferPool* pool, const META::tableMeta* meta, const rowChange* row)
		{
			uint16_t columnIdMap[MAX_COLUMN_COUNT];
			if (unlikely(row->count > MAX_COLUMN_COUNT))
				dsFailedAndLogIt(errorCode::COLUMN_COUNT_NOT_MATCH, "column count:" << row->count << " over " << MAX_COLUMN_COUNT, WARNING);
			memset(columnIdMap, 0, meta->m_columnsCount * sizeof(uint16_t));
			for (int i = 0; i < row->count; i++)
				columnIdMap[row->columnChanges[i].columnId] = i + 1;

			v = (version*)pool->alloc(sizeof(version) + DATABASE_INCREASE::DMLRecord::allocSize(meta) + row->varColumnSize);
			v->data.data = ((char*)&v->data) + sizeof(v->data);
			v->data.initRecord(((char*)&v->data) + sizeof(v->data), meta, DATABASE_INCREASE::RecordType::R_INSERT);
			for (int i = 0; i < meta->m_columnsCount; i++)
			{
				const META::columnMeta* columnMeta = meta->getColumn(i);
				if (columnIdMap[i] == 0)
				{
					if (!columnMeta->m_nullable || columnMeta->m_isPrimary)
						dsFailedAndLogIt(errorCode::COLUMN_IS_NOT_NULLABLE, "column:" << columnMeta->m_columnName << " is not nullable", WARNING);
					if (META::columnInfos[static_cast<uint8_t>(columnMeta->m_columnType)].fixed)
						v->data.setFixedColumnNull(i);
					else
						v->data.setVarColumnNull(i);
				}
				else
				{
					if (META::columnInfos[static_cast<uint8_t>(columnMeta->m_columnType)].fixed)
						v->data.setFixedColumn(i, row->columnChanges[columnIdMap[i] - 1].newValue);
					else
						v->data.setVarColumn(i, row->columnChanges[columnIdMap[i] - 1].newValue, row->columnChanges[columnIdMap[i] - 1].size);
				}
			}
			v->data.finishedSet();
			v->data.head->timestamp = timer::getNowTimestamp();
			v->prev = v->next = nullptr;
			dsOk();
		}

		inline void copyColumn(META::COLUMN_TYPE type, uint16_t columnId, const char* v, uint32_t size, DATABASE_INCREASE::DMLRecord& r)
		{
			if (META::columnInfos[static_cast<uint16_t>(type)].fixed)
			{
				if (v == nullptr)
					r.setFixedColumnNull(columnId);
				else
					r.setFixedColumnByMemcopy(columnId, v);
			}
			else
			{
				if (v == nullptr)
					r.setVarColumnNull(columnId);
				else
					r.setVarColumn(columnId, v, size);
			}
		}
		dsStatus& update(bufferPool* pool, uint64_t tid, const rowChange* change)
		{
			const META::tableMeta* meta = data.meta;
			int32_t deltaSize = 0;
			for (int i = 0; i < change->count; i++)
			{
				columnChange& c = change->columnChanges[i];
				const META::columnMeta* col = meta->getColumn(c.columnId);
				if (col == nullptr)
					dsFailedAndLogIt(errorCode::FIELD_NOT_FOUND, "field not found,column index:" << c.columnId << ",table:" << meta->m_dbName << "." << meta->m_tableName, ERROR);
				if (!META::columnInfos[static_cast<uint8_t>(col->m_columnType)].fixed)
					deltaSize += (c.size - data.varColumnSize(c.columnId));
			}
			version* v = (version*)pool->alloc(sizeof(version) + data.head->minHead.size + deltaSize);
			v->prev = this;
			v->next = nullptr;
			v->data.initRecord(((char*)&v->data) + sizeof(v->data), meta, DATABASE_INCREASE::RecordType::R_INSERT);
			v->data.head->txnId = tid;
			v->data.head->timestamp = timer::getNowTimestamp();
			for (uint16_t i = 0, cid = 0; i < meta->m_columnsCount; i++)
			{
				const META::columnMeta* col = meta->getColumn(i);
				if (cid < change->count && change->columnChanges[cid].columnId == i)
				{
					copyColumn(col->m_columnType, i, change->columnChanges[cid].newValue, change->columnChanges[cid].size, v->data);
					cid++;
				}
				else
				{
					if (META::columnInfos[static_cast<uint16_t>(col->m_columnType)].fixed)
						copyColumn(col->m_columnType, i, data.column(i), META::columnInfos[static_cast<uint16_t>(col->m_columnType)].columnTypeSize, v->data);
					else
						copyColumn(col->m_columnType, i, data.column(i), data.varColumnSize(i), v->data);
				}
			}
			barrier;
			this->next = v;
			dsOk();
		}
		//delete
		dsStatus& drop(bufferPool* pool, uint64_t tid)
		{
			version* v = (version*)pool->alloc(offsetof(version, data) + sizeof(DATABASE_INCREASE::record) + sizeof(DATABASE_INCREASE::recordHead));
			v->prev = this;
			v->next = nullptr;
			v->data.data = ((char*)&v->data) + sizeof(v->data);
			v->data.init(v->data.data + sizeof(DATABASE_INCREASE::record));
			v->data.head->minHead.size = sizeof(DATABASE_INCREASE::recordHead);
			v->data.head->minHead.type = static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE);
			v->data.head->txnId = tid;
			v->data.head->timestamp = timer::getNowTimestamp();
			barrier;
			this->next = v;
			dsOk();
		}
	};
	struct row {
		version* current;
		version* head;
		version* tail;
		std::atomic_uint32_t owner;
		inline void init()
		{
			current = head = tail = nullptr;
			owner.store(0, std::memory_order_relaxed);
		}
		inline bool lock(uint32_t uid)
		{
			uint32_t _owner;
			do {
				_owner = owner.load(std::memory_order_relaxed);
				if (_owner != 0 && _owner != uid)
					return false;
			} while (!owner.compare_exchange_weak(_owner, uid, std::memory_order_relaxed));
			return true;
		}
		inline bool unlock(uint32_t uid)
		{
			uint32_t _owner;
			do {
				_owner = owner.load(std::memory_order_relaxed);
				if (_owner != uid)
					return false;
			} while (!owner.compare_exchange_weak(_owner, 0, std::memory_order_relaxed));
			return true;
		}
		inline void unlockWithOutCheck()
		{
			owner.store(0, std::memory_order_relaxed);
		}
		dsStatus& select(bufferPool* pool, clientHandle* client,const version*& v)
		{
			/*
			if (client->m_trans->m_level == TRANS_ISOLATION_LEVEL::READ_COMMITTED)
			{
				v = current;
				if (v == nullptr || v->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
				{
					v = nullptr;
					dsFailedAndLogIt(errorCode::ROW_NOT_EXIST, "select failed for row not exist", WARNING);
				}
				dsOk();
			}
			else
			{
				dsFailedAndLogIt(errorCode::TRANS_ISOLATION_LEVEL_NOT_SUPPORT, "transaction isolation not support", WARNING);
			}
			*/
			v = current;
			if (v == nullptr || v->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
			{
				v = nullptr;
				dsFailedAndLogIt(errorCode::ROW_NOT_EXIST, "select failed for row not exist", WARNING);
			}
			dsOk();
		}

		dsStatus& insert(bufferPool* pool, clientHandle* client, version * v)
		{
			if (tail != nullptr && tail->data.head->minHead.type != static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
				dsFailed(errorCode::ROW_NOT_EXIST, "can not delete, row not exist");
			if (!lock(client->m_uid))
				dsFailed(errorCode::GET_LOCK_FAILED, "get lock failed");
			if (tail != nullptr && tail->data.head->minHead.type != static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
			{
				unlockWithOutCheck();
				dsFailed(errorCode::ROW_NOT_EXIST, "can not delete, row not exist");
			}
			if (tail != nullptr)
			{
				v->prev = tail;
				barrier;
				tail->next = v;
			}
			else
			{
				barrier;
				tail = v;
				current = head = v;
			}
			dsOk();
		}
		dsStatus& update(bufferPool* pool, clientHandle* client, const rowChange* change)
		{
			if (tail == nullptr || tail->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
				dsFailed(errorCode::ROW_NOT_EXIST, "can not update, row not exist");
			if (!lock(client->m_uid))
				dsFailed(errorCode::GET_LOCK_FAILED, "get lock failed");
			if (tail == nullptr || tail->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
			{
				unlockWithOutCheck();
				dsFailed(errorCode::ROW_NOT_EXIST, "can not update, row not exist");
			}
			if (!dsCheck(tail->update(pool, client->m_txnId, change)))
			{
				unlockWithOutCheck();
				dsReturn(getLocalStatus());
			}
			tail = tail->next;
			dsOk();
		}
		dsStatus& drop(bufferPool* pool, clientHandle* client)
		{
			if (tail == nullptr || tail->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
				dsFailed(errorCode::ROW_NOT_EXIST, "can not delete, row not exist");
			if (!lock(client->m_uid))
				dsFailed(errorCode::GET_LOCK_FAILED, "get lock failed");
			if (tail == nullptr || tail->data.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DELETE))
			{
				unlockWithOutCheck();
				dsFailed(errorCode::ROW_NOT_EXIST, "can not delete, row not exist");
			}
			if (!dsCheck(tail->drop(pool, client->m_txnId)))
			{
				unlockWithOutCheck();
				dsReturn(getLocalStatus());
			}
			tail = tail->next;
			dsOk();
		}
		inline void commit()
		{
			current = tail;
			unlockWithOutCheck();
		}
		inline void rollback()
		{
			version* next = current->next;
			current->next = nullptr;
			unlockWithOutCheck();
			while (next != nullptr)
			{
				version* tmp = next->next;
				bufferPool::free(next);
				next = tmp;
			}
		}
		static void destroy(row * r)
		{
			for (version* v = r->head, *tmp; v != nullptr;)
			{
				tmp = v->next;
				bufferPool::free(v);
				v = tmp;
			}
			bufferPool::free(r);
		}
	};
}
