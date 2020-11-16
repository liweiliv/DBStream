#pragma once
#include "util/status.h"
#include "clientHandel.h"
#include "thread/shared_mutex.h"
#include "util/sparsepp/spp.h"
#include "table.h"
namespace KVDB
{
	class database {
	private:
		spp::sparse_hash_map<std::string, tableInterface*> m_tableMap;
		shared_mutex m_lock;
	public:
		dsStatus& insert(bufferPool* pool, clientHandle* client)
		{
			m_lock.lock_shared();
			spp::sparse_hash_map<std::string, tableInterface*>::iterator iter = m_tableMap.find(client->m_change->table);
			if (iter == m_tableMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::TABLE_NOT_EXIST, "table not exist", WARNING);
			}
			if (unlikely(!dsCheck(iter->second->insert(pool, client))))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			else
			{
				m_lock.unlock_shared();
				dsOk();
			}
		}
		dsStatus& update(bufferPool* pool, clientHandle* client)
		{
			m_lock.lock_shared();
			spp::sparse_hash_map<std::string, tableInterface*>::iterator iter = m_tableMap.find(client->m_change->table);
			if (iter == m_tableMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::TABLE_NOT_EXIST, "table not exist", WARNING);
			}
			if (unlikely(!dsCheck((iter->second->update(pool, client)))))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			else
			{
				m_lock.unlock_shared();
				dsOk();
			}
		}
		dsStatus& drop(bufferPool* pool, clientHandle* client)
		{
			m_lock.lock_shared();
			spp::sparse_hash_map<std::string, tableInterface*>::iterator iter = m_tableMap.find(client->m_change->table);
			if (iter == m_tableMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::TABLE_NOT_EXIST, "table not exist", WARNING);
			}
			if (unlikely(!dsCheck(iter->second->drop(pool, client))))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			else
			{
				m_lock.unlock_shared();
				dsOk();
			}
		}

		dsStatus& select(bufferPool* pool, clientHandle* client)
		{
			m_lock.lock_shared();
			spp::sparse_hash_map<std::string, tableInterface*>::iterator iter = m_tableMap.find(client->m_change->table);
			if (iter == m_tableMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::TABLE_NOT_EXIST, "table not exist", WARNING);
			}
			if (unlikely(!dsCheck(iter->second->drop(pool, client))))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			else
			{
				m_lock.unlock_shared();
				dsOk();
			}
		}

		dsStatus& createTable(clientHandle* client, META::tableMeta* meta)
		{
			if (meta->m_primaryKey == nullptr)
				dsFailedAndLogIt(errorCode::PRIMARYKEY_NOT_EXIST, "table must have primary key", WARNING);
			tableInterface* t;
			if (meta->m_primaryKey->columnCount > 1)
				t = new table<META::unionKey>(meta->m_tableName.c_str(), meta);
			else
			{
				switch (meta->getColumn(meta->m_primaryKey->columnInfo[0].columnId)->m_columnType)
				{
				case META::COLUMN_TYPE::T_INT32:
					t = new table<int32_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_INT64:
					t = new table<int64_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_STRING:
					t = new table<META::binaryType>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_UINT32:
					t = new table<uint32_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_UINT64:
					t = new table<uint64_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_INT16:
					t = new table<int16_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_INT8:
					t = new table<int8_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_UINT16:
					t = new table<uint16_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_UINT8:
					t = new table<uint8_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_JSON:
				case META::COLUMN_TYPE::T_XML:
				case META::COLUMN_TYPE::T_BIG_NUMBER:
					t = new table<META::binaryType>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_DATE:
				case META::COLUMN_TYPE::T_TIME:
					t = new table<int32_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_DATETIME:
					t = new table<int64_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_TIMESTAMP:
					t = new table<uint64_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_YEAR:
					t = new table<int16_t>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_FLOAT:
					t = new table<float>(meta->m_tableName.c_str(), meta);
					break;
				case META::COLUMN_TYPE::T_DOUBLE:
					t = new table<double>(meta->m_tableName.c_str(), meta);
					break;
				default:
					dsFailedAndLogIt(errorCode::PRIMARYKEY_NOT_SUPPORT_COLUMN_TYPE, "table must have primary key", WARNING);
				}
			}

			spp::sparse_hash_map<std::string, tableInterface*>::iterator iter = m_tableMap.find(meta->m_tableName);
			if (iter != m_tableMap.end())
			{
				delete t;
				dsFailedAndLogIt(errorCode::TABLE_EXIST, "table exist", WARNING);
			}
			m_lock.lock();
			iter = m_tableMap.find(meta->m_tableName);
			if (iter != m_tableMap.end())
			{
				m_lock.unlock();
				delete t;
				dsFailedAndLogIt(errorCode::TABLE_EXIST, "table exist", WARNING);
			}
			m_tableMap.insert(std::pair<std::string, tableInterface*>(meta->m_tableName, t));
			m_lock.unlock();
			dsOk();
		}
	};
}
