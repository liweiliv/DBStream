#include "instance.h"
#include "transaction.h"
#include "errorCode.h"
#include "rowChange.h"
#include "database.h"
#include "memory/bufferPool.h"
#include "temporaryTables.h"
namespace KVDB
{
	dsStatus& instance::startTrans(clientHandle* handle)
	{
		if (handle->m_trans->m_start)
			dsFailedAndLogIt(errorCode::TRANSACTION_HAS_START, "transaction has started", WARNING);
		handle->m_txnId = m_tid.fetch_add(1);
		handle->m_trans->m_start = true;
		handle->m_trans->clear();
		dsOk();
	}
	dsStatus& instance::rowChange(clientHandle* handle)
	{
		if (handle->m_change == nullptr)
			dsFailed(errorCode::INVALID_ROW_CHANGE, "invalid row change", WARNING);
		if (handle->m_change->table.empty())
			dsFailed(errorCode::NO_TABLE_SELECTED, "No table selected", WARNING);

		dbMap::iterator iter;
		if (handle->m_change->database.empty())
		{
			if (handle->m_currentDatabase.empty())
				dsFailed(errorCode::NO_DATABASE_SELECTED, "No database selected", WARNING);
			m_lock.lock_shared();
			if ((iter = m_dbMap.find(handle->m_currentDatabase)) == m_dbMap.end())
			{
				m_lock.unlock_shared();
				dsFailed(errorCode::DATABASE_NOT_EXIST, "database not exist", WARNING);
			}
		}
		else
		{
			m_lock.lock_shared();
			if ((iter = m_dbMap.find(handle->m_change->database)) == m_dbMap.end())
			{
				m_lock.unlock_shared();
				dsFailed(errorCode::DATABASE_NOT_EXIST, "database not exist", WARNING);
			}
		}
		database* db = iter->second;
		switch (handle->m_change->type)
		{
		case DATABASE_INCREASE::RecordType::R_INSERT:
			if (!dsCheck(db->insert(m_pool, handle)))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			dsOk();
		case DATABASE_INCREASE::RecordType::R_UPDATE:
			if (!dsCheck(db->update(m_pool, handle)))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			dsOk();
		case DATABASE_INCREASE::RecordType::R_DELETE:
			if (!dsCheck(db->drop(m_pool, handle)))
			{
				m_lock.unlock_shared();
				dsReturn(getLocalStatus());
			}
			dsOk();
		default:
			m_lock.unlock_shared();
			dsFailedAndLogIt(errorCode::UNKNOWN_OPERATION_TYPE, "unknown operation type", WARNING);
		}
	}
	dsStatus& instance::select(clientHandle* handle)
	{
		dsOk();
	}
	dsStatus& instance::ddl(clientHandle* handle)
	{
		dsOk();
	}
	dsStatus& instance::commit(clientHandle* handle)
	{
		if (!dsCheck(handle->m_trans->commit(nullptr)))
			dsReturn(getLocalStatus());

		dsOk();
	}
	dsStatus& instance::rollback(clientHandle* handle)
	{
		if (!dsCheck(handle->m_trans->rollback()))
			dsReturn(getLocalStatus());
		dsOk();
	}
}