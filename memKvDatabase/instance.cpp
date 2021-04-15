#include "instance.h"
#include "transaction.h"
#include "errorCode.h"
#include "rowChange.h"
#include "database.h"
#include "memory/bufferPool.h"
#include "temporaryTables.h"
#include "walLogWriter.h"
#include "util/arrayQueue.h"
#include "service.h"
namespace KVDB
{
	DS instance::startTrans(clientHandle* handle)
	{
		if (handle->m_trans->m_start)
			dsFailedAndLogIt(errorCode::TRANSACTION_HAS_START, "transaction has started", WARNING);
		handle->m_txnId = m_tid.fetch_add(1);
		handle->m_trans->m_start = true;
		handle->m_trans->clear();
		dsOk();
	}
	DS instance::rowChange(clientHandle* handle)
	{
		if (handle->m_change == nullptr)
			dsFailedAndLogIt(errorCode::INVALID_ROW_CHANGE, "invalid row change", WARNING);
		if (handle->m_change->table.empty())
			dsFailedAndLogIt(errorCode::NO_TABLE_SELECTED, "No table selected", WARNING);

		dbMap::iterator iter;
		if (handle->m_change->database.empty())
		{
			if (handle->m_currentDatabase.empty())
				dsFailedAndLogIt(errorCode::NO_DATABASE_SELECTED, "No database selected", WARNING);
			m_lock.lock_shared();
			if ((iter = m_dbMap.find(handle->m_currentDatabase)) == m_dbMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::DATABASE_NOT_EXIST, "database not exist", WARNING);
			}
		}
		else
		{
			m_lock.lock_shared();
			if ((iter = m_dbMap.find(handle->m_change->database)) == m_dbMap.end())
			{
				m_lock.unlock_shared();
				dsFailedAndLogIt(errorCode::DATABASE_NOT_EXIST, "database not exist", WARNING);
			}
		}
		database* db = iter->second;
		switch (handle->m_change->type)
		{
		case DATABASE_INCREASE::RecordType::R_INSERT:
			dsReturnWithOp(db->insert(m_pool, handle), m_lock.unlock_shared());
		case DATABASE_INCREASE::RecordType::R_UPDATE:
			dsReturnWithOp(db->update(m_pool, handle), m_lock.unlock_shared());
		case DATABASE_INCREASE::RecordType::R_DELETE:
			dsReturnWithOp(db->drop(m_pool, handle), m_lock.unlock_shared());
		default:
			m_lock.unlock_shared();
			dsFailedAndLogIt(errorCode::UNKNOWN_OPERATION_TYPE, "unknown operation type", WARNING);
		}
	}
	DS instance::select(clientHandle* handle)
	{
		dsOk();
	}
	DS instance::ddl(clientHandle* handle)
	{
		dsOk();
	}
	DS instance::commit(clientHandle* handle)
	{
		dsReturn(handle->m_trans->commit());
	}
	DS instance::rollback(clientHandle* handle)
	{
		dsReturn(handle->m_trans->rollback());
	}
	DS instance::asyncWriteWalLog(clientHandle* handle)
	{
		if (m_walLogWriter == nullptr)
			dsOk();
		dsReturn(m_walLogWriter->writeTrans(handle->m_trans->m_redoListHead));
	}
	void instance::workThread(int tid)
	{
		clientHandle* current;
		while (m_running)
		{
			if (m_walFinishTask.pop(current))
			{
				if (!dsCheck(current->m_trans->commit()))
				{

				}
			}
		}
	}

	void instance::walWriteThread()
	{
		clientHandle* current;
		while (m_running)
		{
			if (m_walTask.pop(current))
			{
				if (unlikely(!dsCheck(m_walLogWriter->writeTrans(current->m_trans->m_redoListHead))))
				{
					m_running = false;
					m_error = getLocalStatus();
					LOG(ERROR) << "write wal log failed,error info:" << m_error.toString();
					break;
				}
			}
		}
	}



}