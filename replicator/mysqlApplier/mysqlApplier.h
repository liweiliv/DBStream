#pragma once
#include "replicator/applier.h"
#include "mysqlConnector.h"
#include "util/itoaSse.h"
#include "meta/metaData.h"
#include "replicator/tableInfo.h"
class mysqlConnector;
namespace REPLICATOR {
	class mysqlApplier:public applier {
	private:
		MYSQL* m_connect;
		mysqlConnector  m_connector;
		char* m_defaultSqlBuffer;
		int64_t m_defaultSqlBufferSize;
		int64_t m_sqlBufferSize;
		int64_t m_sqlBufferPos;
		std::string m_txnTableSqlHead;
	public:
		mysqlApplier(uint32_t id,Config * conf):applier(id,conf),m_connect(nullptr),m_connector(conf), m_sqlBufferPos(0)
		{
			m_defaultSqlBuffer = new char[m_defaultSqlBufferSize = 1024 * 32];
			if (!m_txnTable.empty())
				m_txnTableSqlHead.append("insert into `").append(m_txnDatabase).append("`.`").append(m_txnTable).append("` (`id`,`checkpoint`,`timestamp`) values(");
		}
		virtual ~mysqlApplier() 
		{
			if (m_connect != nullptr) 
			{
				mysql_close(m_connect);
				m_connect = nullptr;
			}
			delete[]m_defaultSqlBuffer;
		}
		virtual int reconnect()
		{
			if (m_connect != nullptr)
			{
				mysql_close(m_connect);
				m_connect = nullptr;
			}
			int32_t connErrno;
			const char* connError;
			if (nullptr == (m_connect = m_connector.getConnect(connErrno, connError)))
			{
				m_errno = connErrno;
				m_error = connError == nullptr ? "" : connError;
				LOG(ERROR) << "mysqlApplier connect to database failed";
				return -1;
			}
			return 0;
		}
		void createTxnTableSql(char * sqlBuffer,const RPC::Record* record);
		int addColumnValue(char* sqlBuffer, const META::ColumnMeta* column, RPC::DMLRecord* dml, bool newOrOld);
		inline void reallocSqlBuf(char*& buffer, int64_t needed)
		{
			char* newBuffer = (char*)malloc(m_sqlBufferSize = (m_sqlBufferSize + needed + m_sqlBufferSize));
			memcpy(newBuffer, buffer, m_sqlBufferPos);
			if (buffer != m_defaultSqlBuffer)
				free(buffer);
			buffer = m_defaultSqlBuffer;
		}
		inline void createBegin()
		{
			memcpy(m_defaultSqlBuffer + m_sqlBufferPos, "BEGIN;\n", 7);
			m_sqlBufferPos += 7;
		}
		inline void createCommit(char*& sqlBuffer)
		{
			if (m_sqlBufferPos + 9 < m_sqlBufferSize)
				reallocSqlBuf(sqlBuffer, 9);
			memcpy(sqlBuffer + m_sqlBufferPos, "COMMIT;\n", 8);
			m_sqlBufferPos += 8;
		}
		int createMergedDMLSql(replicatorRecord* record, char*& sqlBuffer);
		int createDDLSql(replicatorRecord* record, char*& sqlBuffer)
		{
			const RPC::DDLRecord* ddl = static_cast<const RPC::DDLRecord*>(record->record);
			if (ddl->databaseSize() > 0)
			{
				memcpy(sqlBuffer+ m_sqlBufferPos, "USE `", sizeof("USE `") - 1);
				m_sqlBufferPos = sizeof("USE `") - 1;
				m_sqlBufferPos += mysql_real_escape_string_quote(m_connect, sqlBuffer + m_sqlBufferPos, ddl->database, ddl->databaseSize(), '\\');
				sqlBuffer[m_sqlBufferPos++] = '`';
				sqlBuffer[m_sqlBufferPos++] = ';';
			}
			memcpy(sqlBuffer + m_sqlBufferPos, ddl->ddl, ddl->ddlSize());
			m_sqlBufferPos += ddl->ddlSize();
			sqlBuffer[m_sqlBufferPos++] = ';';
			sqlBuffer[m_sqlBufferPos++] = '\n';
			return 0;
		}
		int createSingleDMLSql(replicatorRecord* record, char*& sqlBuffer);
		int optimize(transaction* t, char*& sqlBuffer);
		int createWhereConditionInSql(char*& sqlBuffer, RPC::DMLRecord* dml);
		int createDMLSql(replicatorRecord* record, char*& sqlBuffer)
		{
			if (record->mergeNext == nullptr)
				return createSingleDMLSql(record, sqlBuffer);
			else
				return createMergedDMLSql(record, sqlBuffer);
		}
		int runSql(char *&sqlBuffer)
		{
			sqlBuffer[m_sqlBufferPos] = '\0';
			if (unlikely(!m_dryRun&&0 != mysql_real_query(m_connect, sqlBuffer, m_sqlBufferPos)))
			{
				m_errno = mysql_errno(m_connect);
				m_error = mysql_error(m_connect);
				LOG(ERROR) << "sql :" << sqlBuffer << " run failed for:" << m_errno << "," << m_error;
				return -1;
			}
			if (unlikely(m_printSql))
			{
				LOG(INFO) << sqlBuffer;
			}
			if (sqlBuffer != m_defaultSqlBuffer)
			{
				free(sqlBuffer);
				sqlBuffer = m_defaultSqlBuffer;
				m_sqlBufferSize = m_defaultSqlBufferSize;
			}
			m_sqlBufferPos = 0;
			return 0;
		}
		virtual int rollback(char *& sqlBuffer)
		{
			memcpy(sqlBuffer, "ROLLBACK;\n", sizeof("ROLLBACK;\n") - 1);
			m_sqlBufferPos = sizeof("ROLLBACK;\n") - 1;
			return runSql(sqlBuffer);
		}
		virtual int apply(transaction* t)
		{
			if (unlikely(t == nullptr))
				return -1;
			transaction* current = t;
			char* sqlBuffer = m_defaultSqlBuffer;
			m_sqlBufferSize = m_defaultSqlBufferSize;
			m_sqlBufferPos = 0;

			if (t->recordCount > 1|| t->mergeNext!=nullptr||(!m_txnTable.empty()&& !(t->firstRecord->record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL))))
			{
				createBegin();
				if (!m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
					return -1;
			}
			if (!m_txnTable.empty() && !(t->recordCount == 1 && t->firstRecord->record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL)))
			{
				createTxnTableSql(sqlBuffer, t->firstRecord->record);
				if (!m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
					return -1;
			}
			while (current != nullptr)
			{
				replicatorRecord* record = current->firstRecord;
				if (likely(record != nullptr))
				{
					while (true)
					{
						if (record->mergeNext == nullptr || record->mergeNext->record->head->recordId > record->mergePrev->record->head->recordId)
						{
							if (record->record->head->minHead.type <= static_cast<uint8_t>(RPC::RecordType::R_REPLACE))
							{
								if (0 != createDMLSql(record, sqlBuffer))
								{
									rollback(sqlBuffer);
									return -1;
								}
							}
							else if (record->record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL))
							{

							}
							if (!m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
								return -1;
						}
						if (record == current->lastRecord)
							break;
						record = record->nextInTrans;
					}
				}
				current = t->mergeNext;
			}
			if (t->recordCount > 1 || t->mergeNext != nullptr || (!m_txnTable.empty() && !(t->firstRecord->record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL))))
			{
				createCommit(sqlBuffer);
				if (!m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
					return -1;
			}
			if (m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
				return -1;
			return 0;
		}
	};
	extern "C" DLL_EXPORT  applier * instance(int id,Config * conf)
	{
		LOG(INFO) << "create new mysqlApplier,id:" << id;
		return new mysqlApplier(id,conf);
	}
}
