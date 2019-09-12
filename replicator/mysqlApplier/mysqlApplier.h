#pragma once
#include "replicator/applier.h"
#include "mysqlConnector.h"
#include "util/itoaSse.h"
#include "meta/metaData.h"
class mysqlConnector;
namespace REPLICATOR {
	class mysqlApplier:public applier {
	private:
		MYSQL* m_connect;
		mysqlConnector  m_connector;
		char* m_defaultSqlBuffer;
		int64_t m_defaultSqlBufferSize;
		int64_t m_sqlBufferPos;
		bool m_batchCommit;
		bool m_dryRun;
		bool m_printSql;
	public:
		mysqlApplier(config * conf):applier(), m_connector(conf){}
		virtual ~mysqlApplier() {}
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
		int addColumnValue(char* sqlBuffer, const META::columnMeta* column, DATABASE_INCREASE::DMLRecord* dml);
		void reallocSqlBuf(char*& buffer, int64_t& bufferSize, int64_t needed)
		{
			char* newBuffer = (char*)malloc(bufferSize = (bufferSize + needed + bufferSize));
			memcpy(newBuffer, buffer, m_sqlBufferPos);
			if (buffer != m_defaultSqlBuffer)
				free(buffer);
			buffer = m_defaultSqlBuffer;
		}
		void createBegin()
		{
			memcpy(m_defaultSqlBuffer + m_sqlBufferPos, "BEGIN;\n", 7);
			m_sqlBufferPos += 7;
		}
		void createCommit(char*& sqlBuffer, int64_t& sqlBufferSize)
		{
			if (m_sqlBufferPos + 9 < sqlBufferSize)
				reallocSqlBuf(sqlBuffer, sqlBufferSize, 9);
			memcpy(sqlBuffer + m_sqlBufferPos, "COMMIT;\n", 8);
			m_sqlBufferPos += 8;
		}
		int createMergedDMLSql(replicatorRecord* record, char*& sqlBuffer, int64_t& sqlBufferSize)
		{

		}
		void addTableName(char* sqlBuffer, const META::tableMeta * meta)
		{
			sqlBuffer[m_sqlBufferPos++] = ' ';
			sqlBuffer[m_sqlBufferPos++] = '`';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, meta->m_dbName.c_str(), meta->m_dbName.size());
			sqlBuffer[m_sqlBufferPos++] = '`';
			sqlBuffer[m_sqlBufferPos++] = '.';
			sqlBuffer[m_sqlBufferPos++] = '`';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, meta->m_tableName.c_str(), meta->m_tableName.size());
			sqlBuffer[m_sqlBufferPos++] = '`';
			sqlBuffer[m_sqlBufferPos++] = ' ';
		}
		
		int createSingleDMLSql(replicatorRecord* record, char*& sqlBuffer, int64_t& sqlBufferSize);
		int createDMLSql(replicatorRecord* record, char*& sqlBuffer, int64_t& sqlBufferSize)
		{
			if (record->mergeNext == nullptr)
				return createSingleDMLSql(record, sqlBuffer, sqlBufferSize);
			else
				return createMergedDMLSql(record, sqlBuffer, sqlBufferSize);
		}
		int runSql(char *&sqlBuffer, int64_t &sqlBufferSize)
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
				sqlBufferSize = m_defaultSqlBufferSize;
			}
			m_sqlBufferPos = 0;
		}
		virtual int apply(transaction* t)
		{
			transaction* current = t;
			char* sqlBuffer = m_defaultSqlBuffer;
			int64_t sqlBufferSize = m_defaultSqlBufferSize;
			if (t->recordCount > 1|| t->mergeNext!=nullptr)
			{
				createBegin();
				if (!m_batchCommit && unlikely(runSql(sqlBuffer, sqlBufferSize) != 0))
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
							if (!m_batchCommit && unlikely(runSql(sqlBuffer, sqlBufferSize) != 0))
								return -1;
						}
						if (record == current->lastRecord)
							break;
						record = record->nextInTrans;
					}
				}
				current = t->mergeNext;
			}
			if (t->recordCount > 1 || t->mergeNext != nullptr)
			{
				createCommit(sqlBuffer, sqlBufferSize);
				if (!m_batchCommit && unlikely(runSql(sqlBuffer, sqlBufferSize) != 0))
					return -1;
			}
			if (m_batchCommit && unlikely(runSql(sqlBuffer, sqlBufferSize) != 0))
				return -1;
			return 0;
		}
	};
}
