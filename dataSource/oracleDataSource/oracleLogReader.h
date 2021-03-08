#pragma once
#include "occi.h"
#include "oci.h"
#include "util/config.h"
#include <stdint.h>
#include "oracleConnect.h"
#include "oracleRawDataParse.h"
#include "thread/threadPool.h"
#include "memory/ringBuffer.h"
namespace DATA_SOURCE
{
	struct oracleLogEntry {
		uint64_t scn;
		uint64_t timestamp;
		uint64_t tableId;
		uint64_t rba;
		char txnId[20];
		char rowId[20];
		uint32_t sqlSize;
		char sql[1];
	};
	enum class ORACLE_READER_CODE {
		OK,
		NEED_RECONNECT,
		ALL_RESULT_READED,
		NEED_RELOCATE_LOG,
		FATAL_ERROR
	};
	constexpr auto GET_REDO_LOG_FILE_BY_SCN = "SELECT MEMBER, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# FROM V$LOG L JOIN v$logfile LF ON L.GROUP#=LF.GROUP#  WHERE FIRST_CHANGE#<=%llu AND (NEXT_CHANGE#>%llu OR NEXT_CHANGE#=null) AND ROWNUM<=1 AND THREAD#=%d ORDER BY FIRST_CHANGE# DESC";

	constexpr auto GET_ARCHIVED_LOG_FILE_BY_SCN = "SELECT NAME, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# FROM V$ARCHIVED_LOG WHERE FIRST_CHANGE#<=%llu AND NEXT_CHANGE#>%llu  AND THREAD#=%d AND DELETED = 'NO'  ORDER BY FIRST_CHANGE# DESC ";;
	class oracleLogReader {
	private:
		struct oracleLogFile {
			std::string fileName;
			uint64_t firstChange;
			uint64_t nextChange;
			uint32_t sequence;
			bool isRedo;
			bool isCurrent;
			uint32_t archivedThread;
			std::string status;
		};
		struct oracleInstance
		{
			int threadId;
			uint64_t currentScn;
			uint64_t currenttTimestamp;
			oracleLogFile currentLogFile;
			
			oracle::occi::Connection* conn;
			oracle::occi::Statement* readStmt;
			oracle::occi::ResultSet* readRs;
		};
		oracleConnect* m_connector;
		oracle::occi::Connection* m_conn;
		config* m_conf;
		std::string m_currentFileName;
		uint64_t m_currentScn;
		uint64_t m_currenttTimestamp;
		threadPool<oracleLogReader, void> m_readThreads;
	private:
		ORACLE_READER_CODE logminer(oracleInstance* instance)
		{
			ORACLE_READER_CODE code;
			try
			{
				if (instance->readRs == nullptr)
				{
					if (instance->readStmt == nullptr)
					{
						if (instance->conn == nullptr && !dsCheck(m_connector->connect(instance->conn)))
							return ORACLE_READER_CODE::FATAL_ERROR;
						instance->readStmt = instance->conn->createStatement();
					}
					std::string currentFile = instance->currentLogFile.fileName;
					if (ORACLE_READER_CODE::OK != (code = getLogFileByScn(instance->conn, instance->currentScn, instance->threadId, instance->currentLogFile)))
					{
						LOG(ERROR) << "oracle instance threadId :" << instance->threadId << " getLogFileByScn :" << instance->currentScn << " failed";
						return code;
					}
					oracle::occi::Number start;
					oracle::occi::Number end;

					if (currentFile != instance->currentLogFile.fileName)
					{
						instance->readStmt->execute("BEGIN SYS.DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME = > ? , OPTIONS = > DBMS_LOGMNR.NEW); END");
						uint64ToOracleNumber((OCINumber*)&start, instance->currentLogFile.firstChange);
					}
					else
					{
						uint64ToOracleNumber((OCINumber*)&start, instance->currentScn);
					}
					if (instance->currentLogFile.nextChange != ULLONG_MAX)
						uint64ToOracleNumber((OCINumber*)&end, instance->currentLogFile.nextChange);
					else
					{
						uint64_t databaseCurrentScn = 0;
						if (ORACLE_READER_CODE::OK != (code = getCurrentScnFromDatabase(instance->conn, databaseCurrentScn)))
						{
							LOG(ERROR) << "oracle instance threadId :" << instance->threadId << " getCurrentScnFromDatabase  failed";
							return code;
						}
						uint64ToOracleNumber((OCINumber*)&end, databaseCurrentScn);
					}

					instance->readStmt->setSQL("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?);END;");
					instance->readStmt->setNumber(1, start);
					instance->readStmt->execute();

					instance->readStmt->setSQL("SELECT SQL_REDO , SCN, TIMESTAMP, XID, OPERATION_CODE, ROLLBACK, ROW_ID, DATA_OBJ#, CSF  FROM V$LOGMNR_CONTENTS");
					instance->readStmt->setPrefetchMemorySize(1024 * 1024);
					instance->readRs = instance->readStmt->executeQuery();
				}
			}
			catch (oracle::occi::SQLException exp) {
				LOG(ERROR) << "oracle instance threadId :" << instance->threadId << " do logminer failed for :" << exp.getErrorCode() << ":" << exp.getMessage();
				if (oracleConnect::errorRecoverable(exp.getErrorCode()))
					return ORACLE_READER_CODE::NEED_RECONNECT;
				else
					return ORACLE_READER_CODE::FATAL_ERROR;
			}
		}
		ORACLE_READER_CODE readRowFromResult(oracleInstance* instance, oracleLogEntry*& entry)
		{
			try
			{
				if (!instance->readRs->next())
				{
					instance->readStmt->closeResultSet(instance->readRs);
					instance->readRs = nullptr;
					return ORACLE_READER_CODE::ALL_RESULT_READED;
				}
			}
			catch (oracle::occi::SQLException exp)
			{
				LOG(ERROR) << "oracle instance threadId :" << instance->threadId << " readRowFromResult failed for :" << exp.getErrorCode() << ":" << exp.getMessage();
				if (oracleConnect::errorRecoverable(exp.getErrorCode()))
					return ORACLE_READER_CODE::NEED_RECONNECT;
				else
					return ORACLE_READER_CODE::FATAL_ERROR;
			}
		}
		ORACLE_READER_CODE read(oracleInstance* instance, oracleLogEntry *&entry)
		{
			ORACLE_READER_CODE code;
			int8_t retry = 10;
RETRY:
			if (instance->readRs == nullptr)
			{
				if ((code = logminer(instance)) != ORACLE_READER_CODE::OK)
					goto CHECK;
			}
			if ((code = readRowFromResult(instance, entry)) != ORACLE_READER_CODE::OK)
				goto CHECK;
			return ORACLE_READER_CODE::OK;
CHECK:
			switch (code)
			{
			case ORACLE_READER_CODE::NEED_RECONNECT:
				m_connector->close(instance->conn, instance->readStmt, instance->readRs);
				if (!dsCheck(m_connector->connect(instance->conn)))
					return ORACLE_READER_CODE::FATAL_ERROR;
				std::this_thread::sleep_for(std::chrono::milliseconds(1000));//sleep 1s every reconect
			case ORACLE_READER_CODE::NEED_RELOCATE_LOG:
				instance->currentLogFile.fileName.clear();
			case ORACLE_READER_CODE::ALL_RESULT_READED:
				if (--retry >= 0)
					goto RETRY;
				else
					LOG(ERROR) << "oracle instance threadId :" << instance->threadId << " read log failed";
			default:
				return ORACLE_READER_CODE::FATAL_ERROR;
			}
		}
		void readThread(int threadId)
		{

		}
		void getLogFileByScnFromResult(oracle::occi::ResultSet* rs, oracleLogFile& fileInfo)
		{
			oracle::occi::Number number;
			fileInfo.fileName = rs->getString(1);
			if (!rs->isNull(2))
			{
				number = rs->getNumber(2);
				oracleNumberToUInt64((uint8_t*)&number, fileInfo.firstChange, true);
			}
			else
				fileInfo.firstChange = ULLONG_MAX;
			if (!rs->isNull(3))
			{
				number = rs->getNumber(3);
				oracleNumberToUInt64((uint8_t*)&number, fileInfo.nextChange, true);
				if (fileInfo.nextChange == 281474976710655LL)
					fileInfo.nextChange = ULLONG_MAX;
			}
			else
				fileInfo.nextChange = ULLONG_MAX;
			if (!rs->isNull(4))
				fileInfo.sequence = rs->getInt(4);
			else
				fileInfo.sequence = UINT_MAX;
		}

		ORACLE_READER_CODE getLogFileByScn(oracle::occi::Connection* conn, uint64_t scn, int threadId, oracleLogFile& fileInfo)
		{
			char sql[sizeof(GET_REDO_LOG_FILE_BY_SCN) + 80];
			sprintf(sql, GET_REDO_LOG_FILE_BY_SCN, scn, scn, threadId);
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::Statement* stmt = nullptr;
			try {
				stmt = m_conn->createStatement();
				rs = stmt->executeQuery(sql);
				if (rs == nullptr || !rs->next())
				{
					if (rs != nullptr)
						stmt->closeResultSet(rs);
					sprintf(sql, GET_ARCHIVED_LOG_FILE_BY_SCN, scn, scn, threadId);
					rs = stmt->executeQuery(sql);
					if (rs == nullptr || !rs->next())
					{
						m_connector->closeStmt(conn, stmt, rs);
						LOG(ERROR) << "get log file by scn:" << scn << " failed for empty result";
						return ORACLE_READER_CODE::FATAL_ERROR;
					}
				}
				getLogFileByScnFromResult(rs, fileInfo);
				LOG(INFO) << "find scn " << scn << " in log file:" << fileInfo.fileName;
				m_connector->closeStmt(conn, stmt, rs);
				return ORACLE_READER_CODE::OK;
			}
			catch (oracle::occi::SQLException exp) {
				LOG(ERROR) << "getLogFileByScn failed for:" << exp.getErrorCode() << ":" << exp.getMessage();
				m_connector->closeStmt(conn, stmt, rs);
				if (oracleConnect::errorRecoverable(exp.getErrorCode()))
					return ORACLE_READER_CODE::NEED_RECONNECT;
				else
					return ORACLE_READER_CODE::FATAL_ERROR;
			}
		}

		ORACLE_READER_CODE getCurrentScnFromDatabase(oracle::occi::Connection* conn, uint64_t& scn)
		{
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::Statement* stmt = nullptr;
			scn = 0;
			try {
				stmt = m_conn->createStatement();
				rs = stmt->executeQuery("SELECT CURRENT_SCN FROM V$DATABASE");
				if (rs == nullptr || !rs->next())
				{
					if (rs != nullptr)
						stmt->closeResultSet(rs);
					conn->terminateStatement(stmt);
					LOG(ERROR) << "get current scn from database failed for empty result";
					return ORACLE_READER_CODE::FATAL_ERROR;
				}
				oracle::occi::Number number = rs->getNumber(1);
				oracleNumberToUInt64((const uint8_t*)&number, scn, true);
				m_connector->closeStmt(m_conn, stmt, rs);
				return ORACLE_READER_CODE::OK;
			}
			catch (oracle::occi::SQLException exp) {
				LOG(ERROR) << "get current scn from database failed for :" << exp.getErrorCode() << ":" << exp.getMessage();
				m_connector->closeStmt(m_conn, stmt, rs);
				if (oracleConnect::errorRecoverable(exp.getErrorCode()))
					return ORACLE_READER_CODE::NEED_RECONNECT;
				else
					return ORACLE_READER_CODE::FATAL_ERROR;
			}
		}
	public:
		oracleLogEntry* read()
		{

		}
	};
}
