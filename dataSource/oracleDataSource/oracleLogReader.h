#pragma once
#include "occi.h"
#include "oci.h"
#include "util/config.h"
#include <stdint.h>
#include "oracleConnect.h"
#include "oracleRawDataParse.h"
#include "thread/threadPool.h"
namespace DATA_SOURCE
{
	struct oracleLogEntry {
		uint64_t scn;
		uint64_t timestamp;
		uint64_t tableId;
		char txnId[20];
		char rowId[20];
		uint32_t sqlSize;
		char sql[1];
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
		};
		oracleConnect* m_connector;
		oracle::occi::Connection* m_conn;
		config* m_conf;
		std::string m_currentFileName;
		uint64_t m_currentScn;
		uint64_t m_currenttTimestamp;
		threadPool<oracleLogReader, void> m_readThreads;
	private:
		void getLogFileByScnFromResult(oracle::occi::ResultSet* rs, oracleLogFile& fileInfo)
		{
			oracle::occi::Number number;
			fileInfo.fileName = rs->getString(1);
			if (!rs->isNull(2))
			{
				number = rs->getNumber(2);
				fileInfo.firstChange = oracleNumberToInt64((uint8_t*)&number);
			}
			else
				fileInfo.firstChange = ULLONG_MAX;
			if (!rs->isNull(3))
			{
				number = rs->getNumber(3);
				fileInfo.nextChange = oracleNumberToInt64((uint8_t*)&number);
			}
			else
				fileInfo.nextChange = ULLONG_MAX;
			if (!rs->isNull(4))
				fileInfo.sequence = rs->getInt(4);
			else
				fileInfo.sequence = UINT_MAX;
		}
		int getLogFileByScn(oracle::occi::Connection*& conn, uint64_t scn, int threadId, oracleLogFile& fileInfo)
		{
			char sql[sizeof(GET_REDO_LOG_FILE_BY_SCN) + 80];
			sprintf(sql, GET_REDO_LOG_FILE_BY_SCN, scn, scn, threadId);
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::Statement* stmt = nullptr;
			try {
				stmt = m_conn->createStatement();
				if (stmt == nullptr)
				{
					LOG(ERROR) << "createStatement failed ";
					return -1;
				}
				rs = stmt->executeQuery(sql);
				if (rs == nullptr || !rs->next())
				{
					if (rs != nullptr)
						stmt->closeResultSet(rs);
					sprintf(sql, GET_ARCHIVED_LOG_FILE_BY_SCN, scn, scn, threadId);
					rs = stmt->executeQuery(sql);
					if (rs == nullptr || !rs->next())
					{
						if (rs != nullptr)
							stmt->closeResultSet(rs);
						conn->terminateStatement(stmt);
						LOG(ERROR) << "get log file by scn:" << scn << " failed for empty result";
						return -1;
					}
				}
				getLogFileByScnFromResult(rs, fileInfo);
				LOG(INFO) << "find scn " << scn << " in log file:" << fileInfo.fileName;
				stmt->closeResultSet(rs);
				rs = nullptr;
				conn->terminateStatement(stmt);
			}
			catch (oracle::occi::SQLException exp) {
				LOG(ERROR) << "getLogFileByScn failed for:" << exp.getErrorCode() << ":" << exp.getMessage();
				if (rs != nullptr)
					stmt->closeResultSet(rs);
				if (stmt != nullptr)
					conn->terminateStatement(stmt);
				return -1;
			}
			return 0;
		}
		bool getCurrentScnFromDatabase(oracle::occi::Connection*& conn, uint64_t &scn)
		{
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::Statement* stmt = nullptr;
			scn = 0;
			try {
				stmt = m_conn->createStatement();
				if (stmt == nullptr)
				{
					LOG(ERROR) << "createStatement failed ";
					return -1;
				}
				rs = stmt->executeQuery("SELECT CURRENT_SCN FROM V$DATABASE");
				if (rs == nullptr || !rs->next())
				{
					if (rs != nullptr)
						stmt->closeResultSet(rs);
					conn->terminateStatement(stmt);
					LOG(ERROR) << "get current scn from database failed for empty result";
					return false;
				}
				oracle::occi::Number number = rs->getNumber(1);
				scn = oracleNumberToInt64((const uint8_t*)&number);
				stmt->closeResultSet(rs);
				rs = nullptr;
				conn->terminateStatement(stmt);
				return true;
			}
			catch (oracle::occi::SQLException exp) {
				LOG(ERROR) << "get current scn from database failed for :" << exp.getErrorCode() << ":" << exp.getMessage();
				if (rs != nullptr)
					stmt->closeResultSet(rs);
				if (stmt != nullptr)
					conn->terminateStatement(stmt);
				return false;
			}
		}
	public:
		oracleLogEntry* read()
		{

		}
	};
}