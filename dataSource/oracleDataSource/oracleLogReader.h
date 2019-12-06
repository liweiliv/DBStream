#pragma once
#include "occi.h"
#include "util/config.h"
#include <stdint.h>
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
#define GET_REDO_LOG_FILE_BY_SCN "SELECT MEMBER, FIRST_CHANGE#, NEXT_CHANGE#, SEQUENCE# FROM V$LOG L JOIN v$logfile LF ON L.GROUP#=LF.GROUP#  WHERE FIRST_CHANGE#<=%llu AND (NEXT_CHANGE#>%llu OR NEXT_CHANGE#=null) AND ROWNUM<=1 AND THREAD#=? ORDER BY FIRST_CHANGE# DESC"
	class oracleLogReader {
	private:
		oracle::occi::Connection* m_conn;
		config* m_conf;
		std::string m_currentFileName;
		uint64_t m_currentScn;
		uint64_t m_currenttTimestamp;
	private:
		int getLogFileByScn(uint64_t scn)
		{
			char sql[sizeof(GET_REDO_LOG_FILE_BY_SCN) + 80];
			sprintf(sql, GET_REDO_LOG_FILE_BY_SCN, scn, scn);
			oracle::occi::Statement *stmt = m_conn->createStatement(sql);
			if(stmt)
			stmt->executeQuery();
		}
	public:
		oracleLogEntry* read()
		{

		}
	};
}