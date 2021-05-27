#pragma once
//although through simple configuration, you can use oracleXStreamLogReader to read data from Oracle XStream
//but Oracle XStream is not free for commercial purpose
//[Using the XStream APIs requires purchasing a license for the Oracle GoldenGate product] 
//[https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm]
// 
//more details of oci XStream funcs in https://docs.oracle.com/en/database/oracle/oracle-database/21/lnoci/oci-XStream-functions.html
#include <chrono>
#include <time.h>
#include "glog/logging.h"
#include "message/record.h"
#include "meta/metaData.h"
#include "meta/metaDataCollection.h"
#include "memory/ringBuffer.h"
#include "dataSource/oracleDataSource/ociConnect.h"
#include "dataSource/oracleDataSource/oracleMetaDataCollection.h"
#include "XStreamRecord.h"
namespace DATA_SOURCE
{
	constexpr static auto GET_DB_TIMEZONE_OFFSET = "SELECT TZ_OFFSET(DBTIMEZONE) FROM DUAL";
	class oracleXStreamLogReader
	{
	private:
		ociConnect* m_connect;
		ociConnect::oci* m_currentConn;
		uint64_t m_startScn;
		std::string m_xstreamOutBoundServer;
		lcrHeader m_lcrHeader;
		columnValuesInfo m_newColumns;
		columnValuesInfo m_oldColumns;
		ddlInfo m_ddlInfo;
		lcrAttributes m_attrs;
		chunk m_chunkHead;
		chunk* m_lastChunk;
		int m_chunkCount;
		ringBuffer* m_buffer;
		int32_t m_dbTimezoneOffset;
		oracleMetaDataCollection* m_metaDataCollection;
	public:
		oracleXStreamLogReader(ociConnect* connect, oracleMetaDataCollection* metaDataCollection, const char* serverName, ringBuffer* buffer) :m_connect(connect), m_currentConn(nullptr),
			m_startScn(0), m_xstreamOutBoundServer(serverName), m_lastChunk(nullptr), m_chunkCount(0), m_buffer(buffer), m_metaDataCollection(metaDataCollection)
		{

		}
		~oracleXStreamLogReader()
		{
			if (m_currentConn != nullptr)
				delete m_currentConn;
		}
		DS init()
		{
			dsReturnIfFailed(m_connect->connect(m_currentConn));
			dsReturnIfFailed(updateTimezoneInfo());
			dsOk();
		}
		DS updateTimezoneInfo()
		{
			dsReturnIfFailed(m_currentConn->openStmt());
			if (OCIStmtPrepare(m_currentConn->stmtp, m_currentConn->errp, (const text*)GET_DB_TIMEZONE_OFFSET, (ub4)strlen(GET_DB_TIMEZONE_OFFSET), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT) != OCI_SUCCESS)
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				dsFailedAndLogIt(1, "call OCIStmtPrepare failed for " << errcode << "," << (char*)errBuf, ERROR);
			}
			OCIDefine* defn = nullptr;
			char timezoneOffset[10] = { 0 };
			OCIDefineByPos(m_currentConn->stmtp, &defn, m_currentConn->errp, (ub4)1, timezoneOffset, sizeof(timezoneOffset), SQLT_STR, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);
			if (OCIStmtExecute(m_currentConn->svcp, m_currentConn->stmtp, m_currentConn->errp, (ub4)1, (ub4)0, (OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT) != OCI_SUCCESS)
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << errcode << "," << (char*)errBuf, ERROR);
			}
			do
			{
				m_dbTimezoneOffset = 0;
				int hour = timezoneOffset[1] * 10 + timezoneOffset[2];
				int mininus = timezoneOffset[4] * 10 + timezoneOffset[5];
				m_dbTimezoneOffset = hour * 24 * 3600 + mininus * 60;
				if (timezoneOffset[0] == '-')
					m_dbTimezoneOffset = -m_dbTimezoneOffset;
			} while (OCIStmtFetch(m_currentConn->stmtp, m_currentConn->errp, 1, OCI_FETCH_NEXT, OCI_DEFAULT) == OCI_SUCCESS);
			dsOk();
		}
		DS readLcr()
		{
			void* lcr;
			ub1         lcrType;
			oraub8      flag;
			ub1         fetchLowPosition[OCI_LCR_MAX_POSITION_LEN];
			ub2         fetchLowPositionLength = 0;
			sword status = 0;
			while (status == OCI_SUCCESS)
			{
				while ((status =
					OCIXStreamOutLCRReceive(m_currentConn->svcp, m_currentConn->errp,
						&lcr, &lcrType, &flag,
						fetchLowPosition, &fetchLowPositionLength, OCI_DEFAULT))
					== OCI_STILL_EXECUTING)
				{
					readLcrHeader(lcr);
					LOG(WARNING) << (const char*)m_lcrHeader.cmdType << "," << (const char*)m_lcrHeader.txid << "," << (const char*)m_lcrHeader.srcDbName << "," << (const char*)m_lcrHeader.position;
					if (lcrType == OCI_LCR_XDDL)
					{
						dsReturnIfFailedWithOp(readDDLInfo(lcr), OCILCRFree(m_currentConn->svcp, m_currentConn->errp, lcr, OCI_DEFAULT));
					}
					else if (lcrType == OCI_LCR_XROW)
					{
						DATABASE_INCREASE::RecordType recordType = getType(m_lcrHeader.cmdType, m_lcrHeader.cmdTypeLen);
						if (recordType == DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE)
						{
							LOG(ERROR) << "invalid recordType:" << std::string((const char*)m_lcrHeader.cmdType, m_lcrHeader.cmdTypeLen);
							OCILCRFree(m_currentConn->svcp, m_currentConn->errp, lcr, OCI_DEFAULT);
							continue;
						}
						readExtraAttribute(lcr);
						if (recordType == DATABASE_INCREASE::RecordType::R_INSERT || recordType == DATABASE_INCREASE::RecordType::R_UPDATE)
						{
							readColumnsValue(lcr, OCI_LCR_ROW_COLVAL_NEW, m_lcrHeader.newColumns);
						}
						if (recordType == DATABASE_INCREASE::RecordType::R_DELETE || recordType == DATABASE_INCREASE::RecordType::R_UPDATE)
						{
							readColumnsValue(lcr, OCI_LCR_ROW_COLVAL_OLD, m_lcrHeader.oldColumns);
						}
						if (flag & OCI_XSTREAM_MORE_ROW_DATA)
						{
							getChunks();
						}
						DATABASE_INCREASE::record* record = nullptr;
						lcrToRecord(recordType, record);
					}
					else
					{
						LOG(WARNING) << "unkown lcr type:" << lcrType;
					}
					/*
					if(!OCI_SUCCESS == OCILCRFree(m_currentConn->svcp, m_currentConn->errp, lcr, OCI_DEFAULT))
					{
						int32_t errcode;
						uint8_t errBuf[512] = { 0 };
						OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
						dsFailedAndLogIt(1, "call OCIXStreamOutLCRReceive failed for " << errcode << "," << (char*)errBuf, ERROR);
					}
					*/
				}
			}
			if (status != OCI_SUCCESS)
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				dsFailedAndLogIt(1, "call OCIXStreamOutLCRReceive failed for " << errcode << "," << (char*)errBuf, ERROR);
			}
			dsOk();
		}
		DS attach()
		{
			if (OCI_SUCCESS != OCIXStreamOutAttach(m_currentConn->svcp, m_currentConn->errp, (oratext*)m_xstreamOutBoundServer.c_str(), m_xstreamOutBoundServer.length(), (ub1*)0, 0, OCI_DEFAULT))
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << errcode << "," << (char*)errBuf, ERROR);
			}
			dsOk();
		}
	private:

		void getChunks()
		{
			m_chunkCount = 0;
			chunk* c = nullptr;
			do
			{
				if (c == nullptr)
				{
					m_chunkHead.reset();
					c = &m_chunkHead;
				}
				else
				{
					c->next = new chunk();
					c = c->next;
				}
				m_lastChunk = c;
				/* Get a chunk from outbound server */
				if (OCI_SUCCESS != OCIXStreamOutChunkReceive(m_currentConn->svcp, m_currentConn->errp,
					&c->columnName, &c->columnNameLen, &c->columnDataType,
					&c->columnFlag, &c->columnCharsetId, &c->chunkBytes,
					&c->chunkData, &c->flag, OCI_DEFAULT))
				{
					int32_t errcode;
					uint8_t errBuf[512] = { 0 };
					OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
					LOG(ERROR) << "call OCIXStreamOutChunkReceive failed for " << errcode << "," << (char*)errBuf;
				}
				m_chunkCount++;
			} while (c->flag & OCI_XSTREAM_MORE_ROW_DATA);
		}
		inline DS readLcrHeader(void* lcr)
		{
			if (OCI_SUCCESS != OCILCRHeaderGet(m_currentConn->svcp, m_currentConn->errp,
				&m_lcrHeader.srcDbName, &m_lcrHeader.srcDbNameLen,              /* source db */
				&m_lcrHeader.cmdType, &m_lcrHeader.cmdTypeLen,            /* command type */
				&m_lcrHeader.owner, &m_lcrHeader.ownerLen,                       /* owner name */
				&m_lcrHeader.oname, &m_lcrHeader.onameLen,                      /* object name */
				&m_lcrHeader.tag, &m_lcrHeader.tagLen,                      /* lcr tag */
				&m_lcrHeader.txid, &m_lcrHeader.txidLen, &m_lcrHeader.srcTime,   /* txn id  & src time */
				&m_lcrHeader.oldColumns, &m_lcrHeader.newColumns,              /* OLD/NEW col cnts */
				&m_lcrHeader.position, &m_lcrHeader.positionLen,                 /* LCR position */
				&m_lcrHeader.flag, lcr, OCI_DEFAULT))
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				LOG(ERROR) << "call OCILCRHeaderGet failed for " << errcode << "," << (char*)errBuf;
			}
			dsOk();
		}
		inline DS readExtraAttribute(void* lcr)
		{
			if (OCI_SUCCESS != OCILCRAttributesGet(m_currentConn->svcp, m_currentConn->errp,
				&m_attrs.attrsNumber, m_attrs.attrNames, m_attrs.attrNamesLens,
				m_attrs.attrDataTypes, m_attrs.attrValues, m_attrs.attrIndp, m_attrs.attrValueLength, lcr, MAX_ATTR_COUNT, OCI_DEFAULT))
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				LOG(ERROR) << "call OCILCRAttributesGet failed for " << errcode << "," << (char*)errBuf;
			}
			dsOk();
		}
		inline DS readColumnsValue(void* lcr, ub2  columnValueType, ub2 columnCount)
		{
			columnValuesInfo* column = columnValueType == OCI_LCR_ROW_COLVAL_NEW ? &m_newColumns : &m_oldColumns;
			if (OCI_SUCCESS != OCILCRRowColumnInfoGet(m_currentConn->svcp, m_currentConn->errp, columnValueType,
				&column->columnNumber, column->columnNames, column->columnNamelengths, column->columnDataTypes, column->columnValues,
				column->columnIndicators, column->columnValueLengths, column->columnCharsets, column->columnFlags, column->columnCharsetIds, lcr, MAX_COLUMN_COUNT, OCI_DEFAULT))
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				LOG(ERROR) << "call OCILCRRowColumnInfoGet failed for " << errcode << "," << (char*)errBuf;
			}
			dsOk();
		}
		inline DS readDDLInfo(void* lcr)
		{
			if (unlikely(OCI_SUCCESS != OCILCRDDLInfoGet(m_currentConn->svcp, m_currentConn->errp, &m_ddlInfo.objectType, &m_ddlInfo.objectTypeLength,
				&m_ddlInfo.ddlText, &m_ddlInfo.ddlTextLength, &m_ddlInfo.logonUser, &m_ddlInfo.logonUserLen, &m_ddlInfo.currentSchema, &m_ddlInfo.currentSchemaLen,
				&m_ddlInfo.baseTableOwner, &m_ddlInfo.baseTableOwnerLen, &m_ddlInfo.baseTableName, &m_ddlInfo.baseTableNameLen, &m_ddlInfo.flag, lcr, OCI_DEFAULT)))
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(m_currentConn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				LOG(ERROR) << "call OCILCRDDLInfoGet failed for " << errcode << "," << (char*)errBuf;
			}
			dsOk();
		}
		inline DATABASE_INCREASE::RecordType getType(oratext* cmdType, ub2 cmdTypeLength)
		{
			if (cmdTypeLength == 6)
			{
				if (memcmp(OCI_LCR_ROW_CMD_INSERT, cmdType, 6) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_INSERT;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_UPDATE, cmdType, 6) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_UPDATE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_DELETE, cmdType, 6) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_DELETE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_COMMIT, cmdType, 6) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_COMMIT;
				}
				else
				{
					return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 8)
			{
				if (memcmp(OCI_LCR_ROW_CMD_START_TX, cmdType, 8) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_BEGIN;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_ROLLBACK, cmdType, 8) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_ROLLBACK;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_TRIM, cmdType, 8) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_LOB_TRIM;
				}
				else
				{
					return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 9)
			{
				if (memcmp(OCI_LCR_ROW_CMD_LOB_WRITE, cmdType, 9) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_LOB_WRITE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_ERASE, cmdType, 9) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_LOB_ERASE;
				}
				else
				{
					return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 9)
			{
				if (memcmp(OCI_LCR_ROW_CMD_LOB_WRITE, cmdType, 9) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_LOB_WRITE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_ERASE, cmdType, 9) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_LOB_ERASE;
				}
				else
				{
					return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 12)
			{
				if (memcmp(OCI_LCR_ROW_CMD_CTRL_INFO, cmdType, 12) == 0)
				{
					return DATABASE_INCREASE::RecordType::R_CONTROL;
				}
				else
				{
					return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
				}
			}
			return DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE;
		}

		uint32_t allocSize(DATABASE_INCREASE::RecordType type)
		{
			uint32_t s = 0;
			if (type == DATABASE_INCREASE::RecordType::R_INSERT || type == DATABASE_INCREASE::RecordType::R_UPDATE)
			{
				for (int i = 0; i < m_newColumns.columnNumber; i++)
					s += m_newColumns.columnValueLengths[i];
			}
			if (type == DATABASE_INCREASE::RecordType::R_DELETE || type == DATABASE_INCREASE::RecordType::R_UPDATE)
			{
				for (int i = 0; i < m_oldColumns.columnNumber; i++)
					s += m_oldColumns.columnValueLengths[i];
			}
			return s;
		}

		uint64_t getTimestamp()
		{
			tm t;
			memset(&t, 0, sizeof(t));
			t.tm_year = m_lcrHeader.srcTime.OCIDateYYYY;
			t.tm_mon = m_lcrHeader.srcTime.OCIDateMM;
			t.tm_mday = m_lcrHeader.srcTime.OCIDateDD;
			t.tm_hour = m_lcrHeader.srcTime.OCIDateTime.OCITimeHH;
			t.tm_min = m_lcrHeader.srcTime.OCIDateTime.OCITimeMI;
			t.tm_sec = m_lcrHeader.srcTime.OCIDateTime.OCITimeSS;
			return mktime(&t);
		}

		DS lcrToRecord(DATABASE_INCREASE::RecordType type, DATABASE_INCREASE::record*& record)
		{
			switch (type)
			{
			case DATABASE_INCREASE::RecordType::R_INSERT:
			case DATABASE_INCREASE::RecordType::R_UPDATE:
			case DATABASE_INCREASE::RecordType::R_DELETE:
				META::tableMeta* meta = m_metaDataCollection->get((const char*)m_lcrHeader.owner, (const char*)m_lcrHeader.oname);
				if (meta == nullptr)
					dsFailedAndLogIt(1, "can not find table meta for table:" << (const char*)m_lcrHeader.owner << "." << (const char*)m_lcrHeader.oname, ERROR);
				DATABASE_INCREASE::DMLRecord* record = (DATABASE_INCREASE::DMLRecord*)m_buffer->alloc(sizeof(DATABASE_INCREASE::DMLRecord)
					+ DATABASE_INCREASE::recordHeadSize + (allocSize(type) * 4));
				record->initRecord(((char*)record) + sizeof(DATABASE_INCREASE::DMLRecord), meta, type);
				record->head->timestamp = getTimestamp();
				break;
			}
			dsOk();
		}
	};
}
