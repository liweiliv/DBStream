#pragma once
//although through simple configuration, you can use oracleXStreamLogReader to read data from Oracle XStream
//but Oracle XStream is not free for commercial purpose
//[Using the XStream APIs requires purchasing a license for the Oracle GoldenGate product] 
//[https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm]
// 
//more details of oci XStream funcs in https://docs.oracle.com/en/database/oracle/oracle-database/21/lnoci/oci-XStream-functions.html
#include <chrono>
#include <time.h>
#include <thread>
#include "glog/logging.h"
#include "message/record.h"
#include "meta/metaData.h"
#include "meta/metaDataCollection.h"
#include "memory/ringBuffer.h"
#include "dataSource/dataSource.h"
#include "dataSource/oracleDataSource/oracleIncreaceDataSource.h"
#include "dataSource/oracleDataSource/ociConnect.h"
#include "dataSource/oracleDataSource/oracleMetaDataCollection.h"
#include "util/arrayQueue.h"
#include "XStreamRecord.h"
namespace DATA_SOURCE
{
	constexpr static auto GET_DB_TIMEZONE_OFFSET = "SELECT TZ_OFFSET(DBTIMEZONE) FROM DUAL";
	class oracleXStreamLogReader :public oracleIncreaceDataSource
	{
	private:
		ociConnect m_connect;
		ociConnect::oci* m_currentConn;
		uint64_t m_startScn;
		std::string m_xstreamOutBoundServer;
		XStreamRecord* m_currentRecord;
		ringBuffer m_buffer;
		ringBuffer m_readerBuffer;
		int32_t m_dbTimezoneOffset;
		oracleMetaDataCollection* m_metaDataCollection;
		OCIDate m_prevDate;
		uint64_t m_prevTimestamp;
		arrayQueue<XStreamRecord*> m_innerQueue;
		arrayQueue<RPC::Record*> m_outputQueue;
		sword m_readStatus;
	public:
		oracleXStreamLogReader(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::store* store) :oracleIncreaceDataSource(conf, metaDataCollection, store), m_connect(conf), m_currentConn(nullptr),
			m_startScn(0),  m_metaDataCollection(nullptr), m_prevTimestamp(0),
			m_innerQueue(2048)
		{
			m_xstreamOutBoundServer = conf->get(SECTION, "outBoundServer", "XOUT4");
			memset(&m_prevDate, 0, sizeof(m_prevDate));
		}
		~oracleXStreamLogReader()
		{
			if (m_currentConn != nullptr)
				delete m_currentConn;
		}
		DS init()
		{
			dsReturnIfFailed(m_connect.init());
			dsReturnIfFailed(connect());
			dsOk();
		}

		DS start()
		{
			dsReturnIfFailed(attach());
			if (m_readerAndParserIndependent)
				m_readThreadIsRunning = true;
				m_readThread = std::thread([this]()-> DS {
				while (m_readThreadIsRunning)
				{
					if (!dsCheck(readXStreamRecord()))
					{
						m_readThreadIsRunning = false;
						m_readThreadErrStatus = getLocalStatus();
						dsReturn(m_readThreadErrStatus.code);
					}
				}
				dsOk();
			});

			if (m_asyncRead)
			{
				if (m_readerAndParserIndependent)
				{
					m_parserThreadIsRunning = true;
					m_parserThread = std::thread([this]()-> DS {
						while (m_parserThreadIsRunning)
						{
							RPC::Record* r = nullptr;
							if (!dsCheck(getRecordFormQueueAndParse(r)))
								goto FAILED;
							if (!dsCheck(pushIncreRecord(r)))
								dsOk();
						}
						dsOk();
					FAILED:
						m_parserThreadIsRunning = false;
						m_parserThreadErrStatus = getLocalStatus();
						dsReturn(m_parserThreadErrStatus.code);
					});
				}
				else
				{
					m_parserThreadIsRunning = true;
					m_parserThread = std::thread([this]()-> DS {
						while (m_parserThreadIsRunning)
						{
							RPC::Record* r = nullptr;
							if (!dsCheck(readRecordAndParse(r)))
								goto FAILED;
							if (!dsCheck(pushIncreRecord(r)))
								dsOk();
						}
						dsOk();
					FAILED:
						m_parserThreadIsRunning = false;
						m_parserThreadErrStatus = getLocalStatus();
						dsReturn(m_parserThreadErrStatus.code);
					});
				}
			}
		}

		DS stop()
		{
			m_readThreadIsRunning = false;
			m_parserThreadIsRunning = false;
			if (m_readerAndParserIndependent)
			{
				m_readThread.join();
			}
			if (m_asyncRead)
			{
				m_parserThread.join();
			}
			dsOk();
		}
	private:

		DS connect()
		{
			dsReturnIfFailed(m_connect.connect(m_currentConn));
			dsReturnIfFailed(updateTimezoneInfo());
			dsOk();
		}
		DS updateTimezoneInfo()
		{
			dsReturnIfFailed(m_currentConn->openStmt());
			if (OCIStmtPrepare(m_currentConn->stmtp, m_currentConn->errp, (const text*)GET_DB_TIMEZONE_OFFSET, (ub4)strlen(GET_DB_TIMEZONE_OFFSET), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT) != OCI_SUCCESS)
				dsFailedAndLogIt(1, "call OCIStmtPrepare failed for " << m_currentConn->getErrorStr(), ERROR);
			OCIDefine* defn = nullptr;
			char timezoneOffset[10] = { 0 };
			OCIDefineByPos(m_currentConn->stmtp, &defn, m_currentConn->errp, (ub4)1, timezoneOffset, sizeof(timezoneOffset), SQLT_STR, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);
			if (OCIStmtExecute(m_currentConn->svcp, m_currentConn->stmtp, m_currentConn->errp, (ub4)1, (ub4)0, (OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT) != OCI_SUCCESS)
				dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << m_currentConn->getErrorStr(), ERROR);
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

		inline DS pushCurrentRecord()
		{
			do {
				if (m_innerQueue.pushWithCond(m_currentRecord, 100))
				{
					m_currentRecord = nullptr;
					dsOk();
				}
			} while (m_parserThreadIsRunning);
			dsFailedAndLogIt(1, "oracleXStreamLogReader has stopped", ERROR);
		}

		inline DS pushIncreRecord(RPC::Record* r)
		{
			do {
				if (m_outputQueue.pushWithCond(r, 100))
					dsOk();
			} while (m_parserThreadIsRunning);
			dsFailedAndLogIt(1, "oracleXStreamLogReader has stopped", ERROR);
		}

		DS readXStreamRecord()
		{
			ub1         lcrType;
			oraub8      flag;
			ub1         fetchLowPosition[OCI_LCR_MAX_POSITION_LEN];
			ub2         fetchLowPositionLength = 0;
			if (m_currentRecord == nullptr && m_readerAndParserIndependent)
				m_currentRecord = (XStreamRecord*)m_readerBuffer.alloc(sizeof(m_currentRecord));
			do {
				m_readStatus = OCIXStreamOutLCRReceive(m_currentConn->svcp, m_currentConn->errp,
					&m_currentRecord->lcr, &lcrType, &flag,
					fetchLowPosition, &fetchLowPositionLength, OCI_DEFAULT);
				if (m_readStatus == OCI_SUCCESS)
					continue;
				if (m_readStatus != OCI_STILL_EXECUTING)//todo
				{
					int code;
					std::string err;
					m_currentConn->getError(code, err);
					if (oracleConnectBase::errorRecoverable(code))
					{
						delete m_currentConn;
						dsReturnIfFailed(connect());
						dsReturnIfFailed(attach());
						continue;
					}
					dsFailedAndLogIt(1, "call OCIXStreamOutLCRReceive failed for " << m_currentConn->getErrorStr(), ERROR);
				}

				dsReturnIfFailed(readLcrHeader(m_currentRecord->lcr));
				LOG(WARNING) << (const char*)m_currentRecord->m_lcrHeader.cmdType << "," << (const char*)m_currentRecord->m_lcrHeader.txid << "," << (const char*)m_currentRecord->m_lcrHeader.srcDbName << "," << (const char*)m_currentRecord->m_lcrHeader.position;
				if (lcrType == OCI_LCR_XDDL)
				{
					if(!dsCheck(readDDLInfo(m_currentRecord->lcr)))
						goto FAILED;
					m_currentRecord->recordType = RPC::RecordType::R_DDL;
					if (m_readerAndParserIndependent)
						dsReturn(pushCurrentRecord());
					dsOk();
				}
				else if (lcrType == OCI_LCR_XROW)
				{
					m_currentRecord->recordType = getType(m_currentRecord->m_lcrHeader.cmdType, m_currentRecord->m_lcrHeader.cmdTypeLen);
					if (m_currentRecord->recordType == RPC::RecordType::MAX_RECORD_TYPE)
					{
						if (m_readerAndParserIndependent)
							OCILCRFree(m_currentConn->svcp, m_currentConn->errp, m_currentRecord->lcr, OCI_DEFAULT);
						dsFailedAndLogIt(1, "invalid recordType:" << std::string((const char*)m_currentRecord->m_lcrHeader.cmdType, m_currentRecord->m_lcrHeader.cmdTypeLen), ERROR);
					}

					if (!dsCheck(readExtraAttribute(m_currentRecord->lcr)))
						goto FAILED;

					if (m_currentRecord->recordType == RPC::RecordType::R_INSERT || m_currentRecord->recordType == RPC::RecordType::R_UPDATE)
					{
						if (!dsCheck(readColumnsValue(m_currentRecord->lcr, OCI_LCR_ROW_COLVAL_NEW, m_currentRecord->m_lcrHeader.newColumns)))
							goto FAILED;
					}

					if (m_currentRecord->recordType == RPC::RecordType::R_DELETE || m_currentRecord->recordType == RPC::RecordType::R_UPDATE)
					{
						if (!dsCheck(readColumnsValue(m_currentRecord->lcr, OCI_LCR_ROW_COLVAL_OLD, m_currentRecord->m_lcrHeader.oldColumns)))
							goto FAILED;
					}
					if (flag & OCI_XSTREAM_MORE_ROW_DATA)
					{
						if (!dsCheck(getChunks()))
							goto FAILED;
					}
					if (m_readerAndParserIndependent)
						dsReturn(pushCurrentRecord());
					dsOk();
				}
				else
				{
					LOG(WARNING) << "unkown lcr type:" << lcrType;
					if (m_readerAndParserIndependent)
						OCILCRFree(m_currentConn->svcp, m_currentConn->errp, m_currentRecord->lcr, OCI_DEFAULT);
				}
			} while (m_readThreadIsRunning);
		FAILED:
			if (m_readerAndParserIndependent && m_currentRecord != nullptr && m_currentRecord->lcr != nullptr)
				OCILCRFree(m_currentConn->svcp, m_currentConn->errp, m_currentRecord->lcr, OCI_DEFAULT);
			dsReturn(getLocalStatus().code);
		}
		DS attach()
		{
			if (OCI_SUCCESS != OCIXStreamOutAttach(m_currentConn->svcp, m_currentConn->errp, (oratext*)m_xstreamOutBoundServer.c_str(), m_xstreamOutBoundServer.length(),
				(ub1*)0, 0, m_readerAndParserIndependent ? OCIXSTREAM_OUT_ATTACH_APP_FREE_LCR : OCI_DEFAULT))
				dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << m_currentConn->getErrorStr(), ERROR);
			dsOk();
		}
	private:

		DS getChunks()
		{
			m_currentRecord->m_chunkCount = 0;
			chunk* c = nullptr;
			do
			{
				if (c == nullptr)
				{
					m_currentRecord->m_chunkHead.reset();
					c = &m_currentRecord->m_chunkHead;
				}
				else
				{
					c->next = new chunk();
					c = c->next;
				}
				m_currentRecord->m_lastChunk = c;
				/* Get a chunk from outbound server */
				if (OCI_SUCCESS != OCIXStreamOutChunkReceive(m_currentConn->svcp, m_currentConn->errp,
					&c->columnName, &c->columnNameLen, &c->columnDataType,
					&c->columnFlag, &c->columnCharsetId, &c->chunkBytes,
					&c->chunkData, &c->flag, OCI_DEFAULT))
				{
					dsFailedAndLogIt(1, "call OCIXStreamOutChunkReceive failed for " << m_currentConn->getErrorStr(), ERROR);
				}
				m_currentRecord->m_chunkCount++;
			} while (c->flag & OCI_XSTREAM_MORE_ROW_DATA);
			dsOk();
		}
		inline DS readLcrHeader(void* lcr)
		{
			lcrHeader& lcrHeader = m_currentRecord->m_lcrHeader;
			if (OCI_SUCCESS != OCILCRHeaderGet(m_currentConn->svcp, m_currentConn->errp,
				&lcrHeader.srcDbName, &lcrHeader.srcDbNameLen,              /* source db */
				&lcrHeader.cmdType, &lcrHeader.cmdTypeLen,            /* command type */
				&lcrHeader.owner, &lcrHeader.ownerLen,                       /* owner name */
				&lcrHeader.oname, &lcrHeader.onameLen,                      /* object name */
				&lcrHeader.tag, &lcrHeader.tagLen,                      /* lcr tag */
				&lcrHeader.txid, &lcrHeader.txidLen, &lcrHeader.srcTime,   /* txn id  & src time */
				&lcrHeader.oldColumns, &lcrHeader.newColumns,              /* OLD/NEW col cnts */
				&lcrHeader.position, &lcrHeader.positionLen,                 /* LCR position */
				&lcrHeader.flag, lcr, OCI_DEFAULT))
			{
				dsFailedAndLogIt(1, "call OCILCRHeaderGet failed for " << m_currentConn->getErrorStr(), ERROR);
			}
			dsOk();
		}
		inline DS readExtraAttribute(void* lcr)
		{
			lcrAttributes& attrs = m_currentRecord->m_attrs;
			if (OCI_SUCCESS != OCILCRAttributesGet(m_currentConn->svcp, m_currentConn->errp,
				&attrs.attrsNumber, attrs.attrNames, attrs.attrNamesLens,
				attrs.attrDataTypes, attrs.attrValues, attrs.attrIndp, attrs.attrValueLength, lcr, MAX_ATTR_COUNT, OCI_DEFAULT))
			{
				dsFailedAndLogIt(1, "call OCILCRAttributesGet failed for " << m_currentConn->getErrorStr(), ERROR);
			}
			dsOk();
		}
		inline DS readColumnsValue(void* lcr, ub2  columnValueType, ub2 columnCount)
		{
			columnValuesInfo& column = columnValueType == OCI_LCR_ROW_COLVAL_NEW ? m_currentRecord->m_newColumns : m_currentRecord->m_oldColumns;
			if (OCI_SUCCESS != OCILCRRowColumnInfoGet(m_currentConn->svcp, m_currentConn->errp, columnValueType,
				&column.columnNumber, column.columnNames, column.columnNamelengths, column.columnDataTypes, column.columnValues,
				column.columnIndicators, column.columnValueLengths, column.columnCharsets, column.columnFlags, column.columnCharsetIds, lcr, MAX_COLUMN_COUNT, OCI_DEFAULT))
			{
				dsFailedAndLogIt(1, "call OCILCRRowColumnInfoGet failed for " << m_currentConn->getErrorStr(), ERROR);
			}
			dsOk();
		}
		inline DS readDDLInfo(void* lcr)
		{
			ddlInfo& ddl = m_currentRecord->m_ddlInfo;
			if (unlikely(OCI_SUCCESS != OCILCRDDLInfoGet(m_currentConn->svcp, m_currentConn->errp, &ddl.objectType, &ddl.objectTypeLength,
				&ddl.ddlText, &ddl.ddlTextLength, &ddl.logonUser, &ddl.logonUserLen, &ddl.currentSchema, &ddl.currentSchemaLen,
				&ddl.baseTableOwner, &ddl.baseTableOwnerLen, &ddl.baseTableName, &ddl.baseTableNameLen, &ddl.flag, lcr, OCI_DEFAULT)))
			{
				dsFailedAndLogIt(1, "call OCILCRDDLInfoGet failed for " << m_currentConn->getErrorStr(), ERROR);
			}
			dsOk();
		}
		inline RPC::RecordType getType(oratext* cmdType, ub2 cmdTypeLength)
		{
			if (cmdTypeLength == 6)
			{
				if (memcmp(OCI_LCR_ROW_CMD_INSERT, cmdType, 6) == 0)
				{
					return RPC::RecordType::R_INSERT;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_UPDATE, cmdType, 6) == 0)
				{
					return RPC::RecordType::R_UPDATE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_DELETE, cmdType, 6) == 0)
				{
					return RPC::RecordType::R_DELETE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_COMMIT, cmdType, 6) == 0)
				{
					return RPC::RecordType::R_COMMIT;
				}
				else
				{
					return RPC::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 8)
			{
				if (memcmp(OCI_LCR_ROW_CMD_START_TX, cmdType, 8) == 0)
				{
					return RPC::RecordType::R_BEGIN;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_ROLLBACK, cmdType, 8) == 0)
				{
					return RPC::RecordType::R_ROLLBACK;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_TRIM, cmdType, 8) == 0)
				{
					return RPC::RecordType::R_LOB_TRIM;
				}
				else
				{
					return RPC::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 9)
			{
				if (memcmp(OCI_LCR_ROW_CMD_LOB_WRITE, cmdType, 9) == 0)
				{
					return RPC::RecordType::R_LOB_WRITE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_ERASE, cmdType, 9) == 0)
				{
					return RPC::RecordType::R_LOB_ERASE;
				}
				else
				{
					return RPC::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 9)
			{
				if (memcmp(OCI_LCR_ROW_CMD_LOB_WRITE, cmdType, 9) == 0)
				{
					return RPC::RecordType::R_LOB_WRITE;
				}
				else if (memcmp(OCI_LCR_ROW_CMD_LOB_ERASE, cmdType, 9) == 0)
				{
					return RPC::RecordType::R_LOB_ERASE;
				}
				else
				{
					return RPC::RecordType::MAX_RECORD_TYPE;
				}
			}
			else if (cmdTypeLength == 12)
			{
				if (memcmp(OCI_LCR_ROW_CMD_CTRL_INFO, cmdType, 12) == 0)
				{
					return RPC::RecordType::R_CONTROL;
				}
				else
				{
					return RPC::RecordType::MAX_RECORD_TYPE;
				}
			}
			return RPC::RecordType::MAX_RECORD_TYPE;
		}

		uint32_t allocSize(XStreamRecord* xr, RPC::RecordType type)
		{
			uint32_t s = 0;
			if (type == RPC::RecordType::R_INSERT || type == RPC::RecordType::R_UPDATE)
			{
				for (int i = 0; i < xr->m_newColumns.columnNumber; i++)
					s += xr->m_newColumns.columnValueLengths[i];
			}
			if (type == RPC::RecordType::R_DELETE || type == RPC::RecordType::R_UPDATE)
			{
				for (int i = 0; i < xr->m_oldColumns.columnNumber; i++)
					s += xr->m_oldColumns.columnValueLengths[i];
			}
			return s;
		}

		uint64_t getTimestamp(OCIDate& srcTime)
		{

			if (*(uint32_t*)&m_prevDate == *(uint32_t*)&srcTime && m_prevDate.OCIDateTime.OCITimeHH == srcTime.OCIDateTime.OCITimeHH)
			{
				return m_prevTimestamp + (srcTime.OCIDateTime.OCITimeMI * 60 - srcTime.OCIDateTime.OCITimeSS - m_prevDate.OCIDateTime.OCITimeMI * 60 - m_prevDate.OCIDateTime.OCITimeSS) * 1000;
			}
			else
			{
				tm t;
				memset(&t, 0, sizeof(t));
				t.tm_year = srcTime.OCIDateYYYY;
				t.tm_mon = srcTime.OCIDateMM;
				t.tm_mday = srcTime.OCIDateDD;
				t.tm_hour = srcTime.OCIDateTime.OCITimeHH;
				t.tm_min = srcTime.OCIDateTime.OCITimeMI;
				t.tm_sec = srcTime.OCIDateTime.OCITimeSS;
				memcpy(&m_prevDate, &srcTime, sizeof(m_prevDate));
				return m_prevTimestamp = mktime(&t) * 1000;
			}
		}

		inline DS setColumnValue(RPC::DMLRecord* record, const META::ColumnMeta* col, void* data, uint32_t dataLength, ORACLE_COLUMN_TYPE type, bool isUpdateOldValue)
		{

			if (META::columnInfos[static_cast<int>(col->m_columnType)].fixed)
			{
				if (data == nullptr)
				{
					isUpdateOldValue ? record->setFixedColumnNull(col->m_columnIndex) : record->setUpdatedFixedColumnNull(col->m_columnIndex);
				}
				else
				{
					switch (type)
					{
					case ORACLE_COLUMN_TYPE::number:
					{
						OCINumber number;
						if (col->m_columnType == META::COLUMN_TYPE::T_INT64)
						{
							int64_t v = 0;
							if (OCI_SUCCESS != OCINumberToInt(m_currentConn->errp, (OCINumber*)data, 8, OCI_NUMBER_SIGNED, &v))
								dsFailedAndLogIt(1, "call OCINumberToInt failed for " << m_currentConn->getErrorStr(), ERROR);
							isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, v) : record->setFixedColumn(col->m_columnIndex, v);
						}
						else if (col->m_columnType == META::COLUMN_TYPE::T_INT32)
						{
							int32_t v = 0;
							if (OCI_SUCCESS != OCINumberToInt(m_currentConn->errp, (OCINumber*)data, 4, OCI_NUMBER_SIGNED, &v))
								dsFailedAndLogIt(1, "call OCINumberToInt failed for " << m_currentConn->getErrorStr(), ERROR);
							isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, v) : record->setFixedColumn(col->m_columnIndex, v);
						}
						else if (col->m_columnType == META::COLUMN_TYPE::T_INT16)
						{
							int16_t v = 0;
							if (OCI_SUCCESS != OCINumberToInt(m_currentConn->errp, (OCINumber*)data, 2, OCI_NUMBER_SIGNED, &v))
								dsFailedAndLogIt(1, "call OCINumberToInt failed for " << m_currentConn->getErrorStr(), ERROR);
							isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, v) : record->setFixedColumn(col->m_columnIndex, v);
						}
						else if (col->m_columnType == META::COLUMN_TYPE::T_INT8)
						{
							int8_t v = 0;
							if (OCI_SUCCESS != OCINumberToInt(m_currentConn->errp, (OCINumber*)data, 1, OCI_NUMBER_SIGNED, &v))
								dsFailedAndLogIt(1, "call OCINumberToInt failed for " << m_currentConn->getErrorStr(), ERROR);
							isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, v) : record->setFixedColumn(col->m_columnIndex, v);
						}
						else
						{
							dsFailedAndLogIt(1, "unsupport oracle number trans to " << static_cast<int>(type), ERROR);
						}
					}
					break;
					case ORACLE_COLUMN_TYPE::date:
					{
						META::DateTime d;
						OCIDate* src = (OCIDate*)data;
						d.createDate(src->OCIDateYYYY, src->OCIDateMM, src->OCIDateDD, src->OCIDateTime.OCITimeHH, src->OCIDateTime.OCITimeMI, src->OCIDateTime.OCITimeSS, 0);
						isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, d.time) : record->setFixedColumn(col->m_columnIndex, d.time);
					}
					break;
					case ORACLE_COLUMN_TYPE::timestamp:
					{
						META::DateTime d;
						OCIDate* src = (OCIDate*)data;
						d.createDate(src->OCIDateYYYY, src->OCIDateMM, src->OCIDateDD, src->OCIDateTime.OCITimeHH, src->OCIDateTime.OCITimeMI, src->OCIDateTime.OCITimeSS, 0);
						isUpdateOldValue ? record->setFixedUpdatedColumn(col->m_columnIndex, d.time) : record->setFixedColumn(col->m_columnIndex, d.time);
					}
					break;
					default:
						dsFailedAndLogIt(1, "unsupport column  " << record->meta->m_dbName << "." << record->meta->m_tableName << "." << col->m_columnName << ",oracle data type:" << static_cast<int>(type), ERROR);
					}
				}
			}
			else
			{
				if (data == nullptr)
				{
					isUpdateOldValue ? record->setUpdatedVarColumnNull(col->m_columnIndex) : record->setVarColumnNull(col->m_columnIndex);
				}
				else
				{
					switch (type)
					{
					case ORACLE_COLUMN_TYPE::number:
						dsFailedAndLogIt(1, "unsupport oracle number trans to " << static_cast<int>(col->m_columnType), ERROR);//now not support big number
					case ORACLE_COLUMN_TYPE::Char:
					case ORACLE_COLUMN_TYPE::varchar:
					case ORACLE_COLUMN_TYPE::varchar2:
					case ORACLE_COLUMN_TYPE::clob:
					case ORACLE_COLUMN_TYPE::blob:
					case ORACLE_COLUMN_TYPE::Long:
					case ORACLE_COLUMN_TYPE::longRaw:
					case ORACLE_COLUMN_TYPE::rowId:
						isUpdateOldValue ? record->setVardUpdatedColumn(col->m_columnIndex, (const char*)data, dataLength) :
							record->setVarColumn(col->m_columnIndex, (const char*)data, dataLength);
						break;
					default:
						dsFailedAndLogIt(1, "unsupport column  " << record->meta->m_dbName << "." << record->meta->m_tableName << "." << col->m_columnName << ",oracle data type:" << static_cast<int>(type), ERROR);
					}
				}
			}
			dsOk();
		}

		inline const META::ColumnMeta* getColumn(META::TableMeta* meta, char* name, ub2 length)
		{
			char c = name[length];
			name[length] = '\0';
			const META::ColumnMeta* col = meta->getColumn(name);
			if (col == nullptr)
				return nullptr;
			name[length] = c;
			return col;
		}

		inline DS parseDml(XStreamRecord* xr, RPC::Record*& r)
		{
			META::TableMeta* meta = m_metaDataCollection->get((const char*)xr->m_lcrHeader.owner, (const char*)xr->m_lcrHeader.oname);
			if (meta == nullptr)
				dsFailedAndLogIt(1, "can not find table meta for table:" << (const char*)xr->m_lcrHeader.owner << "." << (const char*)xr->m_lcrHeader.oname, ERROR);
			RPC::DMLRecord* record = (RPC::DMLRecord*)m_buffer.alloc(sizeof(RPC::DMLRecord)
				+ RPC::recordHeadSize + (allocSize(xr, xr->recordType) * 4));
			record->initRecord(((char*)record) + sizeof(RPC::DMLRecord), meta, xr->recordType);
			record->head->timestamp = getTimestamp(xr->m_lcrHeader.srcTime);

			if (xr->recordType == RPC::RecordType::R_INSERT || xr->recordType == RPC::RecordType::R_UPDATE)//insert and update
			{
				for (int i = 0; i < xr->m_newColumns.columnNumber; i++)
				{
					const META::ColumnMeta* col = getColumn(meta, (char*)xr->m_newColumns.columnNames[i], xr->m_newColumns.columnNamelengths[i]);
					if (col == nullptr)
						dsFailedAndLogIt(1, "can not find column " << (const char*)xr->m_newColumns.columnNames[i] << " in table:" << (const char*)xr->m_lcrHeader.owner << "." << (const char*)xr->m_lcrHeader.oname, ERROR);
					dsReturnIfFailed(setColumnValue(record, col, xr->m_newColumns.columnValues[i], xr->m_newColumns.columnValueLengths[i], static_cast<ORACLE_COLUMN_TYPE>(xr->m_newColumns.columnDataTypes[i]), false));
				}
			}
			else //delete
			{
				for (int i = 0; i < xr->m_oldColumns.columnNumber; i++)
				{
					const META::ColumnMeta* col = getColumn(meta, (char*)xr->m_oldColumns.columnNames[i], xr->m_oldColumns.columnNamelengths[i]);
					if (col == nullptr)
						dsFailedAndLogIt(1, "can not find column " << (const char*)xr->m_oldColumns.columnNames[i] << " in table:" << (const char*)xr->m_lcrHeader.owner << "." << (const char*)xr->m_lcrHeader.oname, ERROR);
					dsReturnIfFailed(setColumnValue(record, col, xr->m_oldColumns.columnValues[i], xr->m_oldColumns.columnValueLengths[i], static_cast<ORACLE_COLUMN_TYPE>(xr->m_oldColumns.columnDataTypes[i]), false));
				}
			}

			if (xr->recordType == RPC::RecordType::R_UPDATE)
			{
				for (int i = 0; i < xr->m_oldColumns.columnNumber; i++)
				{
					const META::ColumnMeta* col = getColumn(meta, (char*)xr->m_oldColumns.columnNames[i], xr->m_oldColumns.columnNamelengths[i]);
					if (col == nullptr)
						dsFailedAndLogIt(1, "can not find column " << (const char*)xr->m_oldColumns.columnNames[i] << " in table:" << (const char*)xr->m_lcrHeader.owner << "." << (const char*)xr->m_lcrHeader.oname, ERROR);
					dsReturnIfFailed(setColumnValue(record, col, xr->m_oldColumns.columnValues[i], xr->m_oldColumns.columnValueLengths[i], static_cast<ORACLE_COLUMN_TYPE>(xr->m_oldColumns.columnDataTypes[i]), true));
				}
			}
			r = record;
			dsOk();
		}

		DS lcrToRecord(XStreamRecord* xr, RPC::Record*& record)
		{
			switch (xr->recordType)
			{
			case RPC::RecordType::R_INSERT:
			case RPC::RecordType::R_UPDATE:
			case RPC::RecordType::R_DELETE:
				dsReturn(parseDml(xr, record));
			}
			dsOk();
		}

		dsStatus* getFailedCause()
		{
			if (m_readThreadErrStatus.code != 0)
				return &m_readThreadErrStatus;
			if (m_parserThreadErrStatus.code != 0)
				return &m_parserThreadErrStatus;
			return nullptr;
		}
		
		inline DS getRecordFormQueueAndParse(RPC::Record*& r)
		{
			XStreamRecord* xr = nullptr;
			do {
				if (m_innerQueue.popWithCond(xr, 100))
				{
					dsReturnIfFailedWithOp(lcrToRecord(xr, r), OCILCRFree(m_currentConn->svcp, m_currentConn->errp, xr->lcr, OCI_DEFAULT));
					m_readerBuffer.freeMem(xr);
					if (r != nullptr)
						dsOk();
				}
			} while (m_readThreadIsRunning);
			dsFailedAndLogItWithCause(1, "oracleXStreamLogReader has stopped", ERROR, getFailedCause());
		}

		inline DS readRecordAndParse(RPC::Record*& r)
		{
			do {
				dsReturnIfFailed(readXStreamRecord());
				dsReturnIfFailed(lcrToRecord(m_currentRecord, r));
				if (r != nullptr)
					dsOk();
			} while (m_readThreadIsRunning && m_parserThreadIsRunning);
			dsFailedAndLogItWithCause(1, "oracleXStreamLogReader has stopped", ERROR, getFailedCause());
		}

		inline DS parse(RPC::Record*& r)
		{
			if (m_readerAndParserIndependent)
			{
				dsReturn(getRecordFormQueueAndParse(r));
			}
			else
			{
				dsReturn(readRecordAndParse(r));
			}
		}

	public:
		DS read(RPC::Record *& r)
		{
			r = nullptr;
			if (m_asyncRead)
			{
				do {
					if (m_outputQueue.popWithCond(r, 100))
						dsOk();
				} while (m_parserThreadIsRunning);
				dsFailedAndLogItWithCause(1, "oracleXStreamLogReader has stopped", ERROR, getFailedCause());
			}
			else
			{
				dsReturn(parse(r));
			}
		}
	};
}
