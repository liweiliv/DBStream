/*
 * BinlogEventParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "columnParser.h"
#include "BinlogEventParser.h"
#include "mysqlRecordOffset.h"
#include "glog/logging.h"
#include "util/file.h"
#include "util/winString.h"
#include "sqlParser/sqlParser.h"
#define ROWS_MAPID_OFFSET    0
#define ROWS_FLAGS_OFFSET    6
#define ROWS_VHLEN_OFFSET    8
#define ROWS_V_TAG_LEN       1
#define ROWS_V_EXTRAINFO_TAG 0
namespace DATA_SOURCE {
#define INDEX_OF_BYTE_STORE_DATA_LEN 9
#define INDEX_OF_BYTE_STORE_NUM_OF_COLUMNS_5_1_48 27
#define INDEX_OF_FIRST_COLUMN_BYTE INDEX_OF_BYTE_STORE_NUM_OF_COLUMNS_5_1_48 + 1
#define BYTES_FOR_BITS(b) ((int)(b) + 7) / 8
#define NULL_BIT(p, n) ((UCHAR((p) + ((n) >> 3))) & (1 << ((n) & 0x0007)))
#define UCHAR(ptr) (*(unsigned char*)(ptr))
#define BYTES_FOR_LENGTH(l) (((uint)(l) > UINT_MAX8)?2:1)
#define WRITE_NEW 0
#define DELETE_OLD 0
#define UPDATE_OLD 0
#define UPDATE_NEW 1
#define QUERY_HEADER_MINIMAL_LEN (4 + 4 + 1 + 2)
#define DDL_SQL_MODE_HEAD_STR "SET @@session.sql_mode="
#define DDL_SQL_TIMESTAMP_HEAD_STR "SET TIMESTAMP="
#define DDL_SQL_NAME_HEAD_STR "SET names "
#define DEFAULT_ENCODING  "UTF8"

	void BinlogEventParser::initColumnTypeInfo()
	{
		memset(m_columnInfo, 0, sizeof(m_columnInfo));
		m_columnInfo[MYSQL_TYPE_JSON].parseFunc = parse_MYSQL_TYPE_JSON;
		m_columnInfo[MYSQL_TYPE_BIT].parseFunc = parse_MYSQL_TYPE_BIT;
		m_columnInfo[MYSQL_TYPE_TINY].parseFunc = parse_MYSQL_TYPE_TINY;
		m_columnInfo[MYSQL_TYPE_SHORT].parseFunc = parse_MYSQL_TYPE_SHORT;
		m_columnInfo[MYSQL_TYPE_INT24].parseFunc = parse_MYSQL_TYPE_INT24;
		m_columnInfo[MYSQL_TYPE_LONG].parseFunc = parse_MYSQL_TYPE_LONG;
		m_columnInfo[MYSQL_TYPE_LONGLONG].parseFunc = parse_MYSQL_TYPE_LONGLONG;
		m_columnInfo[MYSQL_TYPE_FLOAT].parseFunc = parse_MYSQL_TYPE_FLOAT;
		m_columnInfo[MYSQL_TYPE_DOUBLE].parseFunc = parse_MYSQL_TYPE_DOUBLE;
		m_columnInfo[MYSQL_TYPE_NEWDECIMAL].parseFunc = parse_MYSQL_TYPE_NEWDECIMAL;
		m_columnInfo[MYSQL_TYPE_TIMESTAMP].parseFunc = parse_MYSQL_TYPE_TIMESTAMP;
		m_columnInfo[MYSQL_TYPE_TIMESTAMP2].parseFunc = parse_MYSQL_TYPE_TIMESTAMP2;
		m_columnInfo[MYSQL_TYPE_DATETIME2].parseFunc = parse_MYSQL_TYPE_DATETIME2;
		m_columnInfo[MYSQL_TYPE_DATETIME].parseFunc = parse_MYSQL_TYPE_DATETIME;
		m_columnInfo[MYSQL_TYPE_TIME2].parseFunc = parse_MYSQL_TYPE_TIME2;
		m_columnInfo[MYSQL_TYPE_YEAR].parseFunc = parse_MYSQL_TYPE_YEAR;
		m_columnInfo[MYSQL_TYPE_NEWDATE].parseFunc = parse_MYSQL_TYPE_NEWDATE;
		m_columnInfo[MYSQL_TYPE_STRING].parseFunc = parse_MYSQL_TYPE_STRING;
		m_columnInfo[MYSQL_TYPE_VARCHAR].parseFunc =
			m_columnInfo[MYSQL_TYPE_VAR_STRING].parseFunc = parse_MYSQL_TYPE_VAR_STRING;
		m_columnInfo[MYSQL_TYPE_TINY_BLOB].parseFunc =
			m_columnInfo[MYSQL_TYPE_MEDIUM_BLOB].parseFunc =
			m_columnInfo[MYSQL_TYPE_LONG_BLOB].parseFunc =
			m_columnInfo[MYSQL_TYPE_BLOB].parseFunc = parse_MYSQL_TYPE_BLOB;

		m_columnInfo[MYSQL_TYPE_ENUM].parseFunc = parse_MYSQL_TYPE_ENUM;
		m_columnInfo[MYSQL_TYPE_SET].parseFunc = parse_MYSQL_TYPE_SET;
		m_columnInfo[MYSQL_TYPE_GEOMETRY].parseFunc = parse_MYSQL_TYPE_GEOMETRY;

		m_columnInfo[MYSQL_TYPE_TINY_BLOB].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_BLOB].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_MEDIUM_BLOB].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_LONG_BLOB].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_DOUBLE].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_FLOAT].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_GEOMETRY].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_JSON].metaLength = 1;

		m_columnInfo[MYSQL_TYPE_SET].metaLength = 2;
		m_columnInfo[MYSQL_TYPE_ENUM].metaLength = 2;
		m_columnInfo[MYSQL_TYPE_STRING].metaLength = 2;
		m_columnInfo[MYSQL_TYPE_BIT].metaLength = 2;
		m_columnInfo[MYSQL_TYPE_VARCHAR].metaLength = 2;
		m_columnInfo[MYSQL_TYPE_NEWDECIMAL].metaLength = 2;

		m_columnInfo[MYSQL_TYPE_TIME2].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_DATETIME2].metaLength = 1;
		m_columnInfo[MYSQL_TYPE_TIMESTAMP2].metaLength = 1;
#ifdef COMPARE_UPDATE
		memset(m_fixedTypeSize, 0, sizeof(m_fixedTypeSize));
		m_fixedTypeSize[MYSQL_TYPE_TINY] = 1;
		m_fixedTypeSize[MYSQL_TYPE_SHORT] = 2;
		m_fixedTypeSize[MYSQL_TYPE_INT24] = 3;
		m_fixedTypeSize[MYSQL_TYPE_LONG] = 4;
		m_fixedTypeSize[MYSQL_TYPE_LONGLONG] = 8;
		m_fixedTypeSize[MYSQL_TYPE_FLOAT] = 4;
		m_fixedTypeSize[MYSQL_TYPE_DOUBLE] = 8;
		m_fixedTypeSize[MYSQL_TYPE_TIMESTAMP] = 4;
		m_fixedTypeSize[MYSQL_TYPE_DATETIME] = 8;
		m_fixedTypeSize[MYSQL_TYPE_YEAR] = 1;
		m_fixedTypeSize[MYSQL_TYPE_NEWDATE] = 3;

		/*
		m_columnInfo[MYSQL_TYPE_NEWDECIMAL].lengthFunc = lengthOf_MYSQL_TYPE_NEWDECIMAL;
		m_columnInfo[MYSQL_TYPE_TIMESTAMP2].lengthFunc = lengthOf_MYSQL_TYPE_TIMESTAMP2;
		m_columnInfo[MYSQL_TYPE_DATETIME2].lengthFunc = lengthOf_MYSQL_TYPE_DATETIME2;
		m_columnInfo[MYSQL_TYPE_TIME2].lengthFunc = lengthOf_MYSQL_TYPE_TIME2;
		m_columnInfo[MYSQL_TYPE_BIT].lengthFunc = lengthOf_MYSQL_TYPE_BIT;
		m_columnInfo[MYSQL_TYPE_JSON].lengthFunc = lengthOf_MYSQL_TYPE_JSON;
		m_columnInfo[MYSQL_TYPE_SET].lengthFunc = lengthOf_MYSQL_TYPE_SET;
		m_columnInfo[MYSQL_TYPE_ENUM].lengthFunc = lengthOf_MYSQL_TYPE_ENUM;
		*/
#endif

	}
	BinlogEventParser::BinlogEventParser(META::metaDataCollection * metaDataManager, ringBuffer* memPool) :
		m_metaDataManager(metaDataManager),  m_currentFileID(0), m_currentOffset(0), m_threadID(0), m_memPool(memPool),m_parsedRecordCount(0)
	{
		m_descEvent = new formatEvent(4, "5.6.16");
		initColumnTypeInfo();
		m_parsedRecords = new DATABASE_INCREASE::record*[2048];
                m_sqlParser = new SQL_PARSER::sqlParser();
	}
	BinlogEventParser::~BinlogEventParser()
	{
		if (m_descEvent != NULL)
			delete m_descEvent;
		delete[]m_parsedRecords;
		delete m_sqlParser;
	}
	int BinlogEventParser::init(const char * sqlParserFuncLibPath,const char* sqlParserTreePath)
	{
		if(0!=m_sqlParser->LoadFuncs(sqlParserFuncLibPath))
		{
			LOG(ERROR)<<"BinlogEventParser load sqlParser funcs from lib :"<<sqlParserFuncLibPath<<" failed";
			return -1;
		}
		if(0!=m_sqlParser->LoadParseTreeFromFile(sqlParserTreePath))
		{
			LOG(ERROR)<<"load parse tree from file :"<<sqlParserTreePath<<" failed";
			return -1;
		}
		LOG(INFO)<<"sqlParser in BinlogEventParser init from :"<<sqlParserFuncLibPath<<" and "<<sqlParserTreePath<<" sucess";
		return 0;
	}

	
	void BinlogEventParser::setInstance(const char* instance)
	{
		m_instance = instance;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::createDescEvent(
		const char * logEvent,size_t size)
	{
		if (m_descEvent != NULL)
			delete m_descEvent;
		m_descEvent = new formatEvent(logEvent,size);
		if (m_descEvent == NULL)
		{
			LOG(ERROR)<<"ILLEGAL Format_description_log_event event";
			return ParseStatus::ILLEGAL;
		}
		else
		{
			return ParseStatus::OK;
		}
	}
	BinlogEventParser::ParseStatus BinlogEventParser::updateFile(
		const char* logEvent, size_t size)
	{
		RotateEvent* tmp = new RotateEvent(logEvent, size, m_descEvent);
		if (tmp != NULL)
		{
			if (tmp->head.timestamp == 0)
			{
				uint64_t fileID = getFileId(tmp->fileName);
				LOG(ERROR) << "get new file " << tmp->fileName << " " << fileID << ",current file:"<<m_currentFileID;
				if (fileID == m_currentFileID + 1 || m_currentFileID == 0)
				{
					m_currentFileID = fileID;
					m_currentOffset = 0;
				}
				else if (fileID != m_currentFileID)
				{
					delete tmp;
					LOG(ERROR) << "binlog file id is not strict increase,current file id is " << m_currentFileID << ",new file is " << tmp->fileName;
					return ParseStatus::ILLEGAL;
				}
			}
			delete tmp;
		}
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseDDL(const char* logEvent, size_t size)
	{
		ParseStatus rtv = ParseStatus::OK;
		const char* sql;
		uint32_t sqlSize = 0;
		QueryEvent::getQuery(logEvent, size, m_descEvent, sql, sqlSize);
		char end = sql[sqlSize];
		((char*)sql)[sqlSize] = '\0';
		SQL_PARSER::handle *handle = nullptr;
		SQL_PARSER::parseValue pret = m_sqlParser->parse(handle,nullptr,sql);
		if (pret != SQL_PARSER::OK||handle == nullptr)
		{
			LOG(ERROR) << "parse ddl :[" << sql << "] failed";
			((char*)sql)[sqlSize] = end;
			rtv = ddl(logEvent,size);
			return rtv;//now ignore ddl parse failed 
		}
		((char*)sql)[sqlSize] = end;
		switch (handle->type)
		{
		case SQL_PARSER::BEGIN:
			rtv = begin(logEvent, size);
			break;
		case SQL_PARSER::COMMIT:
			rtv = commit(logEvent, size);
			break;
		case SQL_PARSER::ROLLBACK:
			rtv = rollback(logEvent, size);
			break;
		default:
			rtv = ddl(logEvent, size);
			break;
		}
		return rtv;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseQuery(const char* logEvent, size_t size)
	{
		const char* query = NULL;
		uint32_t querySize = 0;
		if (0 != QueryEvent::getQuery(logEvent,size, m_descEvent, query, querySize))
			return ParseStatus::ILLEGAL;
		if (query == NULL)
			return ParseStatus::OK;
		if (querySize==5&&memcmp(query, "BEGIN", 5) == 0)
			return begin(logEvent, size);
		else if (querySize==6&&memcmp(query, "COMMIT", 6) == 0)
			return commit(logEvent, size);
		else if (querySize==8&& memcmp(query, "ROLLBACK", querySize) == 0)
			return rollback(logEvent, size);
		else
			return parseDDL(logEvent, size);
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseTableMap(const char* logEvent, size_t size)
	{
		m_tableMap.init(m_descEvent, logEvent, size);
		return ParseStatus::OK;
	}
	static inline void setRecordBasicInfo(const commonMysqlBinlogEventHeader_v4* header, DATABASE_INCREASE::record* r)
	{
		r->head->headSize = DATABASE_INCREASE::recordHeadSize;
		r->head->timestamp = META::timestamp::create(header->timestamp, 0);
		r->head->txnId = 0;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::rollback(const char* logEvent, size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header =(commonMysqlBinlogEventHeader_v4*)logEvent;
		DATABASE_INCREASE::record * r = (DATABASE_INCREASE::record*)m_memPool->alloc(DATABASE_INCREASE::recordSize);
		r->init(((char*)r) + sizeof(DATABASE_INCREASE::record));
		setRecordBasicInfo(header, r);
		r->head->size = DATABASE_INCREASE::recordRealSize;
		r->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
		r->head->type = DATABASE_INCREASE::R_ROLLBACK;
		m_parsedRecords[m_parsedRecordCount++] = r;
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::ddl(const char* logEvent, size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header = (commonMysqlBinlogEventHeader_v4*)logEvent;
		QueryEvent query(logEvent, size, m_descEvent);
		LOG(ERROR)<<"ddl:"<<query.query;
		DATABASE_INCREASE::DDLRecord* r = (DATABASE_INCREASE::DDLRecord*)m_memPool->alloc(sizeof(DATABASE_INCREASE::DDLRecord)+DATABASE_INCREASE::DDLRecord::allocSize(query.db.size(),query.query.size()));
		r->create(((char*)r) + sizeof(DATABASE_INCREASE::DDLRecord), query.charset_inited?query.charset:nullptr,query.sql_mode,query.db.c_str(),query.query.c_str(), query.query.size());
		setRecordBasicInfo(header, r);
		r->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
		r->head->type = DATABASE_INCREASE::R_DDL;
		m_parsedRecords[m_parsedRecordCount++] = r;
		m_metaDataManager->processDDL(query.query.c_str(), query.db.empty()?nullptr:query.db.c_str(),r->head->logOffset);
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::begin(const char * logEvent,size_t size)
	{
		m_threadID = le32toh(*(uint32_t*)(logEvent + sizeof(commonMysqlBinlogEventHeader_v4)));//update thread id
		return ParseStatus::BEGIN;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::commit(const char* logEvent, size_t size)
	{
		m_threadID = 0;
		return ParseStatus::COMMIT;
	}
	void BinlogEventParser::parseRowLogEventHeader(const char*& logevent,uint64_t size,uint64_t &tableId, const uint8_t*& columnBitMap,const uint8_t *&updatedColumnBitMap)
	{
		uint8_t const commonHeaderLen = m_descEvent->common_header_len;
		Log_event_type eventType = (Log_event_type)logevent[EVENT_TYPE_OFFSET];
		uint8_t const postHeaderLen = m_descEvent->post_header_len[eventType - 1];

		const char* postStart = logevent + commonHeaderLen;
		postStart += ROWS_MAPID_OFFSET;
		if (postHeaderLen == 6)
		{
			tableId = uint4korr(postStart);
			postStart += 4;
		}
		else
		{
			tableId = uint6korr(postStart);
			postStart += ROWS_FLAGS_OFFSET;
		}
		/*
		*not use not
		uint16_t m_flags = uint2korr(postStart);
		*/
		postStart += 2;

		uint16_t varHeaderLen = 0;
		if (postHeaderLen == ROWS_HEADER_LEN_V2)
		{
			varHeaderLen = uint2korr(postStart);
			assert(varHeaderLen >= 2);
			varHeaderLen -= 2;
		}
		uint8_t const* const varStart = (const uint8_t*)logevent + commonHeaderLen
			+ postHeaderLen + varHeaderLen;
		uint8_t const* const ptrWidth = varStart;
		uint8_t* ptrAfterWidth = (uint8_t*)ptrWidth;
		size_t m_width = net_field_length(&ptrAfterWidth);
		columnBitMap = ptrAfterWidth;
		ptrAfterWidth += (m_width + 7) / 8;
		if ((eventType == UPDATE_ROWS_EVENT)
			|| (eventType == UPDATE_ROWS_EVENT_V1))
		{
			updatedColumnBitMap = ptrAfterWidth;
			ptrAfterWidth += (m_width + 7) / 8;
		}
		logevent = (const char*)ptrAfterWidth;
	}
	static inline uint8_t getRealType(uint8_t tableDefType, uint8_t metaType, const unsigned char* meta)
	{
		switch (tableDefType)
		{
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_STRING:
		{
			uint8_t type = metaType;
			if (*(uint16_t*)meta >= 256)
			{
				if ((meta[0] & 0x30) != 0x30)
				{
					type = meta[0] | 0x30;
					return type;
				}
			}
			return metaType;
		}
		case MYSQL_TYPE_DATE:
			return MYSQL_TYPE_NEWDATE;
		default:
			return tableDefType;
		}
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseRowData(DATABASE_INCREASE::DMLRecord *record,
		const char*& data, size_t size, bool newORold,const uint8_t* columnBitmap)
	{
		uint32_t metaIndex = 0;
		const uint8_t* nullBitMap = (const uint8_t*)data;
		data += BYTES_FOR_BITS(m_tableMap.columnCount);
		for (uint32_t idx = 0; idx < m_tableMap.columnCount; idx++)
		{
			const META::columnMeta* columnMeta = record->meta->getColumn(idx);
			int ctype = getRealType(m_tableMap.types[idx], columnMeta->m_srcColumnType, m_tableMap.metaInfo + metaIndex);
			if (!TEST_BITMAP(columnBitmap, idx)|| NULL_BIT(nullBitMap, idx))
			{
				if (newORold)
				{
					if (!META::columnInfos[columnMeta->m_columnType].fixed)
						record->setVarColumnNull(idx);
				}
				else
					record->setUpdatedColumnNull(idx);
				metaIndex += m_columnInfo[ctype].metaLength;
				continue;
			}
			if (unlikely(m_columnInfo[ctype].parseFunc == NULL))
			{
				m_error = std::String("unsupport column type :") << ctype<<",column :"<<columnMeta->m_columnName<<",id:"<< columnMeta->m_columnIndex;
				LOG(ERROR) << m_error;
				return ParseStatus::META_NOT_MATCH;
			}
			if (unlikely(0 != m_columnInfo[ctype].parseFunc(columnMeta, record, data, newORold)))
			{
				m_error = std::String("parse column type ") << ctype << " in " << m_currentOffset << "@" << m_currentFileID << " failed,table :" << record->meta->m_dbName << "." << record->meta->m_tableName;
				LOG(ERROR) << m_error;
				return ParseStatus::ILLEGAL;
			}
			metaIndex += m_columnInfo[ctype].metaLength;
		}
		return ParseStatus::OK;
	}
	uint64_t BinlogEventParser::getTableID(const char* data,
		Log_event_type event_type)
	{
		if (m_descEvent->post_header_len[event_type - 1] == 6)
		{
			/* Master is of an intermediate source tree before 5.1.4. Id is 4 bytes */
			return uint4korr(
				data + m_descEvent->common_header_len + ROWS_MAPID_OFFSET);
		}
		else
		{
			return uint6korr(
				data + m_descEvent->common_header_len + ROWS_MAPID_OFFSET);
		}
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseRowLogevent(
		const char * logEvent, size_t size, DATABASE_INCREASE::RecordType type)
	{
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)(logEvent);
		if (m_descEvent->alg != BINLOG_CHECKSUM_ALG_OFF&& m_descEvent->alg != BINLOG_CHECKSUM_ALG_UNDEF)
			size = size - BINLOG_CHECKSUM_LEN;
		uint64_t tableID = 0;
		const uint8_t *columnBitmap = nullptr,*updatedColumnBitMap = nullptr;
		const char* parsePos = logEvent, * end = logEvent + size;
		parseRowLogEventHeader(parsePos, size, tableID, columnBitmap,updatedColumnBitMap);
		assert(m_tableMap.tableID == tableID);
		META::tableMeta* meta = m_metaDataManager->get(m_tableMap.dbName, m_tableMap.tableName, createMysqlRecordOffset(m_currentFileID,m_currentOffset));
		if (meta == nullptr)
		{
			m_error =  std::String("can not get meta of table ") << m_tableMap.dbName << "." << m_tableMap.tableName << "from metaDataManager in " << m_currentOffset << "@" << m_currentFileID;
			LOG(ERROR)<<m_error;
			return ParseStatus::NO_META;
		}
		if (meta->m_columnsCount != m_tableMap.columnCount)
		{
			m_error =  std::String("column count from table_map [") << m_tableMap.columnCount << "] of table " << m_tableMap.dbName << "." << m_tableMap.tableName << " is diffrent from which is from metaDataManager [" << meta->m_columnsCount << "]";
			LOG(ERROR)<<m_error;
			return ParseStatus::META_NOT_MATCH;
		}
		ParseStatus rtv = ParseStatus::OK;
		while (parsePos < end)
		{
			DATABASE_INCREASE::DMLRecord* record = (DATABASE_INCREASE::DMLRecord*)m_memPool->alloc(sizeof(DATABASE_INCREASE::DMLRecord) + DATABASE_INCREASE::recordHeadSize + header->eventSize * 4);
			record->initRecord(((char*)record) + sizeof(DATABASE_INCREASE::DMLRecord),meta,type);
			setRecordBasicInfo(header, record);
			record->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);

			if (type == DATABASE_INCREASE::R_UPDATE || type == DATABASE_INCREASE::R_INSERT)
			{
				if (unlikely(ParseStatus::OK
					!= (rtv = parseRowData(record, parsePos, end - parsePos, true, columnBitmap))))
				{
					LOG(ERROR) << "parse record failed ,column:"<<record->meta->m_dbName<<"."<<record->meta->m_tableName;
					return rtv;
				}
			}
			if (type == DATABASE_INCREASE::R_UPDATE || type == DATABASE_INCREASE::R_DELETE)
			{
				if (type == DATABASE_INCREASE::R_UPDATE)
					record->startSetUpdateOldValue();
				if (unlikely(ParseStatus::OK
					!= (rtv = parseRowData(record, parsePos, end - parsePos, false, type == DATABASE_INCREASE::R_UPDATE? updatedColumnBitMap: columnBitmap))))
				{
					LOG(ERROR)<<"parse record failed";
					return rtv;
				}
			}
			record->finishedSet();
			m_parsedRecords[m_parsedRecordCount++] = record;
			if (unlikely(parsePos > end))
			{
				m_error =  std::String("parse record failed for read over the end of log event,table :") << m_tableMap.dbName << "." << m_tableMap.tableName << " ,checkpoint:" << m_currentOffset << "@" << m_currentFileID;
				LOG(ERROR)<<m_error;
				return ParseStatus::ILLEGAL;
			}
		}
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseTraceID(const char * logEvent,
		size_t size)
	{
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parser(const char* logEvent,size_t size)
	{
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)(logEvent);
		m_currentOffset = header->eventOffset;
		ParseStatus rtv = ParseStatus::OK;
		m_parsedRecordCount = 0;
		switch (header->type)
		{
		case WRITE_ROWS_EVENT:
		case WRITE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(logEvent,size, DATABASE_INCREASE::R_INSERT);
			break;
		case UPDATE_ROWS_EVENT:
		case UPDATE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(logEvent,size, DATABASE_INCREASE::R_UPDATE);
			break;
		case DELETE_ROWS_EVENT:
		case DELETE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(logEvent, size, DATABASE_INCREASE::R_DELETE);
			break;
		case QUERY_EVENT:
			rtv = parseQuery(logEvent, size);
			break;
		case TABLE_MAP_EVENT:
			rtv = parseTableMap(logEvent, size);
			break;
		case XID_EVENT:
			rtv = commit(logEvent, size);
			break;
		case ROTATE_EVENT:
			rtv = updateFile(logEvent, size);
			break;
		case ROWS_QUERY_LOG_EVENT:
			rtv = parseTraceID(logEvent, size);
			break;
		case FORMAT_DESCRIPTION_EVENT:
			rtv = createDescEvent(logEvent, size);
			break;
		default:
			break;
		}
		if (rtv <= FILTER)
		{
			return rtv;
		}
		else
		{
			LOG(ERROR)<<"parse failed ,error:"<<m_error<<" in "<<m_currentOffset<<"@"<<m_currentFileID;
			return rtv;
		}
	}
	std::String BinlogEventParser::getError()
	{
		return m_error;
	}

}
