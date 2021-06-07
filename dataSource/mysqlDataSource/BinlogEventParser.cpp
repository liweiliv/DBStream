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
#include "meta/ddl.h"
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

#define PUSH_RECORD(r) m_parsedRecords[m_parsedRecordCount++] = (r);
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
	BinlogEventParser::BinlogEventParser(META::metaDataCollection* metaDataManager, ringBuffer* memPool) :
		m_metaDataManager(metaDataManager), m_currentFileID(0), m_currentOffset(0), m_threadID(0), m_memPool(memPool), m_parsedRecordCount(0), m_parsedRecordBegin(0)
	{
		m_descEvent = new formatEvent(4, "5.6.16");
		initColumnTypeInfo();
		m_parsedRecords = new DATABASE_INCREASE::record * [M_PARSER_RECORD_CACHE_VOLUMN];
		m_sqlParser = new SQL_PARSER::sqlParser();
	}
	BinlogEventParser::~BinlogEventParser()
	{
		if (m_descEvent != NULL)
			delete m_descEvent;
		delete[]m_parsedRecords;
		delete m_sqlParser;
	}
	DS BinlogEventParser::init(const char* sqlParserFuncLibPath, const char* sqlParserTreePath)
	{
		if (0 != m_sqlParser->LoadFuncs(sqlParserFuncLibPath))
		{
			dsFailedAndLogIt(1, "BinlogEventParser load sqlParser funcs from lib :" << sqlParserFuncLibPath << " failed", ERROR);
		}
		if (0 != m_sqlParser->LoadParseTreeFromFile(sqlParserTreePath))
		{
			dsFailedAndLogIt(1, "load parse tree from file :" << sqlParserTreePath << " failed", ERROR);
		}
		LOG(INFO) << "sqlParser in BinlogEventParser init from :" << sqlParserFuncLibPath << " and " << sqlParserTreePath << " sucess";
		dsOk();
	}


	void BinlogEventParser::setInstance(const char* instance)
	{
		m_instance = instance;
	}
	DS BinlogEventParser::createDescEvent(
		const char* logEvent, size_t size)
	{
		if (m_descEvent != NULL)
			delete m_descEvent;
		m_descEvent = new formatEvent(logEvent, size);
		if (m_descEvent == NULL)
		{
			dsFailedAndLogIt(ParseStatus::ILLEGAL, "ILLEGAL Format_description_log_event event", ERROR);
		}
		dsOk();
	}

	DS BinlogEventParser::updateFile(
		const char* logEvent, size_t size)
	{
		RotateEvent* tmp = new RotateEvent(logEvent, size, m_descEvent);
		if (tmp != NULL)
		{
			if (tmp->head.timestamp == 0)
			{
				uint64_t fileID = getFileId(tmp->fileName);
				LOG(INFO) << "get new file " << tmp->fileName << " " << fileID << ",current file:" << m_currentFileID;
				if (fileID == m_currentFileID + 1 || m_currentFileID == 0)
				{
					m_currentFileID = fileID;
					m_currentOffset = 0;
				}
				else if (fileID != m_currentFileID)
				{
					delete tmp;
					dsFailedAndLogIt(ParseStatus::ILLEGAL, "binlog file id is not strict increase,current file id is " << m_currentFileID << ",new file is " << tmp->fileName, ERROR);
				}
			}
			delete tmp;
		}
		dsOk();;
	}

	DS BinlogEventParser::parseDDL(const char* logEvent, size_t size)
	{
		const char* sql;
		uint32_t sqlSize = 0;
		QueryEvent::getQuery(logEvent, size, m_descEvent, sql, sqlSize);
		char end = sql[sqlSize];
		((char*)sql)[sqlSize] = '\0';
		SQL_PARSER::handle* handle = nullptr;
		SQL_PARSER::parseValue pret = m_sqlParser->parse(handle, nullptr, sql);
		if (pret != SQL_PARSER::OK || handle == nullptr)
		{
			LOG(ERROR) << "parse ddl :[" << sql << "] failed";
			((char*)sql)[sqlSize] = end;
			dsReturn(ddl(logEvent, size));//now ignore ddl parse failed 
		}
		((char*)sql)[sqlSize] = end;
		DS rtv = 0;
		if (handle->userData != nullptr)
		{
			switch (static_cast<META::ddl*>(handle->userData)->m_type)
			{
			case META::BEGIN:
				rtv = begin(logEvent, size);
				break;
			case META::COMMIT:
				rtv = commit(logEvent, size);
				break;
			case META::ROLLBACK:
				rtv = rollback(logEvent, size);
				break;
			default:
				rtv = ddl(logEvent, size);
				break;
			}
		}
		delete handle;
		dsReturn(rtv);
	}
	DS BinlogEventParser::parseQuery(const char* logEvent, size_t size)
	{
		const char* query = nullptr;
		uint32_t querySize = 0;
		if (0 != QueryEvent::getQuery(logEvent, size, m_descEvent, query, querySize))
			return ParseStatus::ILLEGAL;
		if (query == nullptr)
			return ParseStatus::OK;
		if (querySize == 5 && memcmp(query, "BEGIN", 5) == 0)
			return begin(logEvent, size);
		else if (querySize == 6 && memcmp(query, "COMMIT", 6) == 0)
			return commit(logEvent, size);
		else if (querySize == 8 && memcmp(query, "ROLLBACK", querySize) == 0)
			return rollback(logEvent, size);
		else
			return parseDDL(logEvent, size);
	}
	DS BinlogEventParser::parseTableMap(const char* logEvent, size_t size)
	{
		m_tableMap.init(m_descEvent, logEvent, size);
		dsOk();
	}
	static inline void setRecordBasicInfo(const commonMysqlBinlogEventHeader_v4* header, DATABASE_INCREASE::record* r)
	{
		r->head->minHead.headSize = DATABASE_INCREASE::recordHeadSize;
		r->head->timestamp = META::timestamp::create(header->timestamp, 0);
		r->head->txnId = 0;
	}
	DS BinlogEventParser::rollback(const char* logEvent, size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header = (commonMysqlBinlogEventHeader_v4*)logEvent;
		DATABASE_INCREASE::record* r = (DATABASE_INCREASE::record*)m_memPool->alloc(DATABASE_INCREASE::recordSize);
		r->init(((char*)r) + sizeof(DATABASE_INCREASE::record));
		setRecordBasicInfo(header, r);
		r->head->minHead.size = DATABASE_INCREASE::recordRealSize;
		r->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
		r->head->minHead.type = static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_ROLLBACK);
		PUSH_RECORD(r);
		dsOk();
	}
	DS BinlogEventParser::ddl(const char* logEvent, size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header = (commonMysqlBinlogEventHeader_v4*)logEvent;
		QueryEvent query(logEvent, size, m_descEvent);
		LOG(INFO) << "ddl:" << query.query;
		DATABASE_INCREASE::DDLRecord* r = (DATABASE_INCREASE::DDLRecord*)m_memPool->alloc(sizeof(DATABASE_INCREASE::DDLRecord) + DATABASE_INCREASE::DDLRecord::allocSize(query.db.size(), query.query.size() + 1));
		r->create(((char*)r) + sizeof(DATABASE_INCREASE::DDLRecord), query.charset_inited ? query.charset : nullptr, query.sql_mode, query.db.c_str(), query.query.c_str(), query.query.size());
		setRecordBasicInfo(header, r);
		r->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
		r->head->minHead.type = static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_DDL);
		PUSH_RECORD(r);
		m_metaDataManager->processDDL(query.query.c_str(), query.db.empty() ? nullptr : query.db.c_str(), r->head->logOffset);
		dsOk();
	}
	DS BinlogEventParser::begin(const char* logEvent, size_t size)
	{
		m_threadID = le32toh(*(uint32_t*)(logEvent + sizeof(commonMysqlBinlogEventHeader_v4)));//update thread id
		dsReturnCode(ParseStatus::BEGIN);
	}
	DS BinlogEventParser::commit(const char* logEvent, size_t size)
	{
		m_threadID = 0;
		dsReturnCode(ParseStatus::COMMIT);
	}
	void BinlogEventParser::parseRowLogEventHeader(const char*& logevent, uint64_t size, uint64_t& tableId, const uint8_t*& columnBitMap, const uint8_t*& updatedColumnBitMap)
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
		*not use now
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
	DS BinlogEventParser::parseRowData(DATABASE_INCREASE::DMLRecord* record,
		const char*& data, size_t size, bool newORold, const uint8_t* columnBitmap)
	{
		uint32_t metaIndex = 0;
		const uint8_t* nullBitMap = (const uint8_t*)data;
		data += BYTES_FOR_BITS(m_tableMap.columnCount);
		for (uint32_t idx = 0; idx < m_tableMap.columnCount; idx++)
		{
			const META::columnMeta* columnMeta = record->meta->getColumn(idx);
			int ctype = getRealType(m_tableMap.types[idx], columnMeta->m_srcColumnType, m_tableMap.metaInfo + metaIndex);
			if (!TEST_BITMAP(columnBitmap, idx) || NULL_BIT(nullBitMap, idx))
			{
				if (newORold)
				{
					if (!META::columnInfos[TID(columnMeta->m_columnType)].fixed)
						record->setVarColumnNull(idx);
					else
						record->setFixedColumnNull(idx);
				}
				else
				{
					if (!META::columnInfos[TID(columnMeta->m_columnType)].fixed)
						record->setUpdatedVarColumnNull(idx);
					else
						record->setUpdatedFixedColumnNull(idx);
				}
				metaIndex += m_columnInfo[ctype].metaLength;
				continue;
			}
			if (unlikely(m_columnInfo[ctype].parseFunc == NULL))
				dsFailedAndLogIt(ParseStatus::META_NOT_MATCH, "unsupported column type :" << ctype << ",column :" << columnMeta->m_columnName << ",id:" << columnMeta->m_columnIndex, ERROR);
			if (unlikely(0 != m_columnInfo[ctype].parseFunc(columnMeta, record, data, newORold)))
				dsFailedAndLogIt(ParseStatus::ILLEGAL, 
					"parse column type :" << ctype << " in " << m_currentOffset << "@" << m_currentFileID << " failed,table :" << record->meta->m_dbName << "." << record->meta->m_tableName, ERROR);
			metaIndex += m_columnInfo[ctype].metaLength;
		}
		dsOk();
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

	DS BinlogEventParser::parseRowLogevent(
		const char* logEvent, size_t size, DATABASE_INCREASE::RecordType type)
	{
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)(logEvent);
		if (m_descEvent->alg != BINLOG_CHECKSUM_ALG_OFF && m_descEvent->alg != BINLOG_CHECKSUM_ALG_UNDEF)
			size = size - BINLOG_CHECKSUM_LEN;
		uint64_t tableID = 0;
		const uint8_t* columnBitmap = nullptr, * updatedColumnBitMap = nullptr;
		const char* parsePos = logEvent, * end = logEvent + size;
		parseRowLogEventHeader(parsePos, size, tableID, columnBitmap, updatedColumnBitMap);
		assert(m_tableMap.tableID == tableID);
		META::tableMeta* meta = m_metaDataManager->get(m_tableMap.dbName, m_tableMap.tableName, createMysqlRecordOffset(m_currentFileID, m_currentOffset));
		if (meta == nullptr)
		{
			dsFailedAndLogIt(ParseStatus::NO_META,
				"can not get meta of tablee :" << m_tableMap.dbName << "." << m_tableMap.tableName << "from metaDataManager in " << m_currentOffset << "@" << m_currentFileID, ERROR);
		}
		if (meta->m_columnsCount != m_tableMap.columnCount)
		{
			dsFailedAndLogIt(ParseStatus::META_NOT_MATCH,
				"column count from table_map [" << m_tableMap.columnCount << "] of table " << m_tableMap.dbName << "." << m_tableMap.tableName << " is diffrent from which is from metaDataManager [" << meta->m_columnsCount << "]", ERROR);
		}
		while (parsePos < end)
		{
			DATABASE_INCREASE::DMLRecord* record = (DATABASE_INCREASE::DMLRecord*)m_memPool->alloc(sizeof(DATABASE_INCREASE::DMLRecord) + DATABASE_INCREASE::recordHeadSize + header->eventSize * 4);
			record->initRecord(((char*)record) + sizeof(DATABASE_INCREASE::DMLRecord), meta, type);
			setRecordBasicInfo(header, record);
			record->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
			dsReturnIfFailed(parseRowData(record, parsePos, end - parsePos, true, columnBitmap));
			record->finishedSet();
			PUSH_RECORD(record);
			if (unlikely(parsePos > end))
			{
				dsFailedAndLogIt(ParseStatus::ILLEGAL,
					"parse record failed for read over the end of log event,table :" << m_tableMap.dbName << "." << m_tableMap.tableName << " ,checkpoint:" << m_currentOffset << "@" << m_currentFileID, ERROR);
			}
		}
		return ParseStatus::OK;
	}

	DS BinlogEventParser::parseTraceID(const char* logEvent,
		size_t size)
	{
		dsOk();
	}

	DS BinlogEventParser::parseUpdateRowLogevent(const char* logEvent, size_t size)
	{
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)(logEvent);
		if (m_descEvent->alg != BINLOG_CHECKSUM_ALG_OFF && m_descEvent->alg != BINLOG_CHECKSUM_ALG_UNDEF)
			size = size - BINLOG_CHECKSUM_LEN;
		uint64_t tableID = 0;
		const uint8_t* columnBitmap = nullptr, * updatedColumnBitMap = nullptr;
		const char* parsePos = logEvent, * end = logEvent + size;
		parseRowLogEventHeader(parsePos, size, tableID, columnBitmap, updatedColumnBitMap);
		assert(m_tableMap.tableID == tableID);
		META::tableMeta* meta = m_metaDataManager->get(m_tableMap.dbName, m_tableMap.tableName, createMysqlRecordOffset(m_currentFileID, m_currentOffset));

		if (meta == nullptr)
		{
			dsFailedAndLogIt(ParseStatus::NO_META,
				"can not get meta of table " << m_tableMap.dbName << "." << m_tableMap.tableName << "from metaDataManager in " << m_currentOffset << "@" << m_currentFileID, ERROR);
		}
		if (meta->m_columnsCount != m_tableMap.columnCount)
		{
			dsFailedAndLogIt(ParseStatus::META_NOT_MATCH,
				"column count from table_map [" << m_tableMap.columnCount << "] of table " << m_tableMap.dbName << "." << m_tableMap.tableName << " is diffrent from which is from metaDataManager [" << meta->m_columnsCount << "]", ERROR);
		}

		while (parsePos < end)
		{
			DATABASE_INCREASE::DMLRecord* record = (DATABASE_INCREASE::DMLRecord*)m_memPool->alloc(sizeof(DATABASE_INCREASE::DMLRecord) + DATABASE_INCREASE::recordHeadSize + header->eventSize * 4);
			record->initRecord(((char*)record) + sizeof(DATABASE_INCREASE::DMLRecord), meta, DATABASE_INCREASE::RecordType::R_UPDATE);
			setRecordBasicInfo(header, record);
			record->head->logOffset = createMysqlRecordOffset(m_currentFileID, m_currentOffset);
			dsReturnIfFailed(parseRowData(record, parsePos, end - parsePos, true, columnBitmap));
			record->startSetUpdateOldValue();
			dsReturnIfFailed(parseRowData(record, parsePos, end - parsePos, false, updatedColumnBitMap));
			record->finishedSet();
			PUSH_RECORD(record);
			if (unlikely(parsePos > end))
			{
				dsFailedAndLogIt(ParseStatus::META_NOT_MATCH,
					"parse record failed for read over the end of log event,table :" << m_tableMap.dbName << "." << m_tableMap.tableName << " ,checkpoint:" << m_currentOffset << "@" << m_currentFileID, ERROR);
			}
		}
		dsOk();
	}

	DS BinlogEventParser::parser(const char* logEvent, size_t size)
	{
		if (unlikely(m_parsedRecordCount > m_parsedRecordBegin))
			return ParseStatus::REMAIND_RECORD_UNREAD;
		m_parsedRecordCount = m_parsedRecordBegin = 0;
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)(logEvent);
		m_currentOffset = header->eventOffset;
		DS rtv = 0;
		switch (header->type)
		{
		case WRITE_ROWS_EVENT:
		case WRITE_ROWS_EVENT_V1:
			dsReturn(parseRowLogevent(logEvent, size, DATABASE_INCREASE::RecordType::R_INSERT));
		case UPDATE_ROWS_EVENT:
		case UPDATE_ROWS_EVENT_V1:
			dsReturn(parseUpdateRowLogevent(logEvent, size));
			break;
		case DELETE_ROWS_EVENT:
		case DELETE_ROWS_EVENT_V1:
			dsReturn(parseRowLogevent(logEvent, size, DATABASE_INCREASE::RecordType::R_DELETE));
		case QUERY_EVENT:
			dsReturn(parseQuery(logEvent, size));
		case TABLE_MAP_EVENT:
			dsReturn(parseTableMap(logEvent, size));
		case XID_EVENT:
			dsReturn(commit(logEvent, size));
		case ROTATE_EVENT:
			dsReturn(updateFile(logEvent, size));
			break;
		case ROWS_QUERY_LOG_EVENT:
			dsReturn(parseTraceID(logEvent, size));
		case FORMAT_DESCRIPTION_EVENT:
			dsReturn(createDescEvent(logEvent, size));
		default:
			dsOk();
		}
	}
}
