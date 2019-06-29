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
#include "../../glog/logging.h"
#include "../../util/file.h"
#include "../../util/winString.h"
#include "../../sqlParser/sqlParser.h"
#define MAX_BATCH 1024
#define ROWS_MAPID_OFFSET    0
#define ROWS_FLAGS_OFFSET    6
#define ROWS_VHLEN_OFFSET    8
#define ROWS_V_TAG_LEN       1
#define ROWS_V_EXTRAINFO_TAG 0
namespace DATA_SOURCE {

	arena::arena(uint32_t size)
	{
		next = NULL;
		buf = new char[bufSize = size];
		bufUsedSize = 0;
		end = this;
	}
	void arena::clear()
	{
		if (next)
		{
			delete next;
			next = NULL;
		}
		bufSize = 0;
		bufUsedSize = 0;
		end = this;
	}
	arena::~arena()
	{
		if (next)
			delete next;
		delete[] buf;
	}
	void* arena::alloc(uint32_t size)
	{
		if (end->bufSize - end->bufUsedSize < size)
		{
			arena* a = new arena(size > bufSize ? size : bufSize);
			end->next = a;
		}
		char* buf = end->buf + end->bufUsedSize;
		end->bufUsedSize += size;
		return buf;
	}
#pragma pack()
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

		m_columnInfo[MYSQL_TYPE_NEWDECIMAL].lengthFunc = lengthOf_MYSQL_TYPE_NEWDECIMAL;
		m_columnInfo[MYSQL_TYPE_TIMESTAMP2].lengthFunc = lengthOf_MYSQL_TYPE_TIMESTAMP2;
		m_columnInfo[MYSQL_TYPE_DATETIME2].lengthFunc = lengthOf_MYSQL_TYPE_DATETIME2;
		m_columnInfo[MYSQL_TYPE_TIME2].lengthFunc = lengthOf_MYSQL_TYPE_TIME2;
		m_columnInfo[MYSQL_TYPE_BIT].lengthFunc = lengthOf_MYSQL_TYPE_BIT;
		m_columnInfo[MYSQL_TYPE_JSON].lengthFunc = lengthOf_MYSQL_TYPE_JSON;
		m_columnInfo[MYSQL_TYPE_SET].lengthFunc = lengthOf_MYSQL_TYPE_SET;
		m_columnInfo[MYSQL_TYPE_ENUM].lengthFunc = lengthOf_MYSQL_TYPE_ENUM;
#endif

	}
	BinlogEventParser::BinlogEventParser(META::metaDataCollection * metaDataManager) :
		m_metaDataManager(metaDataManager),  m_currentFileID(0), m_currentOffset(0), m_threadID(0)
	{
		m_descEvent = new formatEvent(4, "5.6.16");
		initColumnTypeInfo();
#ifdef COMPARE_UPDATE
		memset(m_oldImageOfUpdateRow, 0, sizeof(m_oldImageOfUpdateRow));
#endif
	}
	BinlogEventParser::~BinlogEventParser()
	{
		if (m_descEvent != NULL)
			delete m_descEvent;
	}
	void BinlogEventParser::setInstance(const char* instance)
	{
		m_instance = instance;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::createDescEvent(
		const char * logEvent,size_t size)
	{
		const char* error_msg;
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
		const char* error_msg;
		RotateEvent* tmp = new RotateEvent(logEvent, size, m_descEvent);
		if (tmp != NULL)
		{
			if (tmp->head.timestamp == 0)
			{
				uint64_t fileID = getFileId(tmp->fileName);
				LOG(ERROR) << "get new file " << tmp->fileName << " " << fileID << ",current file:m_currentFileID";
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
		const char* error_msg;
		const char* sql;
		uint32_t sqlSize = 0;
		QueryEvent::getQuery(logEvent, size, m_descEvent, sql, sqlSize);
		char end = sql[sqlSize];
		((char*)sql)[sqlSize] = '\0';
		SQL_PARSER::handle *handle = nullptr;
		SQL_PARSER::parseValue pret = m_sqlParser->parseSqlType(handle,sql);
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
		const commonMysqlBinlogEventHeader_v4* header = (const commonMysqlBinlogEventHeader_v4*)logEvent;
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
	BinlogEventParser::ParseStatus BinlogEventParser::rollback(const char* logEvent, size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header =(commonMysqlBinlogEventHeader_v4*)logEvent;
		createNewRecord();
		m_currentRecord->setTimestamp(header->timestamp);
		m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
		m_currentRecord->setRecordType(EROLLBACK);
		return commitCachedRecord(wrapper);
	}
	BinlogEventParser::ParseStatus BinlogEventParser::ddl(const char* logEvent, size_t size)
	{
		bool isSingleDDL = (m_firstTransaction == NULL);
		if (isSingleDDL)
		{
			initTransaction(MAX_BATCH);
		}
		createNewRecord();
		m_parseMem.init(m_currentRecord);
		char* envStr;
		uint32_t envStrAllocedSize = sizeof(DDL_SQL_MODE_HEAD_STR) + 22; // 64 bit int sqlmode max is 18446744073709551615
		envStrAllocedSize += sizeof(DDL_SQL_TIMESTAMP_HEAD_STR) + 22 + 11; // SET TIMESTAMP=%lu.%u  unsigned long max length is 22 ,unsigned int max length is 11
		envStrAllocedSize += sizeof(DDL_SQL_NAME_HEAD_STR) + 32; //max 32 byte chaset string
		envStr = m_parseMem.alloc(envStrAllocedSize);
		uint32_t envStrSize = 0;
		if (ddlEvent->tv_usec)
		{
			envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET TIMESTAMP=%ld.%06d;",
				(long)ddlEvent->head.timestamp,
				(int)ddlEvent->tv_usec);
		}
		else
		{
			envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET TIMESTAMP=%ld;",
				(long)ddlEvent->head.timestamp);
		}

		if (likely(ddlEvent->sql_mode_inited))
		{
			envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET @@session.sql_mode=%lu;", (ulong)ddlEvent->sql_mode);
		}
		if (likely(ddlEvent->charset_inited))
		{
			char* charset_p = ddlEvent->charset; // Avoid type-punning warning.
			CHARSET_INFO* cs_info = get_charset(uint2korr(charset_p), MYF(MY_WME));
			if (cs_info && cs_info->csname)
			{
				m_currentRecord->setRecordEncoding(cs_info->csname);
				envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
					"SET names %s;", cs_info->csname);
			}
		}
		m_currentRecord->setNewColumn(
			(binlogBuf*)m_parseMem.alloc(sizeof(binlogBuf) * 2), 2);
		m_currentRecord->putNew(ddlEvent->query.c_str(), ddlEvent->query.size());
		m_currentRecord->putNew(envStr, strlen(envStr));
		m_currentRecord->setRecordType(EDDL);
		m_currentRecord->setTimestamp(ddlEvent->head.timestamp);
		m_currentRecord->setRecordUsec(ddlEvent->tv_usec);
		m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
		m_currentRecord->setThreadId(ddlEvent->thread_id);
		if (!ddlEvent->db.empty())
			m_currentRecord->setDbname(ddlEvent->db.c_str());
		handleDDL(m_currentRecord);
		ParseStatus rtv = ParseStatus::OK;
		if (isSingleDDL)
		{
			commitCachedRecord(wrapper);
			rtv = ParseStatus::TRANS_COMMIT;
		}
		m_parseMem.reset();
		return rtv;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::begin(const char * logEvent,size_t size)
	{
		commonMysqlBinlogEventHeader_v4* header = (commonMysqlBinlogEventHeader_v4*)(logEvent);
		m_threadID = le32toh(*(uint32_t*)(logEvent + sizeof(commonMysqlBinlogEventHeader_v4)));
		createNewRecord();
		m_currentRecord->setTimestamp(header->timestamp);
		m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
		m_currentRecord->setThreadId(m_threadID);
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::commit(const char* logEvent, size_t size)
	{
		if (m_currentTransaction)
		{
			commonMysqlBinlogEventHeader_v4* header =
				static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
			createNewRecord();
			m_currentRecord->setTimestamp(header->timestamp);
			m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
			m_currentRecord->setRecordType(ECOMMIT);
			m_currentRecord->setThreadId(m_threadID);
			commitCachedRecord(wrapper);
		}
		m_threadID = 0;
		return ParseStatus::TRANS_COMMIT;
	}
	void BinlogEventParser::jumpOverRowLogEventHeader(const char*& logevent,
		uint64_t size)
	{
		const char* rowBegin = logevent;
		uint8_t const common_header_len = m_descEvent->common_header_len;
		Log_event_type event_type = (Log_event_type)logevent[EVENT_TYPE_OFFSET];
		uint8_t const post_header_len = m_descEvent->post_header_len[event_type - 1];

		const char* post_start = logevent + common_header_len;
		post_start += ROWS_MAPID_OFFSET;
		if (post_header_len == 6)
		{
			post_start += 4;
		}
		else
		{
			post_start += ROWS_FLAGS_OFFSET;
		}
		uint16_t m_flags = uint2korr(post_start);
		post_start += 2;

		uint16_t var_header_len = 0;
		if (post_header_len == ROWS_HEADER_LEN_V2)
		{
			var_header_len = uint2korr(post_start);
			assert(var_header_len >= 2);
			var_header_len -= 2;
		}
		uint8_t const* const var_start = (const uint8_t*)logevent + common_header_len
			+ post_header_len + var_header_len;
		uint8_t const* const ptr_width = var_start;
		uint8_t* ptr_after_width = (uint8_t*)ptr_width;
		size_t m_width = net_field_length(&ptr_after_width);
		ptr_after_width += (m_width + 7) / 8;
		if ((event_type == UPDATE_ROWS_EVENT)
			|| (event_type == UPDATE_ROWS_EVENT_V1))
		{
			ptr_after_width += (m_width + 7) / 8;
		}
		logevent = (const char*)ptr_after_width;
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
	/*
	 * check if old image and new image is the same
	 * if is the same,not need to parse this column,use ptr of old image
	 * but like blob,varchar,geo... ,those type we do not parse ,only save its point and size,memcpy is a waste of time
	 * use m_ifTypeNeedCompare to filter those type
	 * */
#define checkUpdate(rtv,data,tableMeta,meta,idx,nullBitMap,metaIndex,ctype)\
do \
{\
    if(NULL_BIT(nullBitMap, idx) != 0) \
    {\
        m_currentRecord->putNew(NULL, 0);\
        metaIndex += m_columnInfo[ctype].metaLength;\
        rtv = true;\
    }\
    else\
    {\
    	  uint32_t colSize = m_fixedTypeSize[ctype]>0?m_fixedTypeSize[ctype]:m_oldImageOfUpdateRow[idx+1]-m_oldImageOfUpdateRow[idx];\
        if((m_fixedTypeSize[ctype]>0|| \
        		(m_columnInfo[ctype].lengthFunc!=NULL&& \
                m_columnInfo[ctype].lengthFunc(columnMeta,tableMeta->metaInfo + metaIndex,data)==colSize))&& \
                memcmp(m_oldImageOfUpdateRow[idx],data,colSize)==0 \
                ) \
        {\
            uint32_t s;\
            binlogBuf* olds = m_currentRecord->oldCols(s);\
            m_currentRecord->putNew(olds[idx].buf, olds[idx].buf_used_size);\
            metaIndex += m_columnInfo[ctype].metaLength;\
            data+=colSize;\
            rtv = true;\
        }\
        else \
		    rtv = false; \
    } \
}while(0);

	BinlogEventParser::ParseStatus BinlogEventParser::parseRowData(DATABASE_INCREASE::DMLRecord *record,
		const char*& data, size_t size, bool newORold)
	{
		uint32_t metaIndex = 0;
		const uint8_t* nullBitMap = (const uint8_t*)data;
		data += BYTES_FOR_BITS(m_tableMap.columnCount);
		for (uint32_t idx = 0; idx < m_tableMap.columnCount; idx++)
		{
			char* parsedValue = NULL;
			uint32_t parsedValueSize = 0;
			const META::columnMeta* columnMeta = record->meta->getColumn(idx);
			int ctype = getRealType(m_tableMap.types[idx],columnMeta->m_srcColumnType,m_tableMap.metaInfo + metaIndex);
#ifdef COMPARE_UPDATE
			if (record->head->type == DATABASE_INCREASE::R_UPDATE)
			{
				if (newORold)
				{
					bool rtv = false;
					checkUpdate(rtv, data, tableMeta, meta, idx, nullBitMap, metaIndex, ctype);
					if (rtv)
						continue;
				}
				else
					m_oldImageOfUpdateRow[idx] = data;
			}
#endif
			if (NULL_BIT(nullBitMap, idx) == 0)
			{
				if (unlikely(m_columnInfo[ctype].parseFunc == NULL))
				{
					LOG(ERROR)<<"unsupport column type :"<<ctype;
					return ParseStatus::META_NOT_MATCH;
				}
				if (unlikely(0!= m_columnInfo[ctype].parseFunc(columnMeta,record,data,newORold)))
				{
					LOG(ERROR) << "parse column type %d in %lu@%lu failed,table :%s.%s",
						ctype, m_currentRecord->getCheckpoint2(),
						m_currentRecord->getCheckpoint1(),
						m_currentRecord->dbname(), m_currentRecord->tbname());
					return ParseStatus::ILLEGAL;
				}
			}
			else
			{
				if (newORold)
				{
					record->setFixedColumn
				}
			}
			metaIndex += m_columnInfo[ctype].metaLength;
		}
#ifdef COMPARE_UPDATE
		if (type == EUPDATE && !newORold)
			m_oldImageOfUpdateRow[tableMeta->columnCount] = data;
#endif
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
		const char * logEvent,  DATABASE_INCREASE::RecordType type)
	{
		const commonMysqlBinlogEventHeader_v4* header =
			static_cast<const commonMysqlBinlogEventHeader_v4*>(logEvent);

		enum_binlog_checksum_alg alg = m_descEvent->alg;
		if (alg != BINLOG_CHECKSUM_ALG_OFF
			&& alg != BINLOG_CHECKSUM_ALG_UNDEF)
			rawDataSize = rawDataSize - BINLOG_CHECKSUM_LEN;
		jumpOverRowLogEventHeader(data, rawDataSize);

		const char* parsePos = data, * end = static_cast<const char*>(wrapper->rawData) + rawDataSize;
		uint64_t tableID = getTableID(static_cast<const char*>(wrapper->rawData),
			(Log_event_type)header->type);

		const tableMap* tableMeta = m_tableMapCache.get(tableID);
		assert(tableMeta != NULL);
		ITableMeta* meta = m_metaDataManager->getMeta(tableMeta->dbName,
			tableMeta->tableName);
		if (meta == NULL)
		{
			Log_r::Error("can not get meta of table %s.%s from metaDataManager in %lu@%lu",
				tableMeta->dbName, tableMeta->tableName, m_currentOffset, m_currentFileID);
			return ParseStatus::NO_META;
		}
		if (meta->getColCount() != tableMeta->columnCount)
		{
			Log_r::Error(
				"column count from table_map [%u] of table %s.%s is diffrent from which is from metaDataManager [%d]",
				tableMeta->columnCount, tableMeta->dbName, tableMeta->tableName,
				meta->getColCount());
			return ParseStatus::META_NOT_MATCH;
		}
		if (m_currentTransaction == NULL)
			initTransaction(MAX_BATCH);
		ParseStatus rtv = ParseStatus::OK;
		while (parsePos < end)
		{
			if (m_currentTransaction->recordCount >= MAX_BATCH)
				initTransaction(MAX_BATCH);

			createNewRecord();
			m_parseMem.init(m_currentRecord);
			m_currentRecord->setRecordType(type);
			m_currentRecord->setTimestamp(header->timestamp);
			m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
			m_currentRecord->setDbname(tableMeta->dbName);
			m_currentRecord->setTbname(tableMeta->tableName);
			m_currentRecord->setTableMeta(meta);
			m_currentRecord->setThreadId(m_threadID);
			if (type == EUPDATE || type == EDELETE)
			{
				m_currentRecord->setOldColumn(
					(binlogBuf*)m_parseMem.alloc(
						sizeof(binlogBuf) * tableMeta->columnCount),
					tableMeta->columnCount);
				if (0
					!= (rtv = parseRowData(parsePos,
						data + rawDataSize - parsePos, tableMeta, meta,
						false, type)))
				{
					Log_r::Error("parse record failed");
					return rtv;
				}
			}
			if (type == EUPDATE || type == EINSERT)
			{
				m_currentRecord->setNewColumn(
					(binlogBuf*)m_parseMem.alloc(
						sizeof(binlogBuf) * tableMeta->columnCount),
					tableMeta->columnCount);
				if (ParseStatus::OK
					!= (rtv = parseRowData(parsePos,
						data + rawDataSize - parsePos, tableMeta, meta,
						true, type)))
				{
					Log_r::Error("parse record failed");
					return rtv;
				}
			}
			if (parsePos > end)
			{
				Log_r::Error("parse record failed for read over the end of log event,table :%s.%s ,checkpoint:%lu@%lu", tableMeta->dbName, tableMeta->tableName, m_currentOffset, m_currentFileID);
				return ParseStatus::ILLEGAL;
			}
			m_parseMem.reset();
		}
		if (m_currentTransaction->recordCount >= MAX_BATCH)
		{
			commitCachedRecord(wrapper);
			return ParseStatus::TRANS_COMMIT;
		}
		else
			return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parseTraceID(void* rawData,
		size_t rawDataSize)
	{
		return ParseStatus::OK;
	}
	BinlogEventParser::ParseStatus BinlogEventParser::parser(const char * logEvent)
	{
		commonMysqlBinlogEventHeader_v4* header =
			static_cast<commonMysqlBinlogEventHeader_v4*>(logEvent);
		m_currentOffset = header->eventOffset;
		ParseStatus rtv = ParseStatus::OK;
		switch (header->type)
		{
		case WRITE_ROWS_EVENT:
		case WRITE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(wrapper, DATABASE_INCREASE::R_INSERT);
			break;
		case UPDATE_ROWS_EVENT:
		case UPDATE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(wrapper, DATABASE_INCREASE::R_UPDATE);
			break;
		case DELETE_ROWS_EVENT:
		case DELETE_ROWS_EVENT_V1:
			rtv = parseRowLogevent(wrapper, DATABASE_INCREASE::R_DELETE);
			break;
		case ROTATE_EVENT:
			rtv = updateFile(wrapper);
			break;
		case QUERY_EVENT:
			rtv = parseQuery(wrapper);
			break;
		case TABLE_MAP_EVENT:
			rtv = parseTableMap(wrapper);
			break;
		case ROWS_QUERY_LOG_EVENT:
			rtv = parseTraceID(wrapper->rawData, wrapper->rawDataSize);
			break;
		case FORMAT_DESCRIPTION_EVENT:
			rtv = createDescEvent(wrapper);
			break;
		case XID_EVENT:
			rtv = commit(wrapper);
			break;
		default:
			break;
		}
		if (rtv == ParseStatus::OK || rtv == ParseStatus::FILTER)
		{
			wrapper->rawData = NULL;
			wrapper->rawDataSize = 0;
			return rtv;
		}
		else if (rtv == ParseStatus::TRANS_COMMIT)
		{
			return rtv;
		}
		else
		{
			Log_r::Error("parse failed");
			return rtv;
		}
	}

}
