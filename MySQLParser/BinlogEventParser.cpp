/*
 * BinlogEventParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <stdlib.h>
#include <string.h>
#include <my_sys.h>
#include <m_ctype.h>
#include <mysql.h>
#include <mysql_com.h>
#include "BR.h"
#include "MD.h"
#include "parseMem.h"
#include "MetaDataManager.h"
#include "MySQLTransaction.h"
#include "MempRing.h"
#include "columnParser.h"
#include "itoa.h"
#include "block_file_manager.h"
#include "BinlogEventParser.h"
#include "Log_r.h"
#include "BinlogFilter.h"
#define MAX_BATCH 1024
#define ROWS_MAPID_OFFSET    0
#define ROWS_FLAGS_OFFSET    6
#define ROWS_VHLEN_OFFSET    8
#define ROWS_V_TAG_LEN       1
#define ROWS_V_EXTRAINFO_TAG 0
/**
 Get the length of next field.
 Change parameter to point at fieldstart.

 @param  packet pointer to a buffer containing the field in a row.
 @return pos    length of the next field
 */
static inline unsigned long get_field_length(unsigned char **packet)
{
	unsigned char *pos = *packet;
	uint32_t temp = 0;
	if (*pos < 251)
	{
		(*packet)++;
		return *pos;
	}
	if (*pos == 251)
	{
		(*packet)++;
		return ((unsigned long) ~0); //NULL_LENGTH;
	}
	if (*pos == 252)
	{
		(*packet) += 3;
		memcpy(&temp, pos + 1, 2);
		temp = le32toh(temp);
		return (unsigned long) temp;
	}
	if (*pos == 253)
	{
		(*packet) += 4;
		memcpy(&temp, pos + 1, 3);
		temp = le32toh(temp);
		return (unsigned long) temp;
	}
	(*packet) += 9; /* Must be 254 when here */
	memcpy(&temp, pos + 1, 4);
	temp = le32toh(temp);
	return (unsigned long) temp;
}
struct tableMap
{
	uint64_t tableID;
	const char * dbName;
	const char * tableName;
	uint32_t columnCount;
	const uint8_t * types;
	const uint8_t * metaInfo;
	uint32_t metaInfoSize;
	const char * metaData;
	inline void init(
			const formatEvent* description_event,
			const char * metaData_, size_t size)
	{
		assert(size > 16);
		tableID = 0;
		metaData = metaData_ + description_event->common_header_len;
		const char * pos = metaData;
		if (description_event->post_header_len[TABLE_MAP_EVENT - 1]
				== 6)
		{
			tableID = uint4korr(metaData);
			pos += 4 + 2; //4byte tableID+2byte flag
		}
		else
		{
			tableID = uint6korr(metaData);
			pos += 6 + 2; //6byte tableID+2byte flag
		}

		dbName = pos + 1;
		pos += 1 + 1 + (uint8_t) pos[0]; //1byte dbName length + dbName length +1 byte '/0' end of string
		tableName = pos + 1;
		pos += 1 + 1 + (uint8_t) pos[0]; //1byte tableName length + tableName length +1 byte '/0' end of string
		columnCount = get_field_length((uint8_t **) &pos);
		types = (const uint8_t*) pos;
		pos += columnCount;
		if (pos - metaData_ < size)
		{
			metaInfoSize = get_field_length((uint8_t **) &pos);
			assert(metaInfoSize <= columnCount * sizeof(uint16_t));
			metaInfo = (uint8_t*) pos;
		}
		else
		{
			metaInfo = NULL;
		}
	}
	tableMap(const formatEvent* description_event,
			const char * metaData_, size_t size)
	{
		init(description_event, metaData_, size);
	}
};

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
void * arena::alloc(uint32_t size)
{
	if (end->bufSize - end->bufUsedSize < size)
	{
		arena * a = new arena(size > bufSize ? size : bufSize);
		end->next = a;
	}
	char * buf = end->buf + end->bufUsedSize;
	end->bufUsedSize += size;
	return buf;
}

tableMapCache::tableMapCache()
{
	num = 0;
	memset(maps, 0, sizeof(maps));
}
void tableMapCache::clear()
{
	num = 0;
	maps_.clear();
	a.clear();
}
const tableMap * tableMapCache::get(uint64_t tableID)
{
	if (num < 5)
	{
		uint32_t idx = 0;
		for (; idx < num; idx++)
		{
			if (maps[idx]->tableID > tableID)
				break;
			else if (maps[idx]->tableID == tableID)
				return maps[idx];
		}
	}
	else
	{
		uint32_t start = 0, end = num - 1;
		for (int32_t idx = (start + end) / 2; start <= end;
				idx = (start + end) / 2)
		{
			if (maps[idx]->tableID > tableID)
				end = idx - 1;
			else if (maps[idx]->tableID < tableID)
				start = idx + 1;
			else
				return maps[idx];
		}
		if (start > 0 && start < num && maps[start]->tableID == tableID)
			return maps[start];
	}

	if (num >= MAX_BATCH)
	{
		std::map<uint64_t, tableMap*>::const_iterator iter = maps_.find(
				tableID);
		if (iter == maps_.end())
			return NULL;
		return iter->second;
	}
	else
		return NULL;
}
void tableMapCache::insert(const char *metaData_, uint32_t size,
		const formatEvent* description_event)
{
	char * metaData = (char*)a.alloc(size);
	memcpy(metaData, metaData_, size);
        tableMap * m = static_cast<tableMap*>(a.alloc(sizeof(tableMap)));
        m->init(description_event, metaData, size);
	if (num < 5)
	{
		uint32_t idx = 0;
		for (; idx < num; idx++)
		{
			if (maps[idx]->tableID > m->tableID)
				break;
			else if (maps[idx]->tableID == m->tableID)
			{
				maps[idx] = m;
				return;
			}
		}
		if(num>0)
		{
		    for (int32_t idx_ = num; idx_ > idx; idx_--)
	            maps[idx_] = maps[idx_ - 1];
		}
		maps[idx] = m;
	}
	else if (num < MAX_BATCH - 1)
	{
		int32_t start = 0, end = num - 1;
		for (int32_t idx = (start + end) / 2; start < end;
				idx = (start + end) / 2)
		{
			if (maps[idx]->tableID > m->tableID)
				end = idx - 1;
			else if (maps[idx]->tableID < m->tableID)
				start = idx + 1;
			else
			{
				maps[idx] = m;
				return;
			}
		}
		if (maps[start]->tableID < m->tableID)
			start++;
		for (int32_t idx = num ; idx > start; idx--)
			maps[idx] = maps[idx - 1];
		maps[start] = m;
	}
	else
	{
		if (!maps_.insert(std::pair<uint64_t, tableMap*>(m->tableID, m)).second)
		{
			maps_.erase(m->tableID);
			maps_.insert(std::pair<uint64_t, tableMap*>(m->tableID, m));
			return;
		}
	}
	num++;
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
BinlogRecordImplPool::BinlogRecordImplPool()
{
	memset(m_cache, 0, sizeof(m_cache));
	m_cacheSize = 0;
}
BinlogRecordImplPool::~BinlogRecordImplPool()
{
	for (int idx = 0; idx < m_cacheSize; idx++)
	{
		if (m_cache[idx])
			delete m_cache[idx];
	}
}
BinlogRecordImpl * BinlogRecordImplPool::get()
{
	if (m_cacheSize > 0)
	{
		return m_cache[--m_cacheSize];
	}
	else
	{
		for (; m_cacheSize <= (BinlogRecordImplPoolCacheSize >> 3);
				m_cacheSize++)
		{
			m_cache[m_cacheSize] = new BinlogRecordImpl(true, true);
			m_cache[m_cacheSize]->setUserData(NULL);
		}
		return m_cache[--m_cacheSize];
	}
}
void BinlogRecordImplPool::put(BinlogRecordImpl * record)
{
	if (m_cacheSize > BinlogRecordImplPoolCacheSize)
	{
		delete record;
	}
	else
	{
		record->clear();
		m_cache[m_cacheSize++] = record;
	}
}
void BinlogRecordImplPool::put(BinlogRecordImpl ** record, uint32_t size)
{
	if (size >= BinlogRecordImplPoolCacheSize - m_cacheSize)
	{
		memcpy(&m_cache[m_cacheSize], record,
				sizeof(BinlogRecordImpl*)
						* (BinlogRecordImplPoolCacheSize - m_cacheSize));
		for (int idx = BinlogRecordImplPoolCacheSize - m_cacheSize; idx < size;
				idx++)
			delete record[idx];
		m_cacheSize = BinlogRecordImplPoolCacheSize;
	}
	else
	{
		memcpy(&m_cache[m_cacheSize], record, sizeof(BinlogRecordImpl*) * size);
		m_cacheSize += size;
	}
}

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
    memset(m_fixedTypeSize,0,sizeof(m_fixedTypeSize));
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
BinlogEventParser::BinlogEventParser(BinlogFilter *binlogFilter,
		MetaDataManager *metaDataManager) :
		m_metaDataManager(metaDataManager), m_currentTransaction(NULL), m_firstTransaction(
				NULL), m_currentRecord(NULL), m_currentWrapper(NULL), m_mempoolTransaction(
				new _memp_ring), m_mempoolRecord(new _memp_ring), m_currentFileID(
				0), m_currentOffset(0), m_parseMem(m_mempoolRecord), m_binlogFilter(
				binlogFilter),m_threadID(0)
{
	m_descEvent = new formatEvent(4, "5.6.16");
	init_memp_ring(m_mempoolTransaction);
	init_memp_ring(m_mempoolRecord);
	initColumnTypeInfo();
#ifdef COMPARE_UPDATE
    memset(m_oldImageOfUpdateRow,0,sizeof(m_oldImageOfUpdateRow));
#endif
}
BinlogEventParser::~BinlogEventParser()
{
	destroy_memp_ring(m_mempoolTransaction);
	delete m_mempoolTransaction;
	destroy_memp_ring(m_mempoolRecord);
	delete m_mempoolRecord;
	if (m_descEvent != NULL)
		delete m_descEvent;
}
void BinlogEventParser::setInstance(const char * instance)
{
    m_instance = instance;
}

void BinlogEventParser::freeMem2Parser(BinlogRecordImpl * record)
{
	void * mem = record->getUserData();
	while(mem != NULL)
	{
		void * next = *(void**)mem;
		ring_free(m_mempoolRecord, mem);
		mem = next;
	}
	record->setUserData(NULL);
}
void BinlogEventParser::recycleTransaction(MySQLLogWrapper *wrapper)
{
	MySQLTransaction * recycle = wrapper->transaction;
	while (recycle)
	{
		m_recordPool.put((BinlogRecordImpl**) (&recycle->records[0]),
				recycle->recordCount);
		MySQLTransaction * tmp = recycle->next;
		ring_free(m_mempoolTransaction, recycle);
		recycle = tmp;
	}
	wrapper->transaction = NULL;
}
void BinlogEventParser::initTransaction(uint32_t maxRecordCount)
{
	if (!m_currentTransaction)
	{
		m_currentTransaction = (MySQLTransaction*) ring_alloc(
				m_mempoolTransaction,
				sizeof(MySQLTransaction)
						+ maxRecordCount * sizeof(BinlogRecordImpl*));
		m_firstTransaction = m_currentTransaction;
	}
	else
	{
		m_currentTransaction->next = (MySQLTransaction*) ring_alloc(
				m_mempoolTransaction,
				sizeof(MySQLTransaction)
						+ maxRecordCount * sizeof(BinlogRecordImpl*));
		m_currentTransaction = m_currentTransaction->next;
	}
	memset(m_currentTransaction, 0,
			sizeof(MySQLTransaction)
					+ maxRecordCount * sizeof(BinlogRecordImpl*));
}
void BinlogEventParser::createNewRecord(uint32_t index)
{
	m_currentTransaction->records[index] = m_recordPool.get();
	m_currentRecord =
			static_cast<BinlogRecordImpl*>(m_currentTransaction->records[index]);
	m_currentRecord->setInstance(m_instance.c_str());
}
void BinlogEventParser::createNewRecord()
{
	createNewRecord(m_currentTransaction->recordCount);
	m_currentTransaction->recordCount++;
}

BinlogEventParser::ParseStatus BinlogEventParser::createDescEvent(
		MySQLLogWrapper * wrapper)
{
	const char * error_msg;
	if(m_descEvent!=NULL)
		delete m_descEvent;
	m_descEvent = new formatEvent(static_cast<char*>(wrapper->rawData), wrapper->rawDataSize);
	if (m_descEvent == NULL)
	{
		Log_r::Error("ILLEGAL Format_description_log_event  event");
		return ParseStatus::ILLEGAL;
	}
	else
	{
		return ParseStatus::OK;
	}
}
BinlogEventParser::ParseStatus BinlogEventParser::updateFile(
		MySQLLogWrapper * wrapper)
{
	const char *error_msg;
	RotateEvent * tmp = new RotateEvent(
					static_cast<char*>(wrapper->rawData), wrapper->rawDataSize,m_descEvent);
	if (tmp != NULL)
	{
		if (tmp->head.timestamp == 0)
		{
			uint64_t fileID = block_file_manager::get_id(tmp->fileName);
			Log_r::Error("get new file %s %d,current file:%lu",tmp->fileName,fileID,m_currentFileID);
			if (fileID == m_currentFileID + 1||m_currentFileID==0)
			{
			   assert(fileID == wrapper->fileID);
				m_currentFileID = fileID;
				m_currentOffset = 0;
			}
			else if (fileID != m_currentFileID)
			{
				delete tmp;
				Log_r::Error(
						"binlog file id is not strict increase,current file id is %lu,new file is %s",
						m_currentFileID, tmp->fileName);
				return ParseStatus::ILLEGAL;
			}
		}
		delete tmp;
	}
	return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::handleDDL(
		BinlogRecordImpl *record)
{
	unsigned int columnCount;
	const std::string ddl = std::string(record->newCols(columnCount)[0].buf);
	const char * env;
	if (!ddl.empty())
	{
		std::string query;
		std::string dbname;
		if (record->dbname() && strlen(record->dbname()) != 0)
		{
			query.append("USE `").append(record->dbname()).append("` ;");
			dbname = record->dbname();
		}
		if (columnCount >= 2)
		{
			env = record->newCols(columnCount)[1].buf;
			query.append(env);
		}
		query.append(ddl);
		std::string encoding;
		const char *enc = record->recordEncoding();
		if (enc)
		{
			encoding = std::string(enc);
			size_t index = encoding.find("ASCII");
			if (index != std::string::npos && index != 0)
			{
				encoding = "ascii";
			}
		}
		else
		{
			encoding = std::string(DEFAULT_ENCODING);
		}

		// Parse the new pointer
		std::string ddlerror;
		int ddlerrno;
		if ((m_metaDataManager->handleDDL(dbname, ddl, env,
				record->getTimestamp(), encoding, &ddlerrno, &ddlerror, record))
				!= 0)
		{
			Log_r::Error("can't rebuild metadata using ddl:%s", query.c_str());
			return ParseStatus::OK; //todo
		}
		Log_r::Info("Rebuild metadata successfully");
	}
	return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::parseDDL(
		MySQLLogWrapper * wrapper)
{
	CHARSET_INFO *cs_info;
	const char *error_msg;
	QueryEvent * ev =
			new QueryEvent(
					static_cast<char*>(wrapper->rawData), wrapper->rawDataSize,
					 m_descEvent);
	std::string encoding;
	if (ev->charset_inited)
	{
		cs_info = get_charset(uint2korr(ev->charset), MYF(MY_WME));
		if (cs_info->csname)
			encoding = std::string(cs_info->csname);
	}
	int filtered = 0;
	int objtype = m_binlogFilter->isDDLReplicated2(std::string(ev->query),
			ev->db, encoding, &filtered);

	if (objtype == other || objtype == UnKnown || objtype == Dmlsql)
	{
		std::string completeQuery;
		if (!ev->db.empty())
			completeQuery.append("USE `").append(ev->db).append("`;");
		completeQuery.append(std::string(ev->query));
		Log_r::Notice("ignore sql %s\n", completeQuery.c_str());
		delete ev;
		return ParseStatus::OK;
	}
	if (filtered > 0)
	{
		delete ev;
		return ParseStatus::FILTER;
	}
	ParseStatus rtv = ParseStatus::OK;
	if (objtype == Begin)
		rtv = begin(wrapper);
	else if (objtype == Commit)
		rtv = commit(wrapper);
	else if (objtype == Rollback)
		rtv = rollback(wrapper);
	else
		rtv = ddl(wrapper, ev);
	delete ev;
	return rtv;
}
BinlogEventParser::ParseStatus BinlogEventParser::parseQuery(
		MySQLLogWrapper * wrapper)
{
	const char * query = NULL;
	uint32_t querySize = 0;
        if(0!=QueryEvent::getQuery(static_cast<const char*>(wrapper->rawData),
			wrapper->rawDataSize,m_descEvent,query,querySize))
		return ParseStatus::ILLEGAL;
	if (query == NULL)
		return ParseStatus::OK;
	commonMysqlBinlogEventHeader_v4 * header =
			static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
	if (strncmp(query, "BEGIN",querySize) == 0)
		return begin(wrapper);
	else if (strncmp(query, "COMMIT",querySize) == 0)
		return commit(wrapper);
	else if (strncasecmp(query, "ROLLBACK", querySize) == 0)
		return rollback(wrapper);
	else
		return parseDDL(wrapper);
}
BinlogEventParser::ParseStatus BinlogEventParser::parseTableMap(MySQLLogWrapper * wrapper)
{
	m_tableMapCache.insert(static_cast<char*>(wrapper->rawData),
			wrapper->rawDataSize, m_descEvent);
	return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::commitCachedRecord(MySQLLogWrapper * wrapper)
{
	if (m_firstTransaction != NULL)
	{
		wrapper->transaction = m_firstTransaction;
		m_currentTransaction = NULL;
		m_firstTransaction = NULL;
		return ParseStatus::TRANS_COMMIT;
	}
	else
		return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::rollback(MySQLLogWrapper * wrapper)
{
	if (m_firstTransaction == NULL || m_firstTransaction->recordCount == 0
			|| static_cast<BinlogRecordImpl*>(m_firstTransaction->records[0])->recordType()
					== EBEGIN)
	{
		/*not record in this tansaction has been write to drc queue,clean it*/
		clearMySQLTransaction(m_firstTransaction);
		m_firstTransaction = NULL;
		m_currentTransaction = NULL;
		return ParseStatus::OK;
	}
	else //some record has been write to drc queue,we have to write a EROLLBACK message
	{
		commonMysqlBinlogEventHeader_v4 * header =
				static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
		createNewRecord();
		m_currentRecord->setTimestamp(header->timestamp);
		m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
		m_currentRecord->setRecordType(EROLLBACK);
		return commitCachedRecord(wrapper);
	}
}
BinlogEventParser::ParseStatus BinlogEventParser::ddl(MySQLLogWrapper * wrapper,
		QueryEvent * ddlEvent)
{
	bool isSingleDDL = (m_firstTransaction == NULL);
	if (isSingleDDL)
	{
		initTransaction(MAX_BATCH);
	}
	createNewRecord();
	m_parseMem.init(m_currentRecord);
	char * envStr;
	uint32_t envStrAllocedSize = sizeof(DDL_SQL_MODE_HEAD_STR) + 22; // 64 bit int sqlmode max is 18446744073709551615
	envStrAllocedSize += sizeof(DDL_SQL_TIMESTAMP_HEAD_STR) + 22 + 11; // SET TIMESTAMP=%lu.%u  unsigned long max length is 22 ,unsigned int max length is 11
	envStrAllocedSize += sizeof(DDL_SQL_NAME_HEAD_STR) + 32; //max 32 byte chaset string
	envStr = m_parseMem.alloc(envStrAllocedSize);
	uint32_t envStrSize = 0;
	if (ddlEvent->tv_usec)
	{
		envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET TIMESTAMP=%ld.%06d;",
				(long) ddlEvent->head.timestamp,
				(int) ddlEvent->tv_usec);
	}
	else
	{
		envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET TIMESTAMP=%ld;",
				(long) ddlEvent->head.timestamp);
	}

	if (likely(ddlEvent->sql_mode_inited))
	{
		envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
				"SET @@session.sql_mode=%lu;", (ulong) ddlEvent->sql_mode);
	}
	if (likely(ddlEvent->charset_inited))
	{
		char *charset_p = ddlEvent->charset; // Avoid type-punning warning.
		CHARSET_INFO *cs_info = get_charset(uint2korr(charset_p), MYF(MY_WME));
		if (cs_info && cs_info->csname)
		{
		   m_currentRecord->setRecordEncoding(cs_info->csname);
			envStrSize += snprintf(envStr + envStrSize, envStrAllocedSize,
					"SET names %s;", cs_info->csname);
		}
	}
	m_currentRecord->setNewColumn(
			(binlogBuf*) m_parseMem.alloc(sizeof(binlogBuf) * 2), 2);
	m_currentRecord->putNew(ddlEvent->query.c_str(), ddlEvent->query.size());
	m_currentRecord->putNew(envStr, strlen(envStr));
	m_currentRecord->setRecordType(EDDL);
	m_currentRecord->setTimestamp(ddlEvent->head.timestamp);
	m_currentRecord->setRecordUsec(ddlEvent->tv_usec);
	m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
	m_currentRecord->setThreadId(ddlEvent->thread_id);
	if(!ddlEvent->db.empty())
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
BinlogEventParser::ParseStatus BinlogEventParser::begin(
		MySQLLogWrapper * wrapper)
{
	assert(m_firstTransaction == NULL);
	commonMysqlBinlogEventHeader_v4 * header =
			static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
	initTransaction(MAX_BATCH);
	m_threadID = le32toh(*(uint32_t*)(((const char*)wrapper->rawData)+sizeof(commonMysqlBinlogEventHeader_v4)));
	createNewRecord();
	m_currentRecord->setRecordType(EBEGIN);
	m_currentRecord->setTimestamp(header->timestamp);
	m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
	m_currentRecord->setThreadId(m_threadID);
	return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::commit(
		MySQLLogWrapper * wrapper)
{
	if (m_currentTransaction)
	{
		commonMysqlBinlogEventHeader_v4 * header =
				static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
		createNewRecord();
		m_currentRecord->setTimestamp(header->timestamp);
		m_currentRecord->setCheckpoint(m_currentFileID, m_currentOffset);
		m_currentRecord->setRecordType(ECOMMIT);
		m_currentRecord->setThreadId(m_threadID);
		commitCachedRecord(wrapper);
	}
	m_threadID = 0;
	m_tableMapCache.clear();
	return ParseStatus::TRANS_COMMIT;
}
void BinlogEventParser::jumpOverRowLogEventHeader(const char *& logevent,
		uint64_t size)
{
	const char *rowBegin = logevent;
	uint8 const common_header_len = m_descEvent->common_header_len;
	Log_event_type event_type = (Log_event_type) logevent[EVENT_TYPE_OFFSET];
	uint8 const post_header_len = m_descEvent->post_header_len[event_type - 1];

	const char *post_start = logevent + common_header_len;
	post_start += ROWS_MAPID_OFFSET;
	if (post_header_len == 6)
	{
		post_start += 4;
	}
	else
	{
		post_start += ROWS_FLAGS_OFFSET;
	}
	uint16 m_flags = uint2korr(post_start);
	post_start += 2;

	uint16 var_header_len = 0;
	if (post_header_len == ROWS_HEADER_LEN_V2)
	{
		var_header_len = uint2korr(post_start);
		assert(var_header_len >= 2);
		var_header_len -= 2;
	}
	uchar const * const var_start = (const uchar *) logevent + common_header_len
			+ post_header_len + var_header_len;
	uchar const * const ptr_width = var_start;
	uchar *ptr_after_width = (uchar*) ptr_width;
	size_t m_width = net_field_length(&ptr_after_width);
	ptr_after_width += (m_width + 7) / 8;
	if ((event_type == UPDATE_ROWS_EVENT)
			|| (event_type == UPDATE_ROWS_EVENT_V1))
	{
		ptr_after_width += (m_width + 7) / 8;
	}
	logevent = (const char *) ptr_after_width;
}
static inline uint8_t getRealType(uint8_t tableDefType,uint8_t metaType,const unsigned char * meta)
{
	switch(tableDefType)
	{
		case MYSQL_TYPE_SET:
		case MYSQL_TYPE_ENUM:
		case MYSQL_TYPE_STRING:
		{
			uint8_t type = metaType;
			if(*(uint16_t*)meta>=256)
			{
				if((meta[0]&0x30)!=0x30)
				{
					type=meta[0]|0x30;
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

BinlogEventParser::ParseStatus BinlogEventParser::parseRowData(
		const char *&data, size_t size, const tableMap * tableMeta,
		ITableMeta * meta, bool newORold,RecordType type)
{
	uint32_t metaIndex = 0;
	const uint8_t * nullBitMap = (const uint8_t*)data;
	data += BYTES_FOR_BITS(tableMeta->columnCount);
	for (uint32_t idx = 0; idx < tableMeta->columnCount; idx++)
    {
        char * parsedValue = NULL;
        uint32_t parsedValueSize = 0;
        IColMeta * columnMeta = meta->getCol(idx);
        int ctype = getRealType(tableMeta->types[idx],
                (uint8_t) columnMeta->getType(),
                tableMeta->metaInfo + metaIndex);
#ifdef COMPARE_UPDATE
        if(type==EUPDATE)
        {
            if(newORold)
            {
                bool rtv = false;
                checkUpdate(rtv,data,tableMeta,meta,idx,nullBitMap,metaIndex,ctype);
                if(rtv)
                        continue;
            }
            else
                m_oldImageOfUpdateRow[idx] = data;
        }
#endif
        if (NULL_BIT(nullBitMap, idx) == 0)
        {
			if (m_columnInfo[ctype].parseFunc == NULL)
			{
				Log_r::Error("unsupport column type %d", ctype);
				return ParseStatus::META_NOT_MATCH;
			}
			if (0
					!= m_columnInfo[ctype].parseFunc(columnMeta,
							tableMeta->metaInfo + metaIndex, &m_parseMem, data,
							parsedValue, parsedValueSize))
			{
				Log_r::Error("parse column type %d in %lu@%lu failed,table :%s.%s",
						ctype, m_currentRecord->getCheckpoint2(),
						m_currentRecord->getCheckpoint1(),
						m_currentRecord->dbname(), m_currentRecord->tbname());
				return ParseStatus::ILLEGAL;
			}
		}
		if (newORold)
			m_currentRecord->putNew(parsedValue, parsedValueSize);
		else
			m_currentRecord->putOld(parsedValue, parsedValueSize);
		metaIndex += m_columnInfo[ctype].metaLength;
	}
#ifdef COMPARE_UPDATE
	if(type==EUPDATE&&!newORold)
	    m_oldImageOfUpdateRow[tableMeta->columnCount] = data;
#endif
	return ParseStatus::OK;
}
uint64_t BinlogEventParser::getTableID(const char * data,
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
		MySQLLogWrapper * wrapper, RecordType type)
{
	const char * data = static_cast<const char*>(wrapper->rawData);
	size_t rawDataSize = wrapper->rawDataSize;
	commonMysqlBinlogEventHeader_v4 * header =
			static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);

	enum_binlog_checksum_alg alg = m_descEvent->alg;
	if (alg != BINLOG_CHECKSUM_ALG_OFF
			&& alg != BINLOG_CHECKSUM_ALG_UNDEF)
		rawDataSize = rawDataSize - BINLOG_CHECKSUM_LEN;
	jumpOverRowLogEventHeader(data, rawDataSize);

	const char * parsePos = data, *end = static_cast<const char*>(wrapper->rawData)+rawDataSize;
	uint64_t tableID = getTableID(static_cast<const char*>(wrapper->rawData),
			(Log_event_type)header->type);

	const tableMap * tableMeta = m_tableMapCache.get(tableID);
	assert(tableMeta!=NULL);
	ITableMeta * meta = m_metaDataManager->getMeta(tableMeta->dbName,
			tableMeta->tableName);
	if (meta == NULL)
	{
		Log_r::Error("can not get meta of table %s.%s from metaDataManager in %lu@%lu",
				tableMeta->dbName, tableMeta->tableName,m_currentOffset,m_currentFileID);
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
                    (binlogBuf*) m_parseMem.alloc(
                            sizeof(binlogBuf) * tableMeta->columnCount),
                    tableMeta->columnCount);
            if (0
                    != (rtv = parseRowData(parsePos,
                            data + rawDataSize - parsePos, tableMeta, meta,
                            false,type)))
            {
                Log_r::Error("parse record failed");
                return rtv;
            }
        }
		if (type == EUPDATE || type == EINSERT)
		{
			m_currentRecord->setNewColumn(
					(binlogBuf*) m_parseMem.alloc(
							sizeof(binlogBuf) * tableMeta->columnCount),
					tableMeta->columnCount);
			if (ParseStatus::OK
					!= (rtv = parseRowData(parsePos,
							data + rawDataSize - parsePos, tableMeta, meta,
							true,type)))
			{
				Log_r::Error("parse record failed");
				return rtv;
			}
		}
		if(parsePos>end)
		{
			Log_r::Error("parse record failed for read over the end of log event,table :%s.%s ,checkpoint:%lu@%lu",tableMeta->dbName,tableMeta->tableName,m_currentOffset,m_currentFileID);
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
BinlogEventParser::ParseStatus BinlogEventParser::parseTraceID(void * rawData,
		size_t rawDataSize)
{
	return ParseStatus::OK;
}
BinlogEventParser::ParseStatus BinlogEventParser::parser(MySQLLogWrapper * wrapper)
{
	recycleTransaction(wrapper);
	if (wrapper->rawData == NULL || wrapper->rawDataSize == 0)
		return ParseStatus::OK;
	commonMysqlBinlogEventHeader_v4 * header =
			static_cast<commonMysqlBinlogEventHeader_v4*>(wrapper->rawData);
	m_currentOffset = wrapper->offset;
	ParseStatus rtv = ParseStatus::OK;
	switch (header->type)
	{
	case WRITE_ROWS_EVENT:
	case WRITE_ROWS_EVENT_V1:
		rtv = parseRowLogevent(wrapper, EINSERT);
		break;
	case UPDATE_ROWS_EVENT:
	case UPDATE_ROWS_EVENT_V1:
		rtv = parseRowLogevent(wrapper, EUPDATE);
		break;
	case DELETE_ROWS_EVENT:
	case DELETE_ROWS_EVENT_V1:
		rtv = parseRowLogevent(wrapper, EDELETE);
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

