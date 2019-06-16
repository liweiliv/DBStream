/*
 * binlogEventParser.h
 *
 *  Created on: 2018年7月2日
 *      Author: liwei
 */

#ifndef BINLOGEVENTPARSER_H_
#define BINLOGEVENTPARSER_H_


/*
 * BinlogEventParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <stdlib.h>
#include <string.h>
#include <map>
#include "BR.h"
#include "MD.h"
#include "parseMem.h"
#include "MetaDataManager.h"
#include "MySQLTransaction.h"
#include "MempRing.h"
#include "columnParser.h"
#include "itoa.h"
#include "block_file_manager.h"
#include "BinaryLogEvent.h"
#define MAX_BATCH 32
struct tableMap;
struct arena
{
	arena *next;
	arena *end;
	uint32_t bufSize;
	uint32_t bufUsedSize;
	char *buf;
	arena(uint32_t size = 1024*1024);
	void clear();
	~arena();
	void * alloc(uint32_t size);
};
class tableMapCache
{
private:
	tableMap* maps[MAX_BATCH];
	uint32_t num;
	std::map<uint64_t,tableMap*> maps_;
	arena a;
public:
	tableMapCache();
	inline void clear();
	inline const tableMap * get(uint64_t tableID);
	inline void insert(const char *metaData_,uint32_t size,const formatEvent*
            description_event);
};
#define  BinlogRecordImplPoolCacheSize  512
class BinlogRecordImplPool
{
private:
	BinlogRecordImpl *m_cache[BinlogRecordImplPoolCacheSize];
	uint16_t m_cacheSize;
public:
	BinlogRecordImplPool();
	~BinlogRecordImplPool();
	inline BinlogRecordImpl * get();
	inline void put(BinlogRecordImpl * record);
	inline void put(BinlogRecordImpl ** record, uint32_t size);
};
class BinlogEventParser{
private:
	MetaDataManager *m_metaDataManager;
	MySQLTransaction * m_currentTransaction;
	MySQLTransaction * m_firstTransaction;
	BinlogRecordImpl * m_currentRecord;
	BinlogRecordImplPool m_recordPool;
	MySQLLogWrapper * m_currentWrapper;
	_memp_ring * m_mempoolTransaction;
	_memp_ring * m_mempoolRecord;
	formatEvent *m_descEvent ;
	uint64_t m_currentFileID;
	uint64_t m_currentOffset;
	tableMapCache m_tableMapCache;
	parseMem m_parseMem;
	BinlogFilter *m_binlogFilter;
	std::string m_instance;
	uint32_t m_threadID;
public:
	enum ParseStatus{
			OK,
			TRANS_COMMIT,
			FILTER,
			NO_META,
			META_NOT_MATCH,
			ILLEGAL
		};
private:
	struct MySQL_COLUMN_TYPE_INFO{
		uint8_t type;
		int (*parseFunc)(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
				const char *& data, char *& parsedData, uint32_t &parsedDataSize);
#ifdef COMPARE_UPDATE
		uint32_t (*lengthFunc)(IColMeta * colMeta, const uint8_t *meta,const char * data);
#endif
		uint8_t metaLength;
	};
#ifdef COMPARE_UPDATE
	const char * m_oldImageOfUpdateRow[1025];
	bool m_ifTypeNeedCompare[256];
	uint32_t m_fixedTypeSize[256];
#endif
	MySQL_COLUMN_TYPE_INFO m_columnInfo[256];
	void initColumnTypeInfo();
public:
	BinlogEventParser(BinlogFilter *binlogFilter,MetaDataManager *metaDataManager);
	~BinlogEventParser();
	void freeMem2Parser(BinlogRecordImpl * record);
	void setInstance(const char * instance);
private:
	void recycleTransaction(MySQLLogWrapper *wrapper);
	void initTransaction(uint32_t maxRecordCount);
	inline void createNewRecord(uint32_t index);
	inline void createNewRecord();
	inline const char * getQuery(const char *data,size_t size);
	ParseStatus createDescEvent(MySQLLogWrapper * wrapper);
	ParseStatus updateFile(MySQLLogWrapper * wrapper);
	ParseStatus handleDDL(BinlogRecordImpl *record);
	ParseStatus parseDDL(MySQLLogWrapper * wrapper);
	ParseStatus parseQuery(MySQLLogWrapper * wrapper);
	ParseStatus parseTableMap(MySQLLogWrapper * wrapper);
	ParseStatus commitCachedRecord(MySQLLogWrapper * wrapper);
	ParseStatus rollback(MySQLLogWrapper * wrapper);
	ParseStatus ddl(MySQLLogWrapper * wrapper,QueryEvent* ddlEvent);
	ParseStatus begin(MySQLLogWrapper * wrapper);
	ParseStatus commit(MySQLLogWrapper * wrapper);
	inline void jumpOverRowLogEventHeader(const char *& logevent, uint64_t size);
	ParseStatus parseRowData(const char *&data,size_t size,const tableMap * tableMeta,ITableMeta * meta,bool newORold,RecordType type);
	inline uint64_t getTableID(const char * data, Log_event_type event_type);
	ParseStatus parseRowLogevent(MySQLLogWrapper * wrapper,RecordType type);
	ParseStatus parseTraceID(void * rawData,size_t rawDataSize);
public:
	ParseStatus parser(MySQLLogWrapper * wrapper);
};
#endif /* BINLOGEVENTPARSER_H_ */
