/*
 * BinlogEventParser.h
 *
 *  Created on: 2018年7月2日
 *      Author: liwei
 */

#ifndef BINLOGEVENTPARSER_H_
#define BINLOGEVENTPARSER_H_

#include <stdlib.h>
#include <string.h>
#include <map>
#include "../../message/record.h"
#include "../../memory/ringBuffer.h"
#include "columnParser.h"
#include "BinaryLogEvent.h"
#define COMPARE_UPDATE
namespace SQL_PARSER {
	class sqlParser;
}
namespace DATA_SOURCE
{

#define MAX_BATCH 32
	struct tableMap;
	class BinlogEventParser {
	private:
		META::metaDataCollection* m_metaDataManager;
		formatEvent* m_descEvent;
		uint64_t m_currentFileID;
		uint64_t m_currentOffset;
		std::string m_instance;
		uint32_t m_threadID;
		SQL_PARSER::sqlParser* m_sqlParser;
		tableMap m_tableMap;
	public:
		enum ParseStatus {
			OK,
			TRANS_COMMIT,
			FILTER,
			NO_META,
			META_NOT_MATCH,
			ILLEGAL
		};
	private:
		struct MySQL_COLUMN_TYPE_INFO {
			uint8_t type;
			int (*parseFunc)(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record, const char*& data,bool newOrOld);
#ifdef COMPARE_UPDATE
			uint32_t(*lengthFunc)(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record, const char* data);
#endif
			uint8_t metaLength;
		};
#ifdef COMPARE_UPDATE
		const char* m_oldImageOfUpdateRow[1025];
		bool m_ifTypeNeedCompare[256];
		uint32_t m_fixedTypeSize[256];
#endif
		MySQL_COLUMN_TYPE_INFO m_columnInfo[256];
		void initColumnTypeInfo();
	public:
		BinlogEventParser(META::metaDataCollection * metaDataManager);
		~BinlogEventParser();
		void setInstance(const char* instance);
	private:
		inline void createNewRecord(uint32_t index);
		inline void createNewRecord();
		inline const char* getQuery(const char* logEvent, size_t size);
		ParseStatus createDescEvent(const char* logEvent, size_t size);
		ParseStatus updateFile(const char* logEvent, size_t size);
		ParseStatus handleDDL(const char* logEvent, size_t size);
		ParseStatus parseDDL(const char* logEvent, size_t size);
		ParseStatus parseQuery(const char* logEvent, size_t size);
		ParseStatus parseTableMap(const char* logEvent, size_t size);
		ParseStatus commitCachedRecord(const char* logEvent);
		ParseStatus rollback(const char* logEvent, size_t size);
		ParseStatus ddl(const char* logEvent, size_t size);
		ParseStatus begin(const char* logEvent, size_t size);
		ParseStatus commit(const char* logEvent, size_t size);
		inline void jumpOverRowLogEventHeader(const char*& logevent, uint64_t size);
		ParseStatus parseRowData(DATABASE_INCREASE::DMLRecord* record, const char*& data, size_t size, bool newORold);
		inline uint64_t getTableID(const char* data, Log_event_type event_type);
		ParseStatus parseRowLogevent(const char* logEvent, DATABASE_INCREASE::RecordType type);
		ParseStatus parseTraceID(void* rawData, size_t rawDataSize);
	public:
		ParseStatus parser(const char * logEvent);
	};
}
#endif /* BINLOGEVENTPARSER_H_ */

