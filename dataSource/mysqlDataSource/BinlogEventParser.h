/*
 * BinlogEventParser.h
 *
 *  Created on: 2018年7月2日
 *      Author: liwei
 */

#ifndef BINLOGEVENTPARSER_H_
#define BINLOGEVENTPARSER_H_
#include <stdint.h>
#include <stdlib.h>
#include <map>
#include "util/String.h"
#include "util/status.h"
#include "message/record.h"
#include "memory/ringBuffer.h"
#include "columnParser.h"
#include "BinaryLogEvent.h"
namespace SQL_PARSER {
	class sqlParser;
}
namespace META{
	class metaDataCollection;
}
namespace DATA_SOURCE
{
	static constexpr uint16_t M_PARSER_RECORD_CACHE_VOLUMN = 2048;
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
		ringBuffer* m_memPool;
		DATABASE_INCREASE::record** m_parsedRecords;
		int16_t m_parsedRecordCount;
		int16_t m_parsedRecordBegin;
		String m_error;
	public:
		enum ParseStatus {
			OK,
			BEGIN,
			COMMIT,
			FILTER,
			NO_META,
			META_NOT_MATCH,
			ILLEGAL,
			REMAIND_RECORD_UNREAD
		};
	private:
		struct MySQL_COLUMN_TYPE_INFO {
			uint8_t type;
			int (*parseFunc)(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record, const char*& data,bool newOrOld);
			uint8_t metaLength;
		};
		MySQL_COLUMN_TYPE_INFO m_columnInfo[256];
		void initColumnTypeInfo();
	public:
		BinlogEventParser(META::metaDataCollection * metaDataManager, ringBuffer* memPool);
		~BinlogEventParser();
		void setInstance(const char* instance);
	private:
		DS createDescEvent(const char* logEvent, size_t size);
		DS updateFile(const char* logEvent, size_t size);
		DS parseDDL(const char* logEvent, size_t size);
		DS parseQuery(const char* logEvent, size_t size);
		DS parseTableMap(const char* logEvent, size_t size);
		DS rollback(const char* logEvent, size_t size);
		DS ddl(const char* logEvent, size_t size);
		DS begin(const char* logEvent, size_t size);
		DS commit(const char* logEvent, size_t size);
		void parseRowLogEventHeader(const char*& logevent, uint64_t size, uint64_t& tableId, const uint8_t*& columnBitMap, const uint8_t*& updatedColumnBitMap);
		DS parseRowData(DATABASE_INCREASE::DMLRecord* record, const char*& data, size_t size, bool newORold,const uint8_t* columnBitmap);
		inline uint64_t getTableID(const char* data, Log_event_type event_type);
		DS parseRowLogevent(const char* logEvent, size_t size,DATABASE_INCREASE::RecordType type);
		DS parseUpdateRowLogevent(const char* logEvent, size_t size);
		DS parseTraceID(const char* rawData, size_t size);
	public:
		inline  DATABASE_INCREASE::record*  getRecord()
		{
			if (likely(m_parsedRecordBegin<m_parsedRecordCount))
				return m_parsedRecords[m_parsedRecordBegin++];
			else
				return nullptr;
		}
		DS parser(const char * logEvent,size_t size);
		DS init(const char * sqlParserFuncLibPath,const char * sqlParserTreePath);
	};
}
#endif /* BINLOGEVENTPARSER_H_ */

