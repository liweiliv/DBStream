#pragma once
#include <stdint.h>
#include <thread>
#include <assert.h>
#include "dataSource/dataSource.h"
#include "util/arrayQueue.h"
#include <stdio.h>
class ringBuffer;
namespace DATA_SOURCE
{
	class mysqlBinlogReader;
	class BinlogEventParser;
	class mysqlConnector;
	class mysqlDataSource :public dataSource {
	private:
		static constexpr auto NAME = "mysql";
		bool m_async;
		bool m_running;
		arrayQueue<DATABASE_INCREASE::record*> m_outputQueue;
		mysqlBinlogReader * m_reader;
		BinlogEventParser * m_parser;
		mysqlConnector * m_connector;
		ringBuffer* m_recordBufferPool;
		DATABASE_INCREASE::record* m_currentRecord;
		void initByConf();
		void updateConf(const char *key,const char *value);
		void readThread();
		DATABASE_INCREASE::record* syncRead();
		DATABASE_INCREASE::record* asyncRead();
		std::thread m_readerThread;
	public:
		mysqlDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		virtual DS start();
		virtual DS stop();
		virtual bool running() const;
		virtual ~mysqlDataSource();
		virtual DS read(DATABASE_INCREASE::record*&);
		virtual const char* dataSourceName() const;
	};
}
