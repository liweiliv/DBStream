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
		arrayQueue<RPC::Record*> m_outputQueue;
		mysqlBinlogReader * m_reader;
		BinlogEventParser * m_parser;
		mysqlConnector * m_connector;
		ringBuffer* m_recordBufferPool;
		RPC::Record* m_currentRecord;
		DS initByConf();
		void updateConf(const char *key,const char *value);
		void readThread();
		RPC::Record* syncRead();
		RPC::Record* asyncRead();
		std::thread m_readerThread;
	public:
		mysqlDataSource(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::store* store);
		virtual DS start();
		virtual DS stop();
		virtual bool running() const;
		virtual ~mysqlDataSource();
		virtual DS read(RPC::Record*&);
		virtual const char* dataSourceName() const;
	};
}
