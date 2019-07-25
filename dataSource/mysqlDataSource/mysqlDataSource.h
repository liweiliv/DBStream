#pragma once
#include <stdint.h>
#include <thread>
#include <assert.h>
#include "dataSource/dataSource.h"
#include "util/ringFixedQueue.h"
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
		ringFixedQueue<DATABASE_INCREASE::record*> m_outputQueue;
		mysqlBinlogReader * m_reader;
		BinlogEventParser * m_parser;
		mysqlConnector * m_connector;
		ringBuffer* m_readerBufferPool;
		ringBuffer* m_recordBufferPool;
		DATABASE_INCREASE::record* m_prevRecord;
		void initByConf();
		void updateConf(const char *key,const char *value);
		void readThread();
		DATABASE_INCREASE::record* syncRead();
		DATABASE_INCREASE::record* asyncRead();
		static void asyncReadThread(mysqlDataSource* dataSource)
		{
			dataSource->readThread();
		}
		std::thread m_thread;


	public:
		mysqlDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		virtual bool start();
		virtual bool stop();
		virtual bool running() const;
		virtual ~mysqlDataSource();
		virtual DATABASE_INCREASE::record* read();
		virtual const char* dataSourceName() const;
	};
}
