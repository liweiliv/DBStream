#include "mysqlDataSource.h"
#include "mysqlConnector.h"
#include "mysqlBinlogReader.h"
#include "BinlogEventParser.h"
#include "glog/logging.h"
#include "memory/ringBuffer.h"
#include "store/store.h"
#include "mysqlRecordOffset.h"
#include "initMetaData.h"
#include <thread>
#include "util/valgrindTestUtil.h"
namespace DATA_SOURCE {
#ifdef OS_WIN
#define mysqlFuncLib "mysqlParserFuncs.dll"
#define mysqlParserTree "..\\..\\..\\..\\sqlParser\\ParseTree"
#endif
#ifdef OS_LINUX
	#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
	#define mysqlParserTree "sqlParser/ParseTree"
#endif

	static  DATABASE_INCREASE::record  BEGIN_RECORD;
	static  DATABASE_INCREASE::record * BEGIN_RECORD_PTR = &BEGIN_RECORD;

	static  DATABASE_INCREASE::record  COMMIT_RECORD;
	static  DATABASE_INCREASE::record * COMMIT_RECORD_PTR = &COMMIT_RECORD;


	static constexpr auto ASYNC = "async";
	#define asyncPushRecord(r)	while (unlikely(!m_outputQueue.pushWithCond((r), 1000)))\
	{\
		if (unlikely(!m_running))\
			break;\
	}
	void mysqlDataSource::initByConf()
	{
		std::string rtv = m_connector->initByConf();
		if (!rtv.empty())
		{
			LOG(ERROR) << rtv;
		}
	}
	void mysqlDataSource::updateConf(const char* key, const char* value)
	{

	}
	mysqlDataSource::mysqlDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store):dataSource(conf,metaDataCollection,store),m_async(false),m_running(false)
	{
		m_recordBufferPool = new ringBuffer();
		m_connector = new mysqlConnector(conf);
		m_metaDataCollection = metaDataCollection;
		m_store = store;
		m_reader = new mysqlBinlogReader(m_connector);
		m_parser = new BinlogEventParser(m_metaDataCollection, m_recordBufferPool);
		m_currentRecord = nullptr;
	}
	mysqlDataSource::~mysqlDataSource()
	{
		delete m_reader;
		delete m_parser;
		delete m_connector;
		delete m_recordBufferPool;
	}
	void mysqlDataSource::readThread()
	{
		DS parseStatus;
		while (likely(m_running))
		{
			const char* logEvent;
			size_t size;
			if(unlikely(m_reader->readBinlog(logEvent, size)!= mysqlBinlogReader::READ_OK|| logEvent == nullptr))
				goto FAILED;
			if (!dsCheck(parseStatus = m_parser->parser(logEvent, size)))
			{
				m_errStatus = getLocalStatus();
				goto FAILED;
			}
			switch (parseStatus)
			{
			case BinlogEventParser::ParseStatus::OK:
			{
				DATABASE_INCREASE::record* record;
				do {
					if (nullptr != (record = m_parser->getRecord()))
					{
						asyncPushRecord(record);
					}
					else
						break;
				} while (likely(m_running));
				break;
			}
			case BinlogEventParser::ParseStatus::FILTER:
				break;
			case BinlogEventParser::ParseStatus::BEGIN:
				asyncPushRecord(BEGIN_RECORD_PTR);
				break;
			case BinlogEventParser::ParseStatus::COMMIT:
				asyncPushRecord(COMMIT_RECORD_PTR);
				break;
			default:
				m_lastError = m_parser->getError();
				goto FAILED;
			}
		}
	FAILED:
		m_running = false;
		return;
	}
	DATABASE_INCREASE::record* mysqlDataSource::asyncRead()
	{
		static int i = 0;
		DATABASE_INCREASE::record* record = nullptr;
		do {
			if (m_outputQueue.popWithCond(record, 1000))
			{
				if (record == BEGIN_RECORD_PTR)
					m_store->begin();
				else if (record == COMMIT_RECORD_PTR)
					m_store->commit();
				else
				{
					if((++i&0xffff) == 0)
						LOG(ERROR)<<record->head->logOffset;
					return record;
				}
			}
		} while (likely(m_running));
		return nullptr;
	}
	DATABASE_INCREASE::record* mysqlDataSource::syncRead()
	{
		DATABASE_INCREASE::record* record;
		if (nullptr != (record = m_parser->getRecord()))
			return record;
		const char* logEvent = nullptr;
		size_t size = 0;
		BinlogEventParser::ParseStatus parseStatus;
		do {
			if (unlikely(m_reader->readBinlog(logEvent, size) != mysqlBinlogReader::READ_OK || logEvent == nullptr))
				return nullptr;
			parseStatus = m_parser->parser(logEvent, size);
			switch (parseStatus)
			{
			case BinlogEventParser::ParseStatus::OK:
			{
				if (nullptr != (record = m_parser->getRecord()))
					return record;
				break;
			}
			case BinlogEventParser::ParseStatus::FILTER:
				break;
			case BinlogEventParser::ParseStatus::BEGIN:
				m_store->begin();
				break;
			case BinlogEventParser::ParseStatus::COMMIT:
				m_store->commit();
				break;
			default:
				m_lastError = m_parser->getError();
				return nullptr;
			}
		} while (likely(m_running));
		return nullptr;
	}
	DATABASE_INCREASE::record* mysqlDataSource::read()
	{
		if (likely(m_currentRecord != nullptr))
			m_recordBufferPool->freeMem(m_currentRecord);
		m_currentRecord = m_async ? asyncRead() : syncRead();
		if (m_currentRecord == nullptr)
			return nullptr;
#if 0
		if(m_currentRecord->head->minHead.type>=4)
			vSave(m_currentRecord,m_currentRecord->head->minHead.size+sizeof(DATABASE_INCREASE::DMLRecord));
		else
			vSave(m_currentRecord,m_currentRecord->head->minHead.size+sizeof(DATABASE_INCREASE::DDLRecord));
#endif
		return m_currentRecord;
	}
	const char* mysqlDataSource::dataSourceName() const
	{
		return NAME;
	}
	DS mysqlDataSource::start()
	{
		std::string rtv = m_connector->initByConf();
		if (!rtv.empty())
			dsFailedAndLogIt(1, "mysqlDataSource init from conf failed for:" << rtv, ERROR);
		uint64_t timestamp = 0, logOffset = 0;
		if (!m_store->checkpoint(timestamp, logOffset))
		{
			timestamp = m_conf->getLong(SECTION, std::string(CHECKPOINT_SECTION).append(START_TIMESTAMP).c_str(), 0, 0, 0x0fffffffffffffffull);
			logOffset = m_conf->getLong(SECTION, std::string(CHECKPOINT_SECTION).append(START_LOGPOSITION).c_str(), 0, 0, 0x0fffffffffffffffull);
		}
		else
		{
			META::timestamp t;
			t.time = timestamp;
			timestamp = t.seconds;
		}
		if (logOffset > 0)
		{
			if (mysqlBinlogReader::READ_OK != m_reader->seekBinlogByCheckpoint(fileId(logOffset), offsetInFile(logOffset)))
			{
				dsFailedAndLogIt(1, "mysql datasource init from binlog position:" << offsetInFile(logOffset) << "@" << fileId(logOffset) << " failed", ERROR);
			}
		}
		else if (timestamp > 0)
		{
			if (mysqlBinlogReader::READ_OK != m_reader->seekBinlogByTimestamp(timestamp))
			{
				dsFailedAndLogIt(1, "mysql datasource init from timestamp:" << timestamp << " failed", ERROR);
			}
		}
		else
		{
			dsFailedAndLogIt(1, "can not find nether " << START_TIMESTAMP << " or " << START_LOGPOSITION << " in config", ERROR);
		}
		initMetaData imd(m_connector);
		std::vector<std::string> databases;
		if (0 != imd.getAllUserDatabases(databases))
		{
			dsFailedAndLogIt(1, "get database list from mysql server failed", ERROR);
		}
		if (0 != imd.loadMeta(m_metaDataCollection, databases))
		{
			dsFailedAndLogIt(1, "load meta from mysql server failed", ERROR);
		}
		m_metaDataCollection->print();
		m_running = true;
		if (m_conf->get(SECTION, ASYNC, "false").compare("true") == 0)
		{
			m_async = true;
			m_readerThread = std::thread(asyncReadThread, this);
		}
		else
			m_async = false;
		if(0 != m_parser->init(mysqlFuncLib,mysqlParserTree))
		{
			dsFailedAndLogIt(1, "init binlog parser failed", ERROR);
		}
		dsOk();
	}
	DS mysqlDataSource::stop()
	{
		m_running = false;
		if (m_async)
			m_readerThread.join();
		dsOk();
	}
	bool mysqlDataSource::running()const
	{
		return m_running;
	}
	extern "C" DLL_EXPORT dataSource* instance(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store)
	{
		return new mysqlDataSource(conf,metaDataCollection,store);
	}

}
