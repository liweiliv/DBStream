#include "dataSource/dataSource.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "store/store.h"
#include "util/config.h"
#include "glog/logging.h"
#include "sqlParser/sqlParserUtil.h"
#include "globalInfo/global.h"
#include <stdio.h>
#include <signal.h>
#ifdef OS_WIN
#define mysqlFuncLib "mysqlParserFuncs.dll"
#define mysqlParserTree "..\\..\\..\\..\\sqlParser\\ParseTree"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
#define mysqlParserTree "sqlParser/ParseTree"
#endif

static bool running;
static DATA_SOURCE::dataSource* ds = nullptr;
static void sigHandler(int s)
{
	if(s == 15)
	{
		LOG(WARNING)<<"receive signal:"<<s<<",exit";
		running = false;
		if(ds != nullptr)
			ds->stop();
	}
}
int main(int argc, char* argv[])
{
#ifdef OS_WIN
	SetDllDirectory(".\\lib\\");
#endif
	FLAGS_minloglevel = google::GLOG_INFO;
	google::InitGoogleLogging(argv[0]);
	const char* confPath = nullptr;
	if (argc <= 1)
		confPath = "d.cnf";
	else
		confPath = argv[1];
	config conf(confPath);
	if (conf.load() != 0)
	{
		LOG(ERROR) << "load config failed";
		google::ShutdownGoogleLogging();
		return -1;
	}
	initKeyWords();
	GLOBAL::init();
	STORE::store store(&conf);
	META::metaDataCollection collection("utf8",true,nullptr);
	if(0!=collection.initSqlParser(mysqlParserTree,mysqlFuncLib))
	{
		LOG(ERROR)<<"init sqlparser failed";
		return -1;
	}
	if ((ds = DATA_SOURCE::dataSource::loadDataSource(&conf, &collection, &store)) == nullptr)
	{
		LOG(ERROR) << "load dataSource failed";
		google::ShutdownGoogleLogging();
		return -1;
	}
	LOG(INFO) << "dataSource load success";
	if (0 != store.start())
	{
		LOG(ERROR) << "start store failed";
		google::ShutdownGoogleLogging();
		return -1;
	}
	LOG(INFO) << "store started";
	if (!ds->start())
	{
		LOG(ERROR) << "start data source failed";
		store.stop();
		google::ShutdownGoogleLogging();
		return -1;
	}
	LOG(INFO) << "dataSource started";
	running = true;

	signal(15,sigHandler);
	while (likely(ds->running()&&running))
	{
		DATABASE_INCREASE::record* record = ds->read();
		if (record != nullptr)
		{
			if (0 != store.insert(record))
			{
				LOG(ERROR) << "insert record to store failed";
				break;
			}
		}
		else if(!ds->getLastError().empty())
		{
			LOG(ERROR) << "read record from dataSource failed";
			break;
		}
	}
	ds->stop();
	store.stop();
	DATA_SOURCE::dataSource::destroyDataSource(ds);
	google::ShutdownGoogleLogging();
	GLOBAL::close();
	return 0;
}
