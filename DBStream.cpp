#include "dataSource/dataSource.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "store/store.h"
#include "util/config.h"
#include "glog/logging.h"
#include <stdio.h>
int main(int argc, char* argv[])
{
	const char* confPath = nullptr;
	if (argc <= 1)
		confPath = "./d.cnf";
	else
		confPath = argv[1];
	config conf(confPath);
	if (conf.load() != 0)
	{
		LOG(ERROR) << "load config failed";
		return -1;
	}
	STORE::store store(&conf);
	META::metaDataCollection collection("utf8",nullptr);
	DATA_SOURCE::dataSource* ds = DATA_SOURCE::dataSource::loadDataSource(&conf, &collection, &store);
	if (ds == nullptr)
	{
		LOG(ERROR) << "load dataSource failed";
		return -1;
	}
	if (0 != store.start())
	{
		LOG(ERROR) << "start store failed";
		return -1;
	}
	if (0 != ds->start())
	{
		LOG(ERROR) << "start data source failed";
		store.stop();
		return -1;
	}
	while (likely(ds->running()))
	{
		DATABASE_INCREASE::record* record = ds->read();
		if (record != nullptr)
		{
			if (0 != store.insert(record))
			{
				LOG(ERROR) << "insert record to store failed";
				store.stop();
				ds->stop();
				return -1;
			}
		}
	}
}
