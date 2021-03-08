#pragma once
#include "dataSource/dataSource.h"
#include "oracleBackQuery.h"
#include "oracleLogParser.h"
#include "oracleLogReader.h"
#include "oracleTransactionCache.h"
namespace DATA_SOURCE
{
	class oracleDataSource :public dataSource {
	public:
		oracleDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		DLL_EXPORT virtual bool start();
		DLL_EXPORT virtual bool stop();
		DLL_EXPORT virtual bool running() const;
		DLL_EXPORT virtual DATABASE_INCREASE::record* read();
		DLL_EXPORT virtual const char* dataSourceName() const;
	};
}
