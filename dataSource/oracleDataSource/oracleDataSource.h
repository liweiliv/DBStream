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
	};
}