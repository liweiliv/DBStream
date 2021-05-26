#pragma once
#include "dataSource/dataSource.h"
namespace DATA_SOURCE
{
	class oracleIncreaceDataSource :public dataSource {
	public:
		DLL_EXPORT oracleIncreaceDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		DLL_EXPORT virtual bool start();
		DLL_EXPORT virtual bool stop();
		DLL_EXPORT virtual bool running() const;
		DLL_EXPORT virtual DATABASE_INCREASE::record* read();
		DLL_EXPORT virtual const char* dataSourceName() const;
	};
}