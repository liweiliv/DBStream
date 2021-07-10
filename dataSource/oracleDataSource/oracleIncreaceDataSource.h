#pragma once
#include "dataSource/dataSource.h"
namespace DATA_SOURCE
{
	class oracleIncreaceDataSource :public dataSource {
	public:
		DLL_EXPORT oracleIncreaceDataSource(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::store* store);
		DLL_EXPORT virtual ~oracleIncreaceDataSource() {}
		DLL_EXPORT virtual const char* dataSourceName() const;
	};
}