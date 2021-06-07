#pragma once
#include "dataSource/dataSource.h"
namespace DATA_SOURCE
{
	class oracleIncreaceDataSource :public dataSource {
	public:
		DLL_EXPORT oracleIncreaceDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		DLL_EXPORT virtual ~oracleIncreaceDataSource() {}
		DLL_EXPORT virtual const char* dataSourceName() const;
	};
}