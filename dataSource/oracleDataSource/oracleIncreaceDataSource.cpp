#include "oracleIncreaceDataSource.h"
namespace DATA_SOURCE {
	DLL_EXPORT oracleIncreaceDataSource::oracleIncreaceDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store) :dataSource(conf, metaDataCollection, store)
	{

	}
	DLL_EXPORT bool oracleIncreaceDataSource::start()
	{
		return false;
	}
	DLL_EXPORT bool oracleIncreaceDataSource::stop()
	{
		return false;
	}
	DLL_EXPORT bool oracleIncreaceDataSource::running() const
	{
		return false;
	}
	DLL_EXPORT DATABASE_INCREASE::record* oracleIncreaceDataSource::read()
	{
		return nullptr;
	}
	DLL_EXPORT const char* oracleIncreaceDataSource::dataSourceName() const
	{
		return "oracleInrease";
	}


	extern "C" DLL_EXPORT dataSource * instance(config * conf, META::metaDataCollection * metaDataCollection, STORE::store * store)
	{
		return new oracleIncreaceDataSource(conf, metaDataCollection, store);
	}
}
