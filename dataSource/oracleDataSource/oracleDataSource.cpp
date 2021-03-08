#include "oracleDataSource.h"
namespace DATA_SOURCE {
	oracleDataSource::oracleDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store):dataSource(conf,metaDataCollection,store)
	{

	}
	DLL_EXPORT bool oracleDataSource::start()
	{
		return false;
	}
	DLL_EXPORT bool oracleDataSource::stop()
	{
		return false;
	}
	DLL_EXPORT bool oracleDataSource::running() const
	{
		return false;
	}
	DLL_EXPORT DATABASE_INCREASE::record* oracleDataSource::read()
	{
		return nullptr;
	}
	DLL_EXPORT const char* oracleDataSource::dataSourceName() const
	{
		return "oracleInrease";
	}


	extern "C" DLL_EXPORT dataSource * instance(config * conf, META::metaDataCollection * metaDataCollection, STORE::store * store)
	{
		return new oracleDataSource(conf, metaDataCollection, store);
	}
}
