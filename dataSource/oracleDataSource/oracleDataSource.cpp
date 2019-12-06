#include "oracleDataSource.h"
namespace DATA_SOURCE {
	oracleDataSource::oracleDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store)
	{

	}

	extern "C" DLL_EXPORT dataSource * instance(config * conf, META::metaDataCollection * metaDataCollection, STORE::store * store)
	{
		return new oracleDataSource(conf, metaDataCollection, store);
	}
}