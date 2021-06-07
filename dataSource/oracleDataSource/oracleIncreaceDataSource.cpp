#include "oracleIncreaceDataSource.h"
#include "util/config.h"
#include "oracleConfig.h"
#include "xstream/OracleXStreamDataSource.h"
namespace DATA_SOURCE {
	DLL_EXPORT oracleIncreaceDataSource::oracleIncreaceDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store) :dataSource(conf, metaDataCollection, store)
	{

	}
	DLL_EXPORT const char* oracleIncreaceDataSource::dataSourceName() const
	{
		return "oracleInrease";
	}


	extern "C" DLL_EXPORT dataSource * instance(config * conf, META::metaDataCollection * metaDataCollection, STORE::store * store)
	{
		std::string readerType = conf->get(SECTION, READ_TYPE, LOGMINER);
		if (readerType.compare(LOGMINER) == 0)
		{
			return nullptr;
		}
		else if (readerType.compare(XSTREAM) == 0)
		{
			return new oracleXStreamLogReader(conf, metaDataCollection, store);
		}
		else
		{
			LOG(ERROR) << "invalid " << READ_TYPE << ":" << readerType;
			return nullptr;
		}
	}
}
