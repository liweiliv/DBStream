#include "oracleIncreaceDataSource.h"
#include "util/config.h"
#include "oracleConfig.h"
#include "xstream/OracleXStreamDataSource.h"
namespace DATA_SOURCE {
	DLL_EXPORT oracleIncreaceDataSource::oracleIncreaceDataSource(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::DatabaseInstance* instance) :dataSource(conf, metaDataCollection, instance)
	{

	}
	DLL_EXPORT const char* oracleIncreaceDataSource::dataSourceName() const
	{
		return "oracleInrease";
	}


	extern "C" DLL_EXPORT dataSource * instance(Config * conf, META::metaDataCollection * metaDataCollection, DB_INSTANCE::DatabaseInstance * instance)
	{
		std::string readerType = conf->get(SECTION, READ_TYPE, LOGMINER);
		if (readerType.compare(LOGMINER) == 0)
		{
			return nullptr;
		}
		else if (readerType.compare(XSTREAM) == 0)
		{
			return new oracleXStreamLogReader(conf, metaDataCollection, instance);
		}
		else
		{
			LOG(ERROR) << "invalid " << READ_TYPE << ":" << readerType;
			return nullptr;
		}
	}
}
