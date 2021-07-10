#pragma once
#include <string>
#include <thread>
#include "dataSourceConf.h"
#include "util/winDll.h"
#include "util/status.h"
class Config;
namespace META {
	class MetaDataCollection;
}
namespace DB_INSTANCE {
	class DatabaseInstance;
}
namespace RPC {
	struct Record;
}
namespace DATA_SOURCE
{
	class LocalLogFileCache;
	class DataSourceReader;
	class DataSourceParser;
	class dataSource
	{
	protected:
		Config* m_conf;
		META::MetaDataCollection* m_metaDataCollection;
		bool m_asyncRead;
		bool m_readerAndParserIndependent;

		LocalLogFileCache* m_localFielCache;

		DataSourceReader* m_reader;
		DataSourceParser* m_parser;

		DB_INSTANCE::DatabaseInstance* m_instance;

		dsStatus m_errStatus;
		void* m_dllHandle;
		static dataSource* loadFromDll(const char* fileName, Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::DatabaseInstance* instance);
	public:
		DLL_EXPORT dataSource(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::DatabaseInstance* instance);
		DLL_EXPORT virtual ~dataSource();
		DLL_EXPORT virtual DS initByConf();
		DLL_EXPORT virtual DS start() = 0;
		DLL_EXPORT virtual DS stop() = 0;
		DLL_EXPORT virtual bool running();
		DLL_EXPORT const dsStatus& getLastError()
		{
			return m_errStatus;
		}
		DLL_EXPORT virtual DS read(RPC::Record*&) = 0;
		DLL_EXPORT virtual const char* dataSourceName() const = 0;
		DLL_EXPORT static dataSource* loadDataSource(Config* conf, META::MetaDataCollection* metaDataCollection, DB_INSTANCE::DatabaseInstance* instance);
		DLL_EXPORT static void destroyDataSource(dataSource* ds);
	};
	extern "C" DLL_EXPORT dataSource * instance(Config * conf, META::MetaDataCollection * metaDataCollection, DB_INSTANCE::DatabaseInstance * instance);
}

