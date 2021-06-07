#pragma once
#include <string>
#include <thread>
#include "dataSourceConf.h"
#include "util/winDll.h"
#include "util/status.h"
class config;
namespace META {
	class metaDataCollection;
}
namespace STORE {
	class store;
}
namespace DATABASE_INCREASE {
	struct record;
}
namespace DATA_SOURCE
{
	class localLogFileCache;
	class dataSource
	{
	protected:
		config* m_conf;
		META::metaDataCollection* m_metaDataCollection;
		bool m_asyncRead;
		bool m_readerAndParserIndependent;

		localLogFileCache* m_localFielCache;

		bool m_parserStatus;
		bool m_readerStatus;
		STORE::store* m_store;

		std::thread m_readThread;
		bool m_readThreadIsRunning;
		dsStatus m_readThreadErrStatus;
		std::thread m_parserThread;
		dsStatus m_parserThreadErrStatus;
		bool m_parserThreadIsRunning;

		dsStatus m_errStatus;
		void* m_dllHandle;
		static dataSource* loadFromDll(const char* fileName, config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
	public:
		DLL_EXPORT dataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		DLL_EXPORT virtual ~dataSource();
		DLL_EXPORT virtual DS initByConf();
		DLL_EXPORT virtual DS start() = 0;
		DLL_EXPORT virtual DS stop() = 0;
		DLL_EXPORT virtual bool running();
		DLL_EXPORT const dsStatus& getLastError()
		{
			return m_errStatus;
		}
		DLL_EXPORT virtual DS read(DATABASE_INCREASE::record*&) = 0;
		DLL_EXPORT virtual const char* dataSourceName() const = 0;
		DLL_EXPORT static dataSource* loadDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
		DLL_EXPORT static void destroyDataSource(dataSource* ds);
	};
	extern "C" DLL_EXPORT dataSource * instance(config * conf, META::metaDataCollection * metaDataCollection, STORE::store * store);
}

