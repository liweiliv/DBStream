#pragma once
#include<string>
#include "dataSourceConf.h"
#include "../util/winDll.h"
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

	class dataSource
	{
	protected:
		config* m_conf;
		META::metaDataCollection* m_metaDataCollection;
		STORE::store* m_store;
		std::string m_lastError;
#ifdef OS_WIN
		HINSTANCE m_dllHandle;
#else
		void* m_dllHandle;
#endif
		static dataSource* loadFromDll(const char* fileName, config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
	public:
		DLL_EXPORT dataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store) :m_conf(conf), m_metaDataCollection(metaDataCollection), m_store(store)
		{}
		DLL_EXPORT virtual ~dataSource()
		{}
		DLL_EXPORT virtual bool start() = 0;
		DLL_EXPORT virtual bool stop() = 0;
		DLL_EXPORT virtual bool running() const = 0;
		DLL_EXPORT std::string getLastError()
		{
			return m_lastError;
		}
		DLL_EXPORT virtual DATABASE_INCREASE::record* read()=0;
		DLL_EXPORT virtual const char* dataSourceName() const = 0;
		DLL_EXPORT static dataSource* loadDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
	};
	extern "C" DLL_EXPORT dataSource* instance(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store);
}

