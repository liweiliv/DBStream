#include "dataSource.h"
#include "dataSourceConf.h"
#include "../glog/logging.h"
#include "../util/config.h"
#include "../util/file.h"
#ifdef OS_LINUX
#include <dlfcn.h>
#endif
namespace DATA_SOURCE {
#ifdef OS_LINUX
	dataSource * dataSource::loadFromDll(const char* fileName, config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store)
	{
		void* dllHandle;
		if (checkFileExist(fileName,0)!=0)
			return nullptr;
		dllHandle = dlopen(fileName, RTLD_NOW);
		if (dllHandle == NULL)
		{
			LOG(ERROR)<<"load shared lib:"<<fileName<<" failed for "<<dlerror();
			return nullptr;
		}
		dataSource * (*_instance)(config*, META::metaDataCollection*, STORE::store*) = (dataSource * (*)(config *, META::metaDataCollection *, STORE::store *)) dlsym(dllHandle, "instance");
		if (_instance == nullptr)
		{
			LOG(ERROR) << "load func "<< "[instance]" <<" from :" << fileName << " failed for " << dlerror();
			dlclose(dllHandle);
			return nullptr;
		}
		dataSource* ds = _instance(conf, metaDataCollection, store);
		if (ds == nullptr)
		{
			LOG(ERROR) << "call func " << "[instance]" << " from :" << fileName << " failed ";
			dlclose(dllHandle);
			return nullptr;
		}
		ds->m_dllHandle = dllHandle;
		return ds;
	}
#endif
#ifdef OS_WIN
	dataSource* dataSource::loadFromDll(const char* fileName, config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store)
	{
		HINSTANCE dllHandle;
		dllHandle = LoadLibrary(fileName);
		if (dllHandle == NULL)
		{
			LOG(ERROR) << "load shared lib:" << fileName << " failed for " << GetLastError() << "," << strerror(GetLastError());
			return nullptr;
		}
		dataSource* (*_instance)(config*, META::metaDataCollection*, STORE::store*) = (dataSource * (*)(config*, META::metaDataCollection*, STORE::store*)) GetProcAddress(dllHandle, "instance");
		if (_instance == nullptr)
		{
			LOG(ERROR) << "load func " << "[instance]" << " from :" << fileName << " failed  " << GetLastError() << "," << strerror(GetLastError());
			FreeLibrary(dllHandle);
			return nullptr;
		}
		dataSource* ds = _instance(conf, metaDataCollection, store);
		if (dataSource == nullptr)
		{
			LOG(ERROR) << "call func " << "[instance]" << " from :" << fileName << " failed ";
			FreeLibrary(dllHandle);
			return nullptr;
		}
		ds->m_dllHandle = dllHandle;
		return ds;
	}
#endif
	DLL_EXPORT dataSource* dataSource::loadDataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store)
	{
		std::string dataSourceType = conf->get(SECTION, DATASOURCE_TYPE);
		if (dataSourceType.empty())
		{
			LOG(ERROR) <<"conf:"<< DATASOURCE_TYPE<<" must be setted";
			return nullptr;
		}
		std::string dataSourceLibDirPath = conf->get(SECTION, DATASOURCE_LIBDIR_PATH, DATASOURCE_LIBDIR_DEFAULT_PATH);
#ifdef OS_LINUX
		std::string path = dataSourceLibDirPath.append("lib").append(dataSourceType).append("DataSource.so");
#endif
#ifdef OS_WIN
		std::string path = dataSourceLibDirPath.append(dataSourceType).append("DataSource.dll");
#endif
		dataSource* ds = loadFromDll(path.c_str(), conf, metaDataCollection, store);
		if (ds == nullptr)
		{
			LOG(ERROR) << "load dataSource from :"<<path<<" failed";
			return nullptr;
		}
		LOG(INFO) << "load dataSource from :" << path << " success";
		return ds;
	}

}
