#pragma once
#include <thread>
#include "util/config.h"
#include "glog/logging.h"
#include "util/winDll.h"
class bufferPool;
class bufferBaseAllocer;
namespace META {
	class MetaDataCollection;
}
namespace RPC {
	struct Record;
}
namespace DATABASE {
	class Database;
}
namespace DB_INSTANCE {
	class Schedule;
	DLL_EXPORT class DatabaseInstance {
	private:
		Schedule* m_schedule;
		DATABASE::Database* m_mainStream;
		DATABASE::Database* m_genratedStream;
		META::MetaDataCollection* m_metaDataCollection;
		Config* m_conf;
		bufferPool* m_bufferPool;
		bufferBaseAllocer* m_allocer;
		static void highMemUsageCallback(void* handle, uint16_t usage);
		std::thread m_monitorThread;
		static void monitor(DatabaseInstance* s);
	public:
		DLL_EXPORT DatabaseInstance(Config* conf);
		DLL_EXPORT ~DatabaseInstance();

		DLL_EXPORT int start();
		DLL_EXPORT int stop();
		DLL_EXPORT void begin();
		DLL_EXPORT int insert(RPC::Record* r);
		DLL_EXPORT void commit();
		DLL_EXPORT bool checkpoint(uint64_t& timestamp, uint64_t& logOffset);
		DLL_EXPORT std::string updateConfig(const char* key, const char* value);
	};
}