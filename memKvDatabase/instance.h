#pragma once
#include <string>
#include <atomic>
#include "util/status.h"
#include "util/sparsepp/spp.h"
#include "clientHandel.h"
#include "thread/shared_mutex.h"
class bufferPool;
namespace KVDB {
	class database;
	class temporaryTables;
	typedef spp::sparse_hash_map<std::string, database*> dbMap;
	class instance
	{
	private:
		shared_mutex m_lock;
		bool m_needLogin;
		int m_maxConnection;
		std::atomic_uint64_t m_tid;
		dbMap m_dbMap;
		bufferPool* m_pool;
		bool m_caseSensitive;
	public:
		dsStatus& startTrans(clientHandle* handle);
		dsStatus& rowChange(clientHandle* handle);
		dsStatus& select(clientHandle* handle);
		dsStatus& ddl(clientHandle* handle);
		dsStatus& commit(clientHandle* handle);
		dsStatus& rollback(clientHandle* handle);
	};
}