#pragma once
#include <string>
#include <atomic>
#include "util/status.h"
#include "util/arrayQueue.h"
#include "util/sparsepp/spp.h"
#include "clientHandle.h"
#include "thread/shared_mutex.h"
class bufferPool;
namespace KVDB {
	class database;
	class temporaryTables;
	class walLogWriter;
	class service;
	typedef spp::sparse_hash_map<std::string, database*> dbMap;
	class instance
	{
	private:
		shared_mutex m_lock;
		bool m_needLogin;
		int m_maxConnection;
		std::atomic_uint64_t m_tid;
		dbMap m_dbMap;
		walLogWriter* m_walLogWriter;
		bufferPool* m_pool;
		service* m_service;
		bool m_running;
		bool m_caseSensitive;
		dsStatus m_error;
		arrayQueue<clientHandle*> m_walTask;
		arrayQueue<clientHandle*> m_walFinishTask;
		arrayQueue<clientHandle*>* m_inputTasks;
		int m_workThreadCount;
	public:
		DS startTrans(clientHandle* handle);
		DS rowChange(clientHandle* handle);
		DS select(clientHandle* handle);
		DS ddl(clientHandle* handle);
		DS commit(clientHandle* handle);
		DS rollback(clientHandle* handle);
		DS asyncWriteWalLog(clientHandle* handle);
		void workThread(int tid);
		void walWriteThread();

	};
}