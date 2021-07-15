#pragma once
#include <thread>
#include "util/config.h"
#include "databaseInstance/databaseInstance.h"
#include "thread/shared_mutex.h"
#include "localLogFileCache/localLogFileCache.h"
#include "transactionCache.h"
namespace DATA_SOURCE
{
	class DataSourceParser
	{
	protected:
		Config* m_conf;
		DB_INSTANCE::DatabaseInstance* m_instance;
		LocalLogFileCache* m_localLog;
		TransactionCache* m_transCache;
		bool m_running;
		std::thread m_parseThread;
		shared_mutex m_lock;
	protected:
		virtual void run() = 0;
		virtual std::string getName() = 0;
	public:
		DataSourceParser(Config* conf, DB_INSTANCE::DatabaseInstance* instance, LocalLogFileCache* localLog):m_conf(conf), m_instance(instance), m_localLog(localLog), m_running(false)
		{}
		virtual ~DataSourceParser()
		{
		}
		virtual DS init(RPC::Checkpoint* currentCkp, RPC::Checkpoint* safeCkp) = 0;
		virtual DS start()
		{
			std::lock_guard<shared_mutex> lock(m_lock);
			if (m_running)
				dsFailedAndLogIt(1, getName() << " is started, can not start again", ERROR);
			m_parseThread = std::thread([this]()-> void {run(); });
			m_running = true;
			dsOk();
		}
		virtual DS stop()
		{
			std::lock_guard<shared_mutex> lock(m_lock);
			if (!m_running)
				dsFailedAndLogIt(1, getName() << " is stopped, can not stop again", ERROR);
			m_running = false;
			if (m_parseThread.joinable())
				m_parseThread.join();
		}
		virtual DS isRunning()
		{
			return m_running;
		}
	};
}