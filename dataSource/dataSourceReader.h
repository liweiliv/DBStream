#pragma once
#include <thread>
#include "util/config.h"
#include "thread/shared_mutex.h"
#include "localLogFileCache/localLogFileCache.h"
namespace DATA_SOURCE
{
	class DataSourceReader
	{
	protected:
		LocalLogFileCache* m_localLog;
		Config* m_conf;
		bool m_isStream;
		bool m_running;
		std::thread m_readThread;
		shared_mutex m_lock;
		dsStatus m_error;
	protected:
		virtual void run() = 0;
		virtual std::string getName() = 0;
	public:
		DataSourceReader(LocalLogFileCache * localLog, Config* conf):m_localLog(localLog),m_conf(conf)
		{
		}
		virtual ~DataSourceReader() {}
		virtual DS init(RPC::Checkpoint* currentCkp, RPC::Checkpoint* safeCkp) = 0;
		virtual DS start()
		{
			std::lock_guard<shared_mutex> lock(m_lock);
			if (m_running)
				dsFailedAndLogIt(1, getName() << " is started, can not start again", ERROR);
			m_readThread = std::thread([this]()-> void {run();});
			m_running = true;
			dsOk();
		}
		virtual DS stop()
		{
			std::lock_guard<shared_mutex> lock(m_lock);
			if(!m_running)
				dsFailedAndLogIt(1, getName() << " is stopped, can not stop again", ERROR);
			m_running = false;
			if (m_readThread.joinable())
				m_readThread.join();
		}

		virtual DS isRunning()
		{
			return m_running;
		}

		virtual dsStatus& getError()
		{
			return m_error;
		}
	};
}