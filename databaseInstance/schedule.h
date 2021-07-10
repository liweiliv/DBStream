#pragma once
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <atomic>
#include <mutex>
#include <queue>
#include <time.h>
#include "thread/threadPool.h"
#include "util/unblockedQueue.h"
#include "util/config.h"
#include "glog/logging.h"

namespace DB_INSTANCE {
#define COND_LEVEL 5
#define C_SCHEDULE "schedule"
#define C_MAX_WORKERS C_SCHEDULE ".maxWorders"
#define C_MAX_IDLE_ROUND C_SCHEDULE ".maxIdleRound"

#define SCHEDULE_MAX_WORKER_THREAD 256
#define SCHEDULE_TASK_LEVEL 5
	class job;
	class stream;
	class iterator;
	class Schedule
	{
		friend class threadPool<Schedule, void>;
	public:
		struct streamCmp
		{
			bool operator()(job* a, job* b) const;
		};
	private:
		std::priority_queue<job*, std::vector<job*>, streamCmp> m_task;
		std::mutex m_taskLock;
		std::atomic<uint8_t> m_activeTask;
		bool m_running;
		uint32_t m_allScore;
		uint16_t m_maxWorders;
		int m_maxIdleRound;
		Config* m_config;
		threadPool<Schedule, void> m_threadPool;
	public:
		Schedule(Config* conf) :m_running(false), m_allScore(0), m_config(conf), m_threadPool(createThreadPool(32, this, &Schedule::worker, C_SCHEDULE))
		{
			m_maxWorders = m_config->getLong("store", C_MAX_WORKERS, 8, 1, 32);
			m_maxIdleRound = m_config->getLong("store", C_MAX_IDLE_ROUND, 100, 1, 100000);
			m_threadPool.updateCurrentMaxThread(m_maxWorders);
		}
		std::string updateConfig(const char* key, const char* value);
		inline void putJobToRunning(job* j)
		{
			m_taskLock.lock();
			m_task.push(j);
			m_taskLock.unlock();
		}
		int start()
		{
			m_running = true;
			LOG(INFO) << "schedule work thread starting...";
			m_threadPool.createNewThread();
			LOG(INFO) << "schedule started";
			return 0;
		}
		int stop()
		{
			LOG(INFO) << "schedule stoping...";
			m_running = false;
			m_threadPool.join();
			while (!m_task.empty())
			{
				//todo	job* j = m_task.top();
				//	j->kill();
				m_task.pop();
			}
			LOG(INFO) << "schedule stopped";
			return 0;
		}
	private:
		inline job* getNextActiveHandle();
		void process(job* j);
		void worker();
	};
}