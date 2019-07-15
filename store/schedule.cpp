#include "schedule.h"
#include "stream.h"
#include "iterator.h"
namespace STORE {
	bool schedule::streamCmp::operator()(job* a, job* b) const
	{
		return a->m_vtime < b->m_vtime;
	}
	std::string schedule::updateConfig(const char* key, const char* value)
	{
		if (strcmp(key, C_MAX_WORKERS) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if (value[idx] > '9' || value[idx] < '0')
					return "value of config :maxWorders must be a number";
			}
			if (atol(value) > SCHEDULE_MAX_WORKER_THREAD)
			{
				char numBuf[32] = { 0 };
				sprintf(numBuf, "%d", SCHEDULE_MAX_WORKER_THREAD);
				return std::string("value of config :maxWorders is too large,max is ") + numBuf;
			}
			if (atol(value) <= 0)
			{
				return std::string("value of config :maxWorders must greater than 0 ");
			}
			m_maxWorders = atoi(value);
		}
		else if (strcmp(key, C_MAX_IDLE_ROUND) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if (value[idx] > '9' || value[idx] < '0')
					return "value of config :maxIdleRound must be a number";
			}
			m_maxIdleRound = atoi(value);
		}
		else
			return std::string("unknown config:") + key;
		m_config->set("store", key, value);
		return std::string("update config:") + key + " success";
	}
	void schedule::process(job* j)
	{
		j->sign();
		clock_t now, begin = clock();
		while (m_running)
		{
			if (unlikely(j->m_status == job::KILLED))//todo
				return;
			if (!j->m_process->process())//todo
				return;
			if (j->m_status == job::WAIT_NEXT)
				return;
			now = clock();
			if (now - begin >= CLOCKS_PER_SEC >>8)//work 4 ms
			{
				if (j->m_status == job::READY_FOR_PROCESS)
					putJobToRunning(j);
				return;
			}
		}
	}
	void schedule::worker()
	{
		uint16_t idleRound = 0;
		while (likely(m_running))
		{
			job* j = getNextActiveHandle();
			if (j == NULL)
			{
				if (++idleRound >= m_maxIdleRound&&m_threadPool.getCurrentThreadNumber()>1)//at least we keep one thread work
				{
					return;
				}
				else
				{
					std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
					continue;
				}
			}
			else
			{
				if (idleRound > 10)
					idleRound -= 10;
				else
					idleRound = 0;
			}
			process(j);
		}
		return;
	}
	job* schedule::getNextActiveHandle()
	{
		m_taskLock.lock();
		job* j = m_task.top();
		if (j != nullptr)
		{
			m_task.pop();
			m_allScore -= j->m_vtime;
			m_taskLock.unlock();
			return j;
		}
		else
		{
			m_taskLock.unlock();
			return nullptr;
		}
	}
}
