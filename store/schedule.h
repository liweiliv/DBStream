/*
 * cond.h
 *
 *  Created on: 2019年1月14日
 *      Author: liwei
 */

#ifndef SCHEDULE_H_
#define SCHEDULE_H_
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "../util/unblockedQueue.h"
#include <atomic>
#include <mutex>
#include <queue>
#include <time.h>
#include <thread>
#include "iterator.h"
#include "../util/config.h"
#include <glog/logging.h>
#include "job.h"

namespace STORE{
#define COND_LEVEL 5
#define C_SCHEDULE "schedule"
#define C_MAX_WORKERS C_SCHEDULE ".maxWorders"
#define C_MAX_IDLE_ROUND C_SCHEDULE ".maxIdleRound"

#define SCHEDULE_MAX_WORKER_THREAD 256
#define SCHEDULE_TASK_LEVEL 5
class schedule
{
public:
	struct streamCmp
	{
		inline bool operator()(stream *a, stream *b) const
		{
			return a->m_vtime < b->m_vtime;
		}
	};
private:
	std::priority_queue<stream*,std::vector<job*>, streamCmp> m_task;
	std::mutex m_taskLock;
	std::atomic<uint8_t> m_activeTask;
	std::atomic<bool> m_running;
    std::thread m_workers[SCHEDULE_MAX_WORKER_THREAD];
	std::atomic<int8_t> m_workersStatus[SCHEDULE_MAX_WORKER_THREAD];
	std::atomic<uint16_t> m_workerCount;
	uint32_t m_allScore;
    uint16_t m_maxWorders;
    int m_maxIdleRound;
    config * m_config;
public:
    schedule(config * conf):m_running(false),m_config(conf), m_allScore(0)
    {
		for (uint16_t i = 0; i < SCHEDULE_MAX_WORKER_THREAD; i++)
			m_workersStatus[i].store(-1, std::memory_order_relaxed);
        m_maxWorders = atoi(m_config->get("store",C_MAX_WORKERS,"8").c_str());
		if (m_maxWorders > SCHEDULE_MAX_WORKER_THREAD)
			m_maxWorders = SCHEDULE_MAX_WORKER_THREAD;
        m_maxIdleRound = atoi(m_config->get("store",C_MAX_IDLE_ROUND,"100").c_str());
        m_workerCount.store(0);
    }
    std::string updateConfig(const char *key,const char * value)
    {
        if(strcmp(key,C_MAX_WORKERS)==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if(value[idx]>'9'||value[idx]<'0')
                    return "value of config :maxWorders must be a number";
            }
            if(atol(value)> SCHEDULE_MAX_WORKER_THREAD)
            {
                char numBuf[32] = {0};
                sprintf(numBuf,"%d", SCHEDULE_MAX_WORKER_THREAD);
                return std::string("value of config :maxWorders is too large,max is ")+numBuf;
            }
            if(atol(value)<=0)
            {
                return std::string("value of config :maxWorders must greater than 0 ");
            }
            m_maxWorders  = atoi(value);
        }
        else if(strcmp(key,C_MAX_IDLE_ROUND)==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if(value[idx]>'9'||value[idx]<'0')
                    return "value of config :maxIdleRound must be a number";
            }
            m_maxIdleRound  = atoi(value);
        }
        else
            return std::string("unknown config:")+key;
        m_config->set("store",key,value);
        return std::string("update config:")+key+" success";
    }
    inline void wakeUp(job * s)
    {
		m_taskLock.lock();
		m_task.push(s);
		m_allScore += s->m_vtime;
		m_taskLock.unlock();
    }
    int start()
    {
        m_running = true;
        LOG(INFO)<<"schedule work thread starting...";
		startNewWorkThread();
        LOG(INFO)<<"schedule started";
        return 0;
    }
    int stop()
    {
        LOG(INFO)<<"schedule stoping...";
        m_running =  false;
		for (uint16_t i = 0; i < SCHEDULE_MAX_WORKER_THREAD; i++)
		{
			if (m_workersStatus[i] > 0)
			{
				m_workers[i].join();
				m_workersStatus[i] = -1;
			}
		}
		LOG(INFO)<<"schedule stopped...";
        return 0;
    }
private:
	bool startNewWorkThread()
	{
		for (uint16_t i = 0; i < SCHEDULE_MAX_WORKER_THREAD; i++)
		{
			int8_t status = m_workersStatus[i].load(std::memory_order_acq_rel);
			if (status == 1)
			{
				if (!m_workersStatus[i].compare_exchange_strong(status, 0))
					status = m_workersStatus[i].load(std::memory_order_acq_rel);
				else
				{
					m_workers[i].join();
					m_workersStatus[i].store(-1, std::memory_order_acq_rel);
					status = -1;
				}
			}
			uint16_t workerCount = m_workerCount.load(std::memory_order_acq_rel);
			if (workerCount >= m_maxWorders)
				return false;
			if (status < 0)
			{
				if (!m_workersStatus[i].compare_exchange_strong(status, 0))
					continue;
				m_workers[i] = std::thread(worker, this);
				LOG(INFO) << "schedule work thread :" << i << " start";
				return true;
			}
		}
		return false;
	}
	inline void checkIfIsBusy()
	{
		if (m_workerCount >= m_maxWorders)
			return;
		m_taskLock.lock();
		if (m_task.size() > m_workerCount * 2 && m_allScore / m_task.size() > 128)
		{
			m_taskLock.unlock();
			LOG(INFO) << "now schedule is busy ,try start new work thread";
			startNewWorkThread();
		}
	}
    inline job * getNextActiveHandle()
    {
		m_taskLock.lock();
		job * j = m_task.top();
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
	void process(job * j)
	{
		iterator::status nextStatus;
		clock_t now,begin = clock();
		while (m_running)
		{
			if (j->m_status == job::READY_FOR_PROCESS||j->m_status == job::INTERRUPTTED)
			{
				if (!j->m_process->process());//todo

			}

			if (j->m_status == job::WAIT_NEXT)
			{
				if (h->iter->next())
					h->status = job::READY_FOR_PROCESS;
				else
				{
					switch (h->iter->m_status)
					{
					case iterator::ENDED:
						h->finish(h);
						break;
					case iterator::INVALID:
						h->destroy(h);
						delete h;
						return;
					case iterator::BLOCKED:
						h->score >> 1;
						return;
					}
				}
			}
			now = clock();
			if (now - begin >= CLOCKS_PER_SEC / 200)//work 5 ms
			{
				if (j->m_status == job::READY_FOR_PROCESS)
				{
					wakeUp(j);
				}
				return;
			}
		}
	}
    static void worker(schedule * s,uint16_t id)
    {
        uint16_t idleRound = 0;
        uint16_t workerCount = 0;
        s->m_workerCount++;
        while(s->m_running)
        {
            job * j = s->getNextActiveHandle();
            if(j == NULL)
            {
                if(++idleRound >= s->m_maxIdleRound&&(workerCount = s->m_workerCount.load(std::memory_order_relaxed))>1)
                {
					if (s->m_workerCount.compare_exchange_strong(workerCount, workerCount - 1))
					{
						s->m_workersStatus[id] = 1;
						return;
					}
                }
				else
				{
					std::this_thread::yield();
					continue;
				}
            }
			else
			{
				idleRound -= 10;
			}
			s->process(j);
        }
		s->m_workerCount--;
		s->m_workersStatus[id] = 1;
		return;
    }
};
}



#endif /* SCHEDULE_H_ */
