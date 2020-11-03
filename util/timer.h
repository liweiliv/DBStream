#pragma once
#include <thread>
#include <stdint.h>
#include <map>
#include <time.h>
#include <mutex>
#include "itoaSse.h"
#ifdef OS_WIN
#include <Windows.h>
#endif
class timer
{
public:

#pragma pack(1)
	struct timestamp
	{
		union
		{
			/*Little-Endian */
			struct {
				uint32_t nanoSeconds : 30;
				uint64_t seconds : 34;
			};
			uint64_t time;
		};
		timestamp() :time(0) {}
		timestamp(uint64_t t) :time(t) {}
		timestamp(uint64_t s, uint32_t ns) :seconds(s), nanoSeconds(ns) {}
		timestamp(const timestamp& t) :time(t.time) {}
		timestamp& operator=(const timestamp& t)
		{
			time = t.time;
			return *this;
		}
		inline void add(int64_t second, uint32_t nanoSecond)
		{
			if (nanoSeconds + nanoSecond > 1000000000)
			{
				nanoSeconds = nanoSeconds + nanoSecond - 1000000000;
				second++;
			}
			else
			{
				nanoSeconds += nanoSecond;
			}
			seconds += second;
		}
		inline void add(timestamp time)
		{
			add(time.seconds, time.nanoSeconds);
		}
		static timestamp delta(uint64_t s, uint64_t d)
		{
			timestamp st(s), dt(d);
			timestamp v;
			if (st.nanoSeconds > dt.nanoSeconds)
			{
				v.seconds = st.seconds - dt.seconds;
				v.nanoSeconds = st.nanoSeconds - dt.nanoSeconds;
			}
			else
			{
				v.seconds = st.seconds - dt.seconds - 1;
				v.nanoSeconds = st.nanoSeconds + 1000000000LL - dt.nanoSeconds;
			}
			return v;
		}
		uint8_t toString(char* s)
		{
			uint8_t length = u64toa_sse2(seconds, s);
			s[length - 1] = '.';
			uint8_t nlength = u32toa_sse2_b(nanoSeconds, &s[length + 9]);
			for (int i = length + 9 - nlength; i >= length; i--)
				s[i] = '0';
			return 1 + length + nlength;
		}
		std::string toString()
		{
			char s[32] = { 0 };
			uint8_t length = toString(s);
			return std::string(s, length);
		}
	};
	struct timerTask
	{
		timestamp cycle;
		void* userData;
		void (*action)(uint64_t, void*);
	};
#pragma pack()
private:
	std::multimap<uint64_t, timerTask*> tasks;
	bool running;
	std::mutex lock;
	std::thread timeThreadInfo;
public:
	inline static uint64_t getNowTimestamp()
	{
		timestamp time;
#ifdef OS_WIN
		FILETIME ct;
		GetSystemTimeAsFileTime(&ct);
		uint64_t nowLongTime = (uint64_t)ct.dwLowDateTime +
			(((uint64_t)ct.dwHighDateTime) << 32) - 116444736000000000LL;
		time.seconds = nowLongTime / 10000000;
		time.nanoSeconds = nowLongTime % 10000000;
#endif
#ifdef OS_LINUX
		struct timespec ct;
		clock_gettime(CLOCK_REALTIME, &ct);
		time.seconds = ct.tv_sec;
		time.nanoSeconds = ct.tv_nsec;
#endif
		return time.time;
	}
	timer() :running(false) {}
	~timer()
	{
		stop();
		clear();
	}
	void clear()
	{
		lock.lock();
		for (std::multimap<uint64_t, timerTask*>::iterator iter = tasks.begin(); iter != tasks.end(); )
		{
			delete iter->second;
			tasks.erase(iter++);
		}
		lock.unlock();
	}
	void start()
	{
		lock.lock();
		if (!running)
		{
			running = true;
			timeThreadInfo = std::thread(_timerThread, this);
		}
		lock.unlock();
	}
	void stop()
	{
		lock.lock();
		if (running)
		{
			running = false;
			timeThreadInfo.join();
		}
		lock.unlock();
	}
	bool addTimer(void* userData, void (*action)(uint64_t, void*), uint64_t cycle)
	{
		timerTask* task = new timerTask;
		task->cycle.time = cycle;
		task->userData = userData;
		task->action = action;
		timestamp tm;
		tm.time = getNowTimestamp();;
		tm.add(task->cycle);
		lock.lock();
		if (!running)
		{
			lock.unlock();
			delete task;
			return false;
		}
		tasks.insert(std::pair<uint64_t, timerTask*>(tm.time, task));
		lock.unlock();
		return true;
	}
	static void _timerThread(timer* _timer)
	{
		_timer->timerThread();
	}
	void timerThread()
	{
		while (running)
		{
			uint64_t nowTime;
			lock.lock();

			for (std::multimap<uint64_t, timerTask*>::iterator iter = tasks.begin(); iter != tasks.end(); )
			{
				nowTime = getNowTimestamp();
				if (iter->first <= nowTime)
				{
					timerTask* task = iter->second;
					task->action(nowTime, task->userData);
					tasks.erase(iter++);
					timestamp tm;
					tm.time = nowTime;
					tm.add(task->cycle);
					tasks.insert(std::pair<uint64_t, timerTask*>(tm.time, task));
				}
				else
					break;
			}
			lock.unlock();
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}
};