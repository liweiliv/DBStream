#pragma once
#include <stdint.h>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include "util/likely.h"
template <typename T>
class arrayQueue
{
private:
	T* m_queue;
	uint32_t m_volumn;
	uint32_t m_mask;
	volatile uint32_t m_head;
	volatile uint32_t m_tail;
	std::mutex m_rLock;
	std::mutex m_wLock;
	std::condition_variable m_fullCond;
	std::condition_variable m_emptyCond;

	static uint32_t calVolumn(uint32_t size)
	{
		uint32_t s = 1;
		while ((s <<= 1) < size);
		return s;
	}
	inline uint32_t nextPos(uint32_t pos)
	{
		return (pos + 1) & m_mask;
	}
	inline bool isEndPos(uint32_t pos)
	{
		return nextPos(pos) == m_tail;
	}
public:
	arrayQueue(int size = 256) :m_volumn(calVolumn(size)), m_mask(m_volumn - 1), m_head(0), m_tail(0)
	{
		m_queue = new T[m_volumn];
	}
	inline bool full()
	{
		return isEndPos(m_head);
	}
	inline bool empty()
	{
		return m_head == m_tail;
	}
	inline bool push(T& v)
	{
		uint32_t next = nextPos(m_head);
		if (next == m_tail)
			return false;
		m_queue[next] = v;
		m_head = next;
		return true;
	}
	inline bool pop(T& v)
	{
		if (empty())
			return false;
		uint32_t next = nextPos(m_tail);
		v = m_queue[next];
		m_tail = next;
		return true;
	}
	inline bool pushWithCond(T& v, uint32_t outTime)
	{
		uint32_t next;
		if (unlikely((next = nextPos(m_head)) == m_tail))
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(outTime != 0xffffffffu ? outTime : 1000);
			while ((next = nextPos(m_head)) == m_tail)
			{
				std::unique_lock<std::mutex> lock(m_wLock);
				if (m_fullCond.wait_until(lock, t) == std::cv_status::timeout)
				{
					if (outTime != 0xffffffffu)
						return false;
					else
						t += std::chrono::milliseconds(1000);
				}
			}
		}
		m_queue[next] = v;
		m_head = next;
		m_emptyCond.notify_one();
		return true;
	}
	inline void pushWithCond(T& v)
	{
		pushWithCond(v, 0xffffffffu);
	}
	inline bool popWithCond(T& v, uint32_t outTime)
	{
		if (unlikely(empty()))
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(outTime != 0xffffffffu ? outTime : 1000);
			while (empty())
			{
				std::unique_lock<std::mutex> lock(m_rLock);
				if (m_emptyCond.wait_until(lock, t) == std::cv_status::timeout)
				{
					if (outTime != 0xffffffffu)
						return false;
					else
						t += std::chrono::milliseconds(1000);
				}
			}
		}
		uint32_t next = nextPos(m_tail);
		v = m_queue[next];
		m_tail = next;
		m_fullCond.notify_one();
		return true;
	}
	inline void popWithCond(T& v)
	{
		popWithCond(v, 0xffffffffu);
	}

private:
	inline bool pushWithLock_(T& v, uint32_t outTime)
	{
		uint32_t next;
		std::unique_lock<std::mutex> lock(m_wLock);
		if (unlikely((next = nextPos(m_head)) == m_tail))
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(outTime != 0xffffffffu ? outTime : 1000);
			while ((next = nextPos(m_head)) == m_tail)
			{
				if (m_fullCond.wait_until(lock, t) == std::cv_status::timeout)
				{
					if (outTime != 0xffffffffu)
						return false;
					else
						t += std::chrono::milliseconds(1000);
				}
			}
		}
		m_queue[next] = v;
		m_head = next;
		return true;
	}
public:
	inline void pushWithLock(T& v)
	{
		pushWithLock_(v, 0xffffffffu);
		m_emptyCond.notify_one();
	}
	inline bool pushWithLock(T& v, uint32_t outTime)
	{
		bool success = pushWithLock_(v, outTime);
		m_emptyCond.notify_one();
		return success;
	}
private:
	inline bool popWithLock_(T& v, uint32_t outTime)
	{
		uint32_t next;
		std::unique_lock<std::mutex> lock(m_rLock);
		if (unlikely(m_tail == m_head))
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(outTime != 0xffffffffu ? outTime : 1000);
			while (m_tail == m_head)
			{
				if (m_emptyCond.wait_until(lock, t) == std::cv_status::timeout)
				{
					if (outTime != 0xffffffffu)
						return false;
					else
						t += std::chrono::milliseconds(1000);
				}
			}
		}
		next = nextPos(m_tail);
		v = m_queue[next];
		m_tail = next;
		return true;
	}
public:
	inline void popWithLock(T& v)
	{
		popWithLock_(v, 0xffffffffu);
		m_fullCond.notify_one();
	}
	inline bool popWithLock(T& v, uint32_t outTime)
	{
		bool success = popWithLock_(v, outTime);
		m_fullCond.notify_one();
		return success;
	}
};