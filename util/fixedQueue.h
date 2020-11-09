#pragma once
#include <stdint.h>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include "thread/barrier.h"
template <typename T>
class fixedQueue
{
private:
	T* m_queue;
	uint32_t m_volumn;
	uint32_t m_mask;
	uint32_t m_head;
	uint32_t m_tail;
	std::condition_variable m_cond;
	std::mutex m_lock;
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
		return nextPos(pos) == m_tail
	}
public:
	fixedQueue(int size = 256) :m_volumn(calVolumn(size)), m_mask(m_volumn - 1), m_head(0), m_tail(0)
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
		wmb();
		m_head = next;
		return true;
	}
	inline bool pop(T& v)
	{
		if (empty())
			return false;
		uint32_t next = nextPos(m_tail);
		v = m_queue[next];
		wmb();
		m_tail = next;
		return true;
	}

	inline void pushWithCond(T& v)
	{
		uint32_t next;
		while ((next = nextPos(m_head)) == m_tail)
		{
			std::unique_lock<std::mutex> lock(m_lock);
			auto t = std::chrono::system_clock::now();
			t += std::chrono::microseconds(100);
			m_cond.wait_until(lock, t);
		}		
		m_queue[next] = v;
		wmb();
		m_head = next;
		m_cond.notify_one();
	}
	inline void popWithCond(T& v)
	{
		while (empty())
		{
			std::unique_lock<std::mutex> lock(m_lock);
			auto t = std::chrono::system_clock::now();
			t += std::chrono::microseconds(100);
			m_cond.wait_until(lock, t);
		}
		uint32_t next = nextPos(m_tail);
		v = m_queue[next];
		wmb();
		m_tail = next;
		m_cond.notify_one();
	}
	inline int pushWithLock(T& v)
	{
		uint32_t next;
	RETRY:
		while ((next = nextPos(m_head)) == m_tail)
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::microseconds(100);
			std::unique_lock<std::mutex> lock(m_lock);
			m_cond.wait_until(lock, t);
		}		
		m_lock.lock();
		if (next != nextPos(m_head))
		{
			m_lock.unlock();
			goto RETRY;
		}
		m_queue[next] = v;
		wmb();
		m_head = next;
		m_lock.unlock();
		m_cond.notify_one();
		return next;
	}
	inline void popWithLock(T& v)
	{
		uint32_t next;
	RETRY:
		while ((next = m_tail) == m_head)
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::microseconds(100);
			m_cond.wait_until(lock, t);
		}		m_lock.lock();
		if (next != m_tail)
		{
			m_lock.unlock();
			goto RETRY;
		}
		next = nextPos(next);
		v = m_queue[next];
		wmb();
		m_tail = next;
		m_lock.unlock();
		m_cond.notify_one();
	}
};