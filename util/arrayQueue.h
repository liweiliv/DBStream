#pragma once
#include <stdint.h>
#include <mutex>
#include <chrono>
#include <condition_variable>
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

	inline void pushWithCond(T& v)
	{
		uint32_t next;
		while ((next = nextPos(m_head)) == m_tail)
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::microseconds(100);
			std::unique_lock<std::mutex> lock(m_wLock);
			m_fullCond.wait_until(lock, t);
		}
		m_queue[next] = v;
		m_head = next;
		m_emptyCond.notify_one();
	}
	inline void popWithCond(T& v)
	{
		while (empty())
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(100);
			std::unique_lock<std::mutex> lock(m_rLock);
			m_emptyCond.wait_until(lock, t);
		}
		uint32_t next = nextPos(m_tail);
		v = m_queue[next];
		m_tail = next;
		m_fullCond.notify_one();
	}
private:
	inline void pushWithLock_(T& v)
	{
		uint32_t next;
		std::unique_lock<std::mutex> lock(m_wLock);
		while ((next = nextPos(m_head)) == m_tail)
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::seconds(1);
			m_fullCond.wait_until(lock, t);
		}
		m_queue[next] = v;
		m_head = next;
	}
public:
	inline void pushWithLock(T& v)
	{
		pushWithLock_(v);
		m_emptyCond.notify_one();
	}
private:
	inline void popWithLock_(T& v)
	{
		uint32_t next;
		std::unique_lock<std::mutex> lock(m_rLock);
		while ((next = m_tail) == m_head)
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::seconds(1);
			m_emptyCond.wait_until(lock, t);
		}
		next = nextPos(next);
		v = m_queue[next];
		m_tail = next;
	}
public:
	inline void popWithLock(T& v)
	{
		popWithLock_(v);
		m_fullCond.notify_one();
	}
};