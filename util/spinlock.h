#pragma once
#include <atomic>
#include <thread>
#include <chrono>
#include "likely.h"
class spinlock
{
private:
	std::atomic_flag _lock = ATOMIC_FLAG_INIT;
#ifdef DEBUG
	std::thread::id id;
#endif
public:
	spinlock() :_lock(ATOMIC_FLAG_INIT)
	{}
	inline void lock()
	{
		if (likely(!_lock.test_and_set(std::memory_order_acquire)))
			return;
		for (uint8_t i = 0; _lock.test_and_set(std::memory_order_acquire);)
		{
			if (i < 4)
				i++;
			else
				std::this_thread::sleep_for(std::chrono::nanoseconds(100));//we found it is better than _mm_pause
		}
#ifdef DEBUG
		id = std::this_thread::get_id();
#endif
	}
	inline bool trylock()
	{
		if (!_lock.test_and_set(std::memory_order_acquire))
		{
#ifdef DEBUG
			id = std::this_thread::get_id();
#endif
			return true;
		}
		else
			return false;
	}
	inline void unlock()
	{
		_lock.clear(std::memory_order_release);
	}
};
