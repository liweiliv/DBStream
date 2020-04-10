#pragma once
#include <atomic>
#include <thread>
#include <chrono>
#include "util/likely.h"
#include "util/barrier.h"
#include "yield.h"
#ifdef OS_WIN
#include <winnt.h>
#endif
struct spinlock
{
	std::atomic_flag _lock = ATOMIC_FLAG_INIT;
#ifdef DEBUG
	std::thread::id id;
#endif
	spinlock() //:_lock(ATOMIC_FLAG_INIT)
	{}
	inline void lock()
	{
		if (likely(!_lock.test_and_set(std::memory_order_acquire)))
			return;
		while (_lock.test_and_set(std::memory_order_acquire))
			yield();
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
