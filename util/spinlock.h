#pragma once
#include <atomic>
#include <thread>
#include <chrono>
#include "likely.h"
#include "barrier.h"
#ifdef OS_WIN
#include <winnt.h>
#endif
struct mcsSpinlockNode {
	volatile mcsSpinlockNode* next;
	std::atomic_char wait;
};
static thread_local mcsSpinlockNode MCS_SPINLOCK_LOCAL_NODE;
struct mcsSpinlock {
	std::atomic<mcsSpinlockNode*> next;
#ifdef DEBUG
	std::thread::id id;
#endif
	mcsSpinlock()
	{
		next.store(nullptr, std::memory_order_relaxed);
	}
	inline void lock()
	{
		MCS_SPINLOCK_LOCAL_NODE.next = nullptr;
		mcsSpinlockNode* prev = next.exchange(&MCS_SPINLOCK_LOCAL_NODE,std::memory_order_acq_rel);
		if (likely(prev == nullptr))
		{
#ifdef DEBUG
			id = std::this_thread::get_id();
#endif
			return;
		}
		MCS_SPINLOCK_LOCAL_NODE.wait.store(1, std::memory_order_relaxed);
		prev->next = &MCS_SPINLOCK_LOCAL_NODE;
		if (MCS_SPINLOCK_LOCAL_NODE.wait.load(std::memory_order_relaxed) != 0)
		{
			for (uint8_t i = 0; MCS_SPINLOCK_LOCAL_NODE.wait.load(std::memory_order_relaxed) != 0;) {
#ifdef OS_WIN
				YieldProcessor();
#endif
#ifdef OS_LINUX
				asm volatile ("rep;nop" : : : "memory");
#endif
				std::this_thread::yield();
			}
		}
#ifdef DEBUG
		id = std::this_thread::get_id();
#endif
	}
	inline bool tryLock()
	{
		MCS_SPINLOCK_LOCAL_NODE.next = nullptr;
		mcsSpinlockNode *tail = nullptr;
		if (likely(next.compare_exchange_strong(tail, &MCS_SPINLOCK_LOCAL_NODE, std::memory_order_acq_rel)))
		{
#ifdef DEBUG
			id = std::this_thread::get_id();
#endif
			return true;
		}
		else
		{
			return false;
		}
	}
	inline void unlock()
	{
		if (likely(MCS_SPINLOCK_LOCAL_NODE.next == nullptr))
		{
			mcsSpinlockNode* tail = &MCS_SPINLOCK_LOCAL_NODE;
			if (likely(next.compare_exchange_strong(tail, nullptr, std::memory_order_acq_rel)))
				return;
			for (uint8_t i = 0; MCS_SPINLOCK_LOCAL_NODE.next == nullptr;) {
#ifdef OS_WIN
				YieldProcessor();
#endif
#ifdef OS_LINUX
				asm volatile ("rep;nop" : : : "memory");
#endif
				std::this_thread::yield();

			}

		}
		MCS_SPINLOCK_LOCAL_NODE.next->wait.store(0, std::memory_order_relaxed);
	}
};
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
