#pragma once
#include <atomic>
#include "yield.h"
struct mcsSpinlockNode {
	volatile mcsSpinlockNode* next;
	volatile bool wait;
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
		mcsSpinlockNode* prev = next.exchange(&MCS_SPINLOCK_LOCAL_NODE, std::memory_order_acq_rel);
		if (likely(prev == nullptr))
		{
#ifdef DEBUG
			id = std::this_thread::get_id();
#endif
			return;
		}
		MCS_SPINLOCK_LOCAL_NODE.wait = true;
		prev->next = &MCS_SPINLOCK_LOCAL_NODE;
		while (MCS_SPINLOCK_LOCAL_NODE.wait)
			yield();
#ifdef DEBUG
		id = std::this_thread::get_id();
#endif
	}
	inline bool tryLock()
	{
		MCS_SPINLOCK_LOCAL_NODE.next = nullptr;
		mcsSpinlockNode* tail = nullptr;
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
			while (MCS_SPINLOCK_LOCAL_NODE.next == nullptr)
				yield();
		}
		MCS_SPINLOCK_LOCAL_NODE.next->wait = false;
	}
};