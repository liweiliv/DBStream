#pragma once
#include <stdint.h>
#include <atomic>
#include <assert.h>
#if defined OS_LINUX
#include "rawAtomic.h"
#elif defined OS_WIN
#include "windows.h"
#endif
#include "util/likely.h"
#include "util/winDll.h"

/*
 * Bitfields in the atomic value:
 *  0- 7: locked byte
 *     8: pending
 *  9-15: not used
 * 16-17: tail index
 * 18-31: tail cpu (+1)
 */
#define	_Q_SET_MASK(type)	(((1U << _Q_ ## type ## _BITS) - 1)\
				      << _Q_ ## type ## _OFFSET)
#define _Q_LOCKED_OFFSET	0
#define _Q_LOCKED_BITS		8
#define _Q_LOCKED_MASK		_Q_SET_MASK(LOCKED)

#define _Q_PENDING_OFFSET	(_Q_LOCKED_OFFSET + _Q_LOCKED_BITS)
#define _Q_PENDING_BITS		8

#define _Q_PENDING_MASK		_Q_SET_MASK(PENDING)

#define _Q_TAIL_IDX_OFFSET	(_Q_PENDING_OFFSET + _Q_PENDING_BITS)
#define _Q_TAIL_IDX_BITS	2
#define _Q_TAIL_IDX_MASK	_Q_SET_MASK(TAIL_IDX)

#define _Q_TAIL_CPU_OFFSET	(_Q_TAIL_IDX_OFFSET + _Q_TAIL_IDX_BITS)
#define _Q_TAIL_CPU_BITS	(32 - _Q_TAIL_CPU_OFFSET)
#define _Q_TAIL_CPU_MASK	_Q_SET_MASK(TAIL_CPU)

#define _Q_TAIL_OFFSET		_Q_TAIL_IDX_OFFSET
#define _Q_TAIL_MASK		(_Q_TAIL_IDX_MASK | _Q_TAIL_CPU_MASK)

#define _Q_LOCKED_VAL		(1U << _Q_LOCKED_OFFSET)
#define _Q_PENDING_VAL		(1U << _Q_PENDING_OFFSET)
#define _Q_LOCKED_PENDING_MASK (_Q_LOCKED_MASK | _Q_PENDING_MASK)

#define MAX_LOCK_LOOP_PER_THREAD 4
#pragma pack(4)
struct qspinlock
{
	std::atomic<int32_t> val;
	qspinlock() :val(0)
	{
	}
	inline bool isLocked()
	{
		return val.load(std::memory_order_relaxed) > 0;
	}
	inline bool try_lock()
	{
		int32_t _val = val.load(std::memory_order_relaxed);
		if (unlikely(_val))
			return false;
		return val.compare_exchange_strong(_val, _Q_LOCKED_VAL, std::memory_order_release);
	}
	inline void lock()
	{
		int32_t _val = 0;
		if (val.compare_exchange_strong(_val, _Q_LOCKED_VAL, std::memory_order_release))
			return;
		queuedSpinlockSlowpath(_val);
	}
	inline void unlock()
	{
		val.store(0, std::memory_order_release);
	}
	DLL_EXPORT void queuedSpinlockSlowpath(int32_t val);
	inline int32_t setPendingAndGetOldValue()
	{
		return val.fetch_or(_Q_PENDING_VAL, std::memory_order_release);
	}
	inline void unsetPending()
	{
		val.fetch_and(~_Q_PENDING_VAL,std::memory_order_release);
	}
	inline void clearPendingSetLocked()
	{
		int32_t v = val.fetch_add(-_Q_PENDING_VAL + _Q_LOCKED_VAL);
		assert(v & _Q_PENDING_VAL);
		assert(!(v & _Q_LOCKED_VAL));
	}
	inline uint32_t xchgTail(int tail)
	{
		int32_t _val = val.load(std::memory_order_relaxed);
		for (;;)
		{
			/*
			 * We can use relaxed semantics since the caller ensures that
			 * the MCS node is properly initialized before updating the
			 * tail.
			 */
			if (val.compare_exchange_weak(_val, (_val & _Q_LOCKED_PENDING_MASK) | tail, std::memory_order_release))
				return _val;
		}
	}
};
#pragma pack()
