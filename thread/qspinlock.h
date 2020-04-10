#pragma once
#include <stdint.h>
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
	union {
		uint32_t val;
#ifdef OS_LINUX
		atomic_t aval;
#endif
		struct {
			uint8_t	locked;
			uint8_t	pending;
		};
		struct {
			uint16_t	locked_pending;
			uint16_t	tail;
		};
	};
	qspinlock():val(0)
	{
	}
	inline bool isLocked()
	{
		return *(volatile uint32_t*)&val > 0;
	}
	inline bool try_lock()
	{
		uint32_t _val = *(volatile uint32_t*)&val;
		if (unlikely(_val))
			return false;
#if defined OS_WIN
		return InterlockedCompareExchange(&val, _Q_LOCKED_VAL, _val) == _val;
#elif defined OS_LINUX
		return atomicCmpxchg(&aval, _val, _Q_LOCKED_VAL) == _val;
#endif
	}
	inline void lock()
	{
		uint32_t _val;
#if defined OS_WIN
		if (likely((_val = InterlockedCompareExchange(&val, _Q_LOCKED_VAL, 0)) == 0))
#elif defined OS_LINUX
		if (likely((_val = atomicCmpxchg(&aval, 0, _Q_LOCKED_VAL)) == 0))
#endif
		{
			return;
		}
		queuedSpinlockSlowpath(_val);
	}
	inline void unlock()
	{
		*(volatile uint16_t*)&locked = 0;
	}
	DLL_EXPORT void queuedSpinlockSlowpath(uint32_t val);
	inline uint32_t setPendingAndGetOldValue()
	{
		uint32_t _val = *(volatile uint32_t*)&val, ov;
		do {
#if defined OS_WIN
			if (likely((ov = InterlockedCompareExchange(&val, _Q_PENDING_VAL | _val, _val)) == _val))
#elif defined OS_LINUX
			if (likely((ov = atomicCmpxchg(&aval, _val, _Q_PENDING_VAL | _val)) == _val))
#endif
			{
				return _val;
			}
			_val = ov;
		} while (true);
	}
	inline void unsetPending()
	{
#if _Q_PENDING_BITS < 8
		uint32_t _val = *(volatile uint32_t*)&val, ov;
		do {
#if defined OS_WIN
			if (likely((ov = InterlockedCompareExchange(&val, (~_Q_PENDING_VAL) & _val, _val)) == _val))
#elif defined OS_LINUX
			if (likely((ov = atomicCmpxchg(&aval, _val, (~_Q_PENDING_VAL) & _val)) == _val))
#endif
			{
				return;
			}
			_val = ov;
		} while (true);
#else
		* (volatile uint8_t*)&this->pending = 0;
#endif
	}
	inline void clearPendingSetLocked()
	{
#if _Q_PENDING_BITS < 8
#if defined OS_WIN
		InterlockedAdd((long*)&val, -_Q_PENDING_VAL + _Q_LOCKED_VAL);
#elif defined OS_LINUX
		atomicAdd(-_Q_PENDING_VAL + _Q_LOCKED_VAL, &aval);
#endif
#else
		* (volatile uint16_t*)&this->locked_pending = _Q_LOCKED_VAL;
#endif

	}
	inline uint32_t xchgTail(int tail)
	{
#if defined OS_WIN
		uint32_t old, _new, val = *(volatile uint32_t*)&this->val;
		for (;;)
		{
			_new = (val & _Q_LOCKED_PENDING_MASK) | tail;
			/*
			 * We can use relaxed semantics since the caller ensures that
			 * the MCS node is properly initialized before updating the
			 * tail.
			 */
			old = InterlockedCompareExchange(&this->val, _new, val);
			if (old == val)
				break;
			val = old;
		}
		return old;
#elif defined OS_LINUX
		return ((uint32_t)atomicXchgI16Relaxed((int16_t*)&this->tail, tail >> _Q_TAIL_OFFSET)) << _Q_TAIL_OFFSET;
#endif
	}
};
#pragma pack()
