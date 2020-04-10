#pragma once
#include <stdint.h>
#include "alternative.h"
typedef struct {
	int64_t counter;
} atomic64_t;
static inline void atomic64Inc(atomic64_t* v)
{
	asm volatile(LOCK_PREFIX "incq %0"
		: "=m" (v->counter)
		: "m" (v->counter) : "memory");
}
static inline void atomic64Dec(atomic64_t* v)
{
	asm volatile(LOCK_PREFIX "decq %0"
		: "=m" (v->counter)
		: "m" (v->counter) : "memory");
}
static inline void atomic64Add(int64_t i, atomic64_t* v)
{
	asm volatile(LOCK_PREFIX "addq %1,%0"
		: "=m" (v->counter)
		: "er" (i), "m" (v->counter) : "memory");
}

static inline void atomic64Sub(int64_t i, atomic64_t* v)
{
	asm volatile(LOCK_PREFIX "subq %1,%0"
		: "=m" (v->counter)
		: "er" (i), "m" (v->counter) : "memory");
}
#define __raw_cmpxchg64(ptr, old, _new, lock)			\
({									\
	volatile uint64_t *__ptr = (volatile uint64_t *)(ptr);		\
	int64_t __ret;\
	asm volatile(lock "cmpxchgq %2,%1"			\
		     : "=a" (__ret), "+m" (*__ptr)		\
		     : "r" (_new), "0" (old)			\
		     : "memory");				\
	__ret;								\
})

#define __cmpxchg64(ptr, old, _new)					\
	__raw_cmpxchg64((ptr), (old), (_new),LOCK_PREFIX)
static inline int64_t atomic64Cmpxchg(atomic64_t* v, int64_t old, int64_t _new)
{
	return __cmpxchg64(&v->counter, old, _new);
}
#define __xchg_op64(ptr, arg, lock)					\
	({								\
	      __typeof__ (*(ptr)) __ret = (arg);			\
			asm volatile (lock "xchg" "q %q0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
		__ret;							\
	})
static inline int64_t atomic64Xchg(atomic64_t* v, int64_t _new)
{
	return __xchg_op64(&v->counter, _new, "");
}
