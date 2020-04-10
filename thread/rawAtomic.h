#pragma once
#include <stdint.h>
#include "alternative.h"
typedef struct {
	int32_t counter;
} atomic_t;
static inline void atomicInc(atomic_t* v)
{
	asm volatile(LOCK_PREFIX "incl %0"
		: "+m" (v->counter) :: "memory");
}
static inline void atomicDec(atomic_t* v)
{
	asm volatile(LOCK_PREFIX "decl %0"
		: "+m" (v->counter) :: "memory");
}
static inline void atomicAdd(int32_t i, atomic_t* v)
{
	asm volatile(LOCK_PREFIX "addl %1,%0"
		: "+m" (v->counter)
		: "ir" (i) : "memory");
}

static inline void atomicSub(int32_t i, atomic_t* v)
{
	asm volatile(LOCK_PREFIX "subl %1,%0"
		: "+m" (v->counter)
		: "ir" (i) : "memory");
}
#define __raw_cmpxchg(ptr, old, _new, lock)			\
({									\
	volatile uint32_t *__ptr = (volatile uint32_t *)(ptr);		\
	int64_t __ret;\
	asm volatile(lock "cmpxchgl %2,%1"			\
		     : "=a" (__ret), "+m" (*__ptr)		\
		     : "r" (_new), "0" (old)			\
		     : "memory");				\
	__ret;								\
})

#define __cmpxchg(ptr, old, _new)					\
	__raw_cmpxchg((ptr), (old), (_new),LOCK_PREFIX)
static inline int64_t atomicCmpxchg(atomic_t* v, int64_t old, int32_t _new)
{
	return __cmpxchg(&v->counter, old, _new);
}
#define __xchg_op(ptr, arg, lock)					\
	({								\
	      __typeof__ (*(ptr)) __ret = (arg);			\
			asm volatile (lock "xchg" "l %0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
		__ret;							\
	})
#define __xchg_op_w(ptr, arg, lock)					\
	({								\
	      __typeof__ (*(ptr)) __ret = (arg);			\
			asm volatile (lock "xchg" "w %w0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
		__ret;							\
	})
static inline int32_t atomicXchg(atomic_t* v, int32_t _new)
{
	return __xchg_op(&v->counter, _new, LOCK_PREFIX);
}
static inline int16_t atomicXchgI16(int16_t* v, int16_t _new)
{
	return __xchg_op_w(v, _new, LOCK_PREFIX);
}
static inline int16_t atomicXchgI16Relaxed(int16_t* v, int16_t _new)
{
	return __xchg_op_w(v, _new, "");
}
