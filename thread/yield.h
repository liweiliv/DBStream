#pragma once
#include <thread>
static inline void yield()
{
#ifdef OS_WIN
	YieldProcessor();
#endif
#ifdef OS_LINUX
	asm volatile ("rep;nop" : : : "memory");
#endif
	std::this_thread::yield();
}