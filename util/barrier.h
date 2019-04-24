#pragma once
#ifdef OS_WIN
#include <windows.h>
#define barrier MemoryBarrier();
#else
#ifdef OS_LINUX
#define barrier __asm__ __volatile__("mfence" ::: "memory");
#endif
#endif
