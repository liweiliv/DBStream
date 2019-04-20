#pragma once
#ifdef OS_WIN
#include <windows.h>
#define barrier MemoryBarrier();
#elif define OS_LINUX
#define barrier __asm__ __volatile__("mfence" ::: "memory");
#endif
