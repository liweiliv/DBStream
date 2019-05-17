#pragma once
#ifdef OS_LINUX
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)
#else
#define unlikely(x) x
#define likely(x) x
#endif
