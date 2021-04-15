#pragma once
/*
#ifndef min
	#if defined OS_LINUX
		#define min(a, b) std::min(a, b)
	#elif defined OS_WIN
		#include <minwindef.h>
	#endif
#endif
#ifndef max
	#if defined OS_LINUX
		#define max(a, b) std::min(a, b)
	#elif defined OS_WIN
		#include <minwindef.h>
	#endif
#endif
*/
#ifndef min
	#define min(a, b)            (((a) < (b)) ? (a) : (b))
#endif
#ifndef max
	#define max(a, b)            (((a) > (b)) ? (a) : (b))
#endif

