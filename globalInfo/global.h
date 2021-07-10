#pragma once
#include "util/timer.h"
#include "util/winDll.h"
namespace GLOBAL
{
	extern Timer::Timestamp currentTime;
	DLL_IMPORT extern Timer globalTimer;
	constexpr static auto version = "0.1.0";
	DLL_EXPORT void init();
	DLL_EXPORT void close();
}
