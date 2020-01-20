#pragma once
#include "util/timer.h"
#include "util/winDll.h"
namespace GLOBAL
{
	extern timer::timestamp currentTime;
	DLL_IMPORT extern timer globalTimer;
	constexpr static auto version = "0.1.0";
	DLL_EXPORT void init();
	DLL_EXPORT void close();
}