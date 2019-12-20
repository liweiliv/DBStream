#pragma once
#include "util/timer.h"
#include "util/winDll.h"
namespace GLOBAL
{
	DLL_EXPORT extern timer::timestamp currentTime;
	DLL_EXPORT extern timer globalTimer;
	constexpr static auto version = "0.1.0";
	DLL_EXPORT void init();
	DLL_EXPORT void close();
}