#include "global.h"
namespace GLOBAL
{
	DLL_EXPORT timer::timestamp currentTime;
	DLL_EXPORT timer globalTimer;
	static void timerAction (uint64_t nowTime, void* nptr)
	{
		currentTime.time = nowTime;
	}
	DLL_EXPORT void init()
	{
		currentTime.time = timer::getNowTimestamp();
		globalTimer.start();
		globalTimer.addTimer(nullptr, timerAction, 1000000);
	}
	DLL_EXPORT void close()
	{
		globalTimer.stop();
	}

}