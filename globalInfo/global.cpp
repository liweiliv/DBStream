#include "util/timer.h"
#include "util/winDll.h"
namespace GLOBAL
{
	Timer::Timestamp currentTime;
	DLL_EXPORT Timer globalTimer;
	static void timerAction (uint64_t nowTime, void* nptr)
	{
		currentTime.time = nowTime;
	}
	DLL_EXPORT void init()
	{
		currentTime.time = Timer::getNowTimestamp();
		globalTimer.start();
		globalTimer.addTimer(nullptr, timerAction, 1000000);
	}
	DLL_EXPORT void close()
	{
		globalTimer.stop();
	}
}