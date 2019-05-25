#include "threadLocal.h"
#include <thread>
#include <mutex>
std::thread::id globalThreads[maxThreadCount] ;
static int allActiveThreadCount = 0;
std::mutex globalThreadsLock;
bool initLocalThreadId()
{
	std::lock_guard<std::mutex> lock(globalThreadsLock);
	if (allActiveThreadCount>= maxThreadCount)
		return false;
	std::thread::id zero;
	for (int i = (allActiveThreadCount + 1) % maxThreadCount; i != allActiveThreadCount; i = (i + 1) % maxThreadCount)
	{
		if (globalThreads[i] == zero)
		{
			globalThreads[i] = std::this_thread::get_id();
			threadid = i;
		}
		continue;
	}
}
