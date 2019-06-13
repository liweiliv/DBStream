#pragma once
#define ST_WITHOUT_COUNT
#include "../util/nonBlockStack.h"
#include "../util/unblockedQueue.h"
#include "../util/threadLocal.h"
#include "job.h"
namespace STORE {
	extern thread_local job* currentJob;
	struct cond 
	{
		nonBlockStack<job> s;
		inline void wait()
		{
			s.push(currentJob);
		}
		inline void wakeUp()
		{
			job* j = s.popAll();
			while (j != nullptr)
			{
				j->wakeUp();
				j = j->next;
			}
		}
	};
}
#undef ST_WITHOUT_COUNT