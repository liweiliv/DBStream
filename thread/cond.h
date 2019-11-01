#pragma once
#define ST_WITHOUT_COUNT
#include "util/nonBlockStack.h"
#include "util/unblockedQueue.h"
#include "util/winDll.h"
#include "threadLocal.h"
#include "runable.h"
struct cond
{
	nonBlockStack<runable> s;
	inline void wait()
	{
		s.push(runable::currentRunable());
	}
	inline void wakeUp()
	{
		runable* r = s.popAll();
		while (r != nullptr)
		{
			r->action();
			r = r->next;
		}
	}
};
#undef ST_WITHOUT_COUNT
