#pragma once
#include "util/winDll.h"
#include "threadLocal.h"
class runable
{
public:
	enum runableStatus {
		WAIT_NEXT,
		READY_FOR_PROCESS,
		KILLED,
		FINISH,
		FAULT
	};
	runable* next;
	runableStatus status;
	int threadId;
	runable():next(nullptr), status(READY_FOR_PROCESS){}
	virtual ~runable() {}
	virtual void action() = 0;
	DLL_EXPORT static runable*& currentRunable();
	inline void sign()
	{
		threadId = getThreadId();
		currentRunable() = this;
	}
};