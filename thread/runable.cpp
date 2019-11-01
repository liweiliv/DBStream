#include "runable.h"
static thread_local runable* _currentRunable;
DLL_EXPORT runable *& runable::currentRunable()
{
	return _currentRunable;
}