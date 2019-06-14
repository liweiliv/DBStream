#include "threadLocal.h"
#include "dualLinkList.h"
#include <thread>
#include <mutex>
std::thread::id globalThreads[maxThreadCount] ;
static int allActiveThreadCount = 0;
std::mutex globalThreadsLock;
thread_local int threadid = maxThreadCount + 1;
static bool initLocalThreadId()
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
			return true;
		}
		continue;
	}
	return false;
}
static void unsetThreadId()
{
	if (threadid >= maxThreadCount)
		return;
	std::lock_guard<std::mutex> lock(globalThreadsLock);
	globalThreads[threadid] = std::thread::id();
	threadid = maxThreadCount + 1;
}
struct threadLocalInfo {
	void* v;
	void (*_unset)(void* v);
	globalLockDualLinkListNode node;
};
static globalLockDualLinkList threadLocalList;
void registerThreadLocalVar(void (*_unset)(void* v), void* v)
{
	threadLocalInfo* t = new threadLocalInfo();
	t->v = v;
	t->_unset = _unset;
	threadLocalList.insert(&t->node);
}
void destroyThreadLocalVar(void* v)
{
	globalLockDualLinkList::iterator *iter = new globalLockDualLinkList::iterator(&threadLocalList);
	if (!iter->valid())
		return;
	do {
		threadLocalInfo* t = container_of(iter->value(), threadLocalInfo, node);
		if (t->v == v)
		{
			delete iter;
			threadLocalList.erase(&t->node);
			delete t;
			return;
		}
	} while (iter->next());
	delete iter;
}

threadLocalWrap::threadLocalWrap()
	{
		initLocalThreadId();
	}
threadLocalWrap::~threadLocalWrap()
	{
		globalLockDualLinkList::iterator iter(&threadLocalList);
		do {
			threadLocalInfo* t = container_of(iter.value(), threadLocalInfo, node);
			t->_unset(t->v);
		} while (iter.next());
	}
thread_local threadLocalWrap _threadLocalWrap;
