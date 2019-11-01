#include "threadLocal.h"
#include "util/dualLinkList.h"
#include <thread>
#include <mutex>
static std::thread::id globalThreads[maxThreadCount];
static int allActiveThreadCount = 0;
static std::mutex globalThreadsLock;

thread_local int threadid = maxThreadCount + 1;
thread_local threadLocalWrap _threadLocalWrap;
#ifdef OS_WIN
DLL_EXPORT int getThreadId()
{
	return threadid;
}
DLL_EXPORT threadLocalWrap& getThreadLocalWrap()
{
	return _threadLocalWrap;
}
#endif
static bool initLocalThreadId()
{
	std::lock_guard<std::mutex> lock(globalThreadsLock);
	if (allActiveThreadCount >= maxThreadCount)
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
	globalLockDualLinkList::iterator* iter = new globalLockDualLinkList::iterator(&threadLocalList);
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
	if (iter.valid())
	{
		do {
			globalLockDualLinkListNode* node = iter.value();
			threadLocalInfo* t = container_of(node, threadLocalInfo, node);
			t->_unset(t->v);
		} while (iter.next());
	}

}
