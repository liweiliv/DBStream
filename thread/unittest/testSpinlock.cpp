#include "thread/spinlock.h"
#include "thread/qspinlock.h"
#include "thread/mcsSpinlock.h"
#include "thread/threadLocal.h"
#include <mutex>
#include <assert.h>
#include <util/timer.h>

int64_t idx = 0;
template<typename LOCK>
void testLock(LOCK* lock,int cnt)
{
	timer::timestamp t, t1;
	t.time = timer::getNowTimestamp();
	for (int i = 0; i < cnt; i++)
	{
		lock->lock();
		idx++;
		lock->unlock();
	}
	t1.time = timer::getNowTimestamp();
	printf("---------%s--------\n", __func__);
	printf("%lu.%lu\n", t.seconds, t.nanoSeconds);
	printf("%lu.%lu\n", t1.seconds, t1.nanoSeconds);
	printf("%lu\n", (t1.seconds - 1 - t.seconds) * 1000000000 + 1000000000 - t.nanoSeconds + t1.nanoSeconds);
}
#define THREAD_COUNT  2
template<typename LOCK>
void test()
{
	idx = 0;
	LOCK lock;
	int maxCountPerThread = 10000000;
	std::thread t[THREAD_COUNT];
	for (int i = 0; i < THREAD_COUNT; i++)
	{
		t[i] = std::thread(testLock<LOCK>, &lock, maxCountPerThread);
	}
	for (int i = 0; i < THREAD_COUNT; i++)
	{
		t[i].join();
	}
	printf("%ld\n",idx);
	assert(idx == THREAD_COUNT * maxCountPerThread);
}
int main()
{
//	test<std::mutex>();
//	test<spinlock>();
//	test<mcsSpinlock>();
	test<qspinlock>();
}
