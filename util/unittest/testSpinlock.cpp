#include "util/spinlock.h"
#include <mutex>
#include <assert.h>
#include <util/timer.h>
long idx = 0;
void testThread(mcsSpinlock *lock)
{
	timer::timestamp t,t1;
	t.time=timer::getNowTimestamp();
	for (int i = 0; i < 100000000; i++)
	{
		lock->lock();
		idx++;
		lock->unlock();
	}
	t1.time = timer::getNowTimestamp();
	printf("%lu.%lu\n", t.seconds, t.nanoSeconds);
	printf("%lu.%lu\n", t1.seconds, t1.nanoSeconds);

}
void testThread1(std::mutex* lock)
{

	for (int i = 0; i < 100000000; i++)
	{
		lock->lock();
		idx++;
		lock->unlock();
	}
}
int  test()
{
	std::thread t[1];
	mcsSpinlock lock;
	std::mutex mlock;
	for (int i = 0; i < sizeof(t) / sizeof(std::thread); i++)
		t[i] = std::thread(testThread, &lock);
	for (int i = 0; i < sizeof(t) / sizeof(std::thread); i++)
		t[i].join();
	assert(idx == (sizeof(t) / sizeof(std::thread)) * 100000000);
	return 0;
}
int main()
{
	test();
}