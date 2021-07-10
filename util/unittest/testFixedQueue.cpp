#include "util/arrayQueue.h"
#include "util/timer.h"
#include <thread>
#include <assert.h>
int testSingleThreadPopPush()
{
	arrayQueue<int> q(1024);
	std::thread t1([&]() {
		for (int i = 0; i < 1000000; i++)
			while (!q.push(i));
		});
	std::thread t2([&]() {
		for (int i = 0; i < 1000000; i++) {
			int v = 0;
			while (!q.pop(v));
			assert(v == i);
		}});
	t1.join();
	t2.join();
	return 0;
}
int testSingleThreadPopPushWithCond()
{
	arrayQueue<int> q(1024);
	std::thread t1([&]() {
		for (int i = 0; i < 1000000; i++)
			q.pushWithCond(i);
		});
	std::thread t2([&]() {
		for (int i = 0; i < 1000000; i++) {
			int v;
			q.popWithCond(v);
			assert(v == i);
		}});
	t1.join();
	t2.join();
	return 0;
}
#define THREAD_COUNT  5
#define TEST_COUNT 2000000

#define THREAD_COUNT  5
#define TEST_COUNT 2000000
int testSingleThreadPopMultiThreadPush()
{
	arrayQueue<int> q(1024);
	std::thread t1[THREAD_COUNT];
	for (int tid = 0; tid < THREAD_COUNT; tid++)
	{
		t1[tid] = std::thread([&](int id) {
			for (int i = 0; i < TEST_COUNT; i++) {
				int v = i | (id << 28);
				q.pushWithLock(v);
			}
			}, tid);
	}
	std::thread t2([&]() {
		int vl[THREAD_COUNT] = { 0 };
		for (int i = 0; i < TEST_COUNT * THREAD_COUNT; i++) {
			int v;
			q.popWithLock(v);
			int tid = v >> 28;
			if (vl[tid] != 0)
				assert(vl[tid] == v - 1);
			vl[tid] = v;
		}
		for (int tid = 0; tid < THREAD_COUNT; tid++)
			assert(vl[tid] == ((tid << 28) | (TEST_COUNT - 1)));
		});
	for (int tid = 0; tid < THREAD_COUNT; tid++)
		t1[tid].join();
	t2.join();
	return 0;
}

int main()
{
	uint64_t t1 = Timer::getNowTimestamp();
	testSingleThreadPopPush();
	uint64_t t2 = Timer::getNowTimestamp();
	testSingleThreadPopPushWithCond();
	uint64_t t3 = Timer::getNowTimestamp();
	testSingleThreadPopMultiThreadPush();
	uint64_t t4 = Timer::getNowTimestamp();
	printf("%s\n%s\n%s\n", Timer::Timestamp::delta(t2, t1).toString().c_str(), Timer::Timestamp::delta(t3, t2).toString().c_str()
		, Timer::Timestamp::delta(t4, t3).toString().c_str());
	return 0;
}