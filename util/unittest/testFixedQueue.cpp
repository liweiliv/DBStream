#include "util/fixedQueue.h"
#include "util/timer.h"
#include <thread>
#include <assert.h>
int testSingleThreadPopPush()
{
	fixedQueue<int> q(1024);
	std::thread t1([&]() {
		for (int i = 0; i < 1000000; i++)
			while (!q.push(i));
		});
	std::thread t2([&]() {
		for (int i = 0; i < 1000000; i++) {
			int v;
			while (!q.pop(v));
			assert(v == i);
		}});
	t1.join();
	t2.join();
	return 0;
}
int testSingleThreadPopPushWithCond()
{
	fixedQueue<int> q(1024);
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
int testSingleThreadPopMultiThreadPush()
{
	fixedQueue<int> q(1024);
	std::thread t1[5];
	int** vll = new int *[5];
	for (int tid = 0; tid < 5; tid++)
	{
		vll[tid] = new int[1000000];
		memset(vll[tid], 0, sizeof(int) * 1000000);
	}
	for (int tid = 0; tid < 5; tid++)
	{
		t1[tid] = std::thread([&](int id) {
			for (int i = 0; i < 1000000; i++) {
				int v = i | (id << 28);
				vll[id][i] = q.pushWithLock(v);
			}
			}, tid);
	}
	std::thread t2([&]() {
		int vl[5] = { 0 };
		for (int i = 0; i < 5000000; i++) {
			int v;
			q.popWithCond(v);
			int tid = v >> 28;
			if (vl[tid] != 0)
				assert(vl[tid] == v - 1);
			vl[tid] = v;
		}
		for (int tid = 0; tid < 5; tid++)
			assert(vl[tid] == ((tid << 28) | (1000000 - 1)));
		});
	for (int tid = 0; tid < 5; tid++)
		t1[tid].join();
	t2.join();
	return 0;
}
int main()
{
	uint64_t t1 = timer::getNowTimestamp();
	testSingleThreadPopPush();
	uint64_t t2 = timer::getNowTimestamp();
	testSingleThreadPopPushWithCond();
	uint64_t t3 = timer::getNowTimestamp();
	testSingleThreadPopMultiThreadPush();
	uint64_t t4 = timer::getNowTimestamp();
	printf("%s\n%s\n%s\n", timer::timestamp::delta(t2, t1).toString().c_str(), timer::timestamp::delta(t3, t2).toString().c_str()
		,timer::timestamp::delta(t4, t3).toString().c_str());
	return 0;
}