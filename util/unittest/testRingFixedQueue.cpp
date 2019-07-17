#include "util/ringFixedQueue.h"
#include <thread>
#include <assert.h>
ringFixedQueue<long>* q = nullptr;
void t1()
{
	for (long i = 1; i < 10000000; i++)
		while (!q->push(i, 1000));
}
void t2()
{
	for (long i = 1; i < 10000000; i++)
	{
		long j = 0;
		while (!q->pop(j, 1000));
		assert(j == i);
	}
}
int main()
{
	q = new ringFixedQueue<long>(10240);
	std::thread th1(t1);
	std::thread th2(t2);
	th1.join();
	th2.join();
	return 0;
}
