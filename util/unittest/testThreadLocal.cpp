#include "../threadLocal.h"
#include <stdio.h>
#include <thread>
#ifdef OS_WIN
#pragma comment(lib,"lib\\util.lib")
#endif
struct test{
	int i;
	int j;
};
threadLocal<test> *testv;
void testThread()
{
	test * v = testv->get();
	if (v == nullptr)
	{
		v = new test();
		testv->set(v);
	}
	v->i=getThreadId();
	v->j=getThreadId();
	v = testv->get();
	printf("%d,%d,%d\n", getThreadId(),v->i,v->j);
	std::this_thread::sleep_for(std::chrono::microseconds(1000));
	printf("%d,%d,%d\n", getThreadId(), v->i, v->j);
}
int main()
{
	testv = new threadLocal<test>();
	std::thread t1(testThread);
	std::thread t2(testThread);
	std::thread t3(testThread);
	std::thread t4(testThread);
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	printf("%d\n", getThreadId());
	delete testv;
}
