#include "../threadLocal.h"
#include <stdio.h>
#include <thread>
struct test{
	int i;
	int j;
};
threadLocal<test> *testv;
void testThread()
{
	test * v = testv->get();
	v->i=threadid;
	v->j=threadid;
	v = testv->get();
	printf("%d,%d,%d\n",threadid,v->i,v->j);
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
	delete testv;
}
