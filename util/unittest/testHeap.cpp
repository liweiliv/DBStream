#include "util/heap.h"
#include <stdio.h>
#include <stdlib.h>
#define FAILED do{printf("test %s failed @%d\n", __FUNCTION__, __LINE__); return -1;}while(0);
int test()
{
	heap<int, defaultLessCompare<int> > h(100);
	for(int i=100;i>0;i--)
		h.insert(i);
	int prev = -1;
	for (int i = 0; i < 100; i++)
	{
		int min = h.get();
		if (min < prev)
			FAILED;
		prev = min;
		h.pop();
	}
	for (int i = 100; i > 0; i--)
	{
		h.insert(rand());
	}	
	prev = -1;
	for (int i = 0; i < 50; i++)
	{
		int min = h.get();
		if (min < prev)
			FAILED;
		prev = min;
		h.pop();
	}
	for (int i = 50; i > 0; i--)
	{
		h.insert(rand());
	}
	prev = -1;
	for (int i = 0; i < 100; i++)
	{
		int min = h.get();
		if (min < prev)
			FAILED;
		prev = min;
		h.pop();
	}
		

	for (int i = 50; i > 0; i--)
	{
		h.insert(rand());
	}
	for (int i = 50; i > 0; i--)
	{
		h.popAndInsert(rand());
	}
	 prev = -1;
	for (int i = 0; i < 50; i++)
	{
		int min = h.get();
		if (min < prev)
			FAILED;
		prev = min;
		h.pop();
	}
	return 0;
}
int main()
{
	test();
}