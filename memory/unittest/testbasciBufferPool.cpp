#include "memory/basicBufferPool.h"
#include "stdio.h"
#include "stdlib.h"
#include <string.h>
#include <thread>
void t(basicBufferPool *p,int size)
{
	char** mem = (char**)malloc(sizeof(char*) * 10240);
	memset(mem, 0, sizeof(char*) * 10240);
	for (int i = 0; i < 100000; i++)
	{
		int k = rand() % 10240;
		if (mem[k] != nullptr)
		{
			for (int m = 0; m < size; m += 4)
			{
				if (*(int*)& mem[k][m] != k)
					abort();
			}
			basicBufferPool::free(mem[k]);
			mem[k] = nullptr;
		}
		else
		{
			mem[k] = (char*)p->alloc();
			for (int m = 0; m < size; m += 4)
				* (int*)& mem[k][m] = k;
		}
	}
	free(mem);
 }
void test(int size)
{
	basicBufferPool pool(size,1024*1024*1024);
	std::thread tl[10];
	for (int i = 0; i < 10; i++)
	{
		tl[i] = std::thread(t, &pool, size);
	}
	for (int i = 0; i < 10; i++)
		tl[i].join();
	t(&pool, size);

}
int main()
{
	test(32);
	test(256);
	test(1024);
}
