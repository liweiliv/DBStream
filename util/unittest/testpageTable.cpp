#include "../pageTable.h"
#include <thread>
#include <chrono>
pageTable p;
uint64_t *base;
uint64_t** to;
#define c 10000000
void testThread(unsigned long id)
{
	std::this_thread::sleep_for(std::chrono::nanoseconds(rand()%100000));
	for (int i = 1; i < c; i++)
	{
		uint64_t v = ((id << 32) | i);
		void *rtv = p.set(i, (void*)v);
		if ((unsigned long)rtv != v)
		{
			to[id][i] = ((unsigned long)rtv) ;
			assert((((unsigned long)rtv)& 0xffffffffu) == i);
		}
		else
		{
			base[i] = v;
			to[id][i] = v;
		}
		//rtv = p.get(i);
		//assert(rtv==(void*)to[id][i]);
	}
}
int main()
{
	std::thread *t[10];
	to = new uint64_t*[sizeof(t) / sizeof(std::thread*)];
	base = new uint64_t[c];
	for (int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
	{
		to[i] = new uint64_t[c];
		memset(to[i], 0, c);
		t[i] = new std::thread(testThread, i);
	}
	for (int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
		t[i]->join();
	for (int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
	{
		for (int j = 1; j < c; j++)
		{
			void * v = p.get(j);
			if (base[j] != to[i][j]||(uint64_t)v!=base[j])
				abort();
		}
		delete t[i];
		delete to[i];
	}
	delete base;
	delete []to;
	return 0;
}
