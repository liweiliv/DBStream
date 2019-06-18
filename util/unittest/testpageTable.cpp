#include "../pageTable.h"
#include <thread>
#include <chrono>
pageTable<uint64_t> p;
uint64_t *base;
uint64_t** to;
#define c 10000000
void testThread(uint64_t id)
{
	std::this_thread::sleep_for(std::chrono::nanoseconds(rand()%100000));
	for (unsigned int i = 1; i < c; i++)
	{
		uint64_t v = ((id << 32) | i);
		uint64_t rtv = p.set(i, v);
		if (rtv != v)
		{
			to[id][i] = rtv ;
			if ((rtv & 0xffffffffu) != i)
			{
				abort();
			}
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
	for (unsigned int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
	{
		to[i] = new uint64_t[c];
		memset(to[i], 0, c);
		t[i] = new std::thread(testThread, i);
	}
	for (unsigned int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
		t[i]->join();
	for (unsigned int i = 0; i < sizeof(t) / sizeof(std::thread*); i++)
	{
		for (int j = 1; j < c; j++)
		{
			uint64_t v = p.get(j);
			if (base[j] != to[i][j]||v!=base[j])
				abort();
		}
		delete t[i];
		delete to[i];
	}
	delete base;
	delete []to;
	return 0;
}
