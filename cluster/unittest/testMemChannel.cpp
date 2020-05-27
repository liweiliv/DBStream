#include "cluster/memChannel.h"
#include <thread>
CLUSTER::memChannel channel;
int testR()
{
	char* buffer = new char[512 * 1024];
	int length = 0;
	for (int i = 0; i < 1000000; i++)
	{
		if ((i & 0xfff) == 0)
			length = (i % 64) * 1024 + 32 * 1024;
		else
			length = (i % 100) * 8;
		int rl = 0;
		while ((rl += channel.recv(buffer + rl, length - rl, 1000)) < length);
		for (int j = 0; j < (length & (~0x3)); j += 4)
		{
			if (*(int*)(buffer + j) != i)
			{
				printf("test failed\n");
				exit(-1);
			}
		}
	}
	delete[]buffer;
	printf("read finished\n");
	return 0;
}
int testW()
{
	char* buffer = new char[512 * 1024];
	int length = 0;
	for (int i = 0; i < 1000000; i++)
	{
		if ((i & 0xfff) == 0)
			length = (i % 64) * 1024 + 32 * 1024;
		else
			length = (i % 100) * 8;
		for (int j = 0; j < (length & (~0x3)); j += 4)
			*(int*)(buffer + j) = i;
		int sl = 0;
		while ((sl += channel.send(buffer +sl, length-sl, 1000)) < length);
	}
	printf("write finished\n");
	delete[]buffer;
	return 0;
}
int main()
{
	std::thread tw(testW);
	std::thread tr(testR);
	tw.join();
	tr.join();
	return 0;
}
