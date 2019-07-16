#include "../ringBuffer.h"
#include <assert.h>
void test()
{
	ringBuffer buffer;
	char * mem[256] = {0};
	for(int i = 0;i<1000000;i++)
	{
		mem[i % 256] = (char*)buffer.alloc(256);
		for(int j=0;j<256/4;j++)
		{
			*(int*)(mem[i%256]+j*4) = i % 256;
		}
		int prev =  (i+256-100)%256;
		if(mem[prev]!=nullptr)
		{
			for(int j=0;j<256/4;j++)
				assert(*(int*)(mem[prev]+j*4) == prev);
			buffer.freeMem(mem[prev]);
			mem[prev] = nullptr;
		}
	}
}
int main()
{
	test();
}
