#include <assert.h>
#include <string.h>
#include "memory/buddySystem.h"
#include <map>
#include <list>
int testBasicAllocAndFree()
{
	buddySystem buddy(128u * 1024 * 1024, 128, 18);
	std::map<int,bufferBase*> bmap;
	for (int i = 0; i < 1024 * 1024; i++)
	{
		bufferBase* b = buddy.alloc(128);
		for (uint32_t j = 0; j < 128 / sizeof(int); j++)
			((int*)b->buffer)[j] = i;
		bmap.insert(std::pair<int, bufferBase*>(i, b));
	}
	for (std::map<int, bufferBase*>::iterator iter = bmap.begin(); iter != bmap.end(); iter++)
	{
		bufferBase* b = iter->second;
		for (uint32_t j = 0; j < 128 / sizeof(int); j++)
			assert(((int*)b->buffer)[j] ==  iter->first);
	}
	assert(buddy.alloc(128) == nullptr);
	buddy.free(bmap.find(0)->second);
	bmap.erase(0);
	buddy.free(bmap.find(1)->second);
	bmap.erase(1);
	bufferBase* b = buddy.alloc(256);
	assert(b != nullptr);
	for (int i = 2; i < 4; i++)
	{
		buddy.free(bmap.find(i)->second);
		bmap.erase(i);
	}
	buddy.free(b);
	b = buddy.alloc(512);
	assert(b != nullptr);

	return 0;
}
int main()
{
	testBasicAllocAndFree();
}
