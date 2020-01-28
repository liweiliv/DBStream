#include "util/bitUtil.h"
#include <assert.h>
int test()
{
	uint64_t l = 1;
	for (int i = 1; i < 64; i++)
	{
		assert(highPosOfULong(l) == i);
		l <<= 1;
	}
	assert(highPosOfULong(0) == 0);
	return 0;
}
int main()
{
	return test();
}
