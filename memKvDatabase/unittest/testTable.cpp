#include "memKvDatabase/table.h"
#include "util/timer.h"
using namespace KVDB;
int test()
{
	tbb::concurrent_hash_map < int, int, hashWrap<int> > iMap;
	uint64_t t1 = timer::getNowTimestamp();
	for (int i = 0; i < 1000000; i++)
		iMap.insert(std::pair<int, int>(i, i));
	uint64_t t2 = timer::getNowTimestamp();
	tbb::concurrent_hash_map < int, int, hashWrap<int> >::const_accessor accessor;
	for (int i = 0; i < 1000000; i++)
		iMap.find(accessor, i);
	uint64_t t3 = timer::getNowTimestamp();
	printf("%s\n%s\n", timer::timestamp::delta(t2, t1).toString().c_str(), timer::timestamp::delta(t3, t2).toString().c_str());
	return 0;
}
int main()
{
	test();
	return 0;
}