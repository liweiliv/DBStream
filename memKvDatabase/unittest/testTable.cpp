#include "memKvDatabase/table.h"
#include "util/timer.h"
using namespace KVDB;
int test()
{
	tbb::concurrent_hash_map < int, int, hashWrap<int> > iMap;
	uint64_t t1 = Timer::getNowTimestamp();
	for (int i = 0; i < 1000000; i++)
		iMap.insert(std::pair<int, int>(i, i));
	uint64_t t2 = Timer::getNowTimestamp();
	tbb::concurrent_hash_map < int, int, hashWrap<int> >::const_accessor accessor;
	for (int i = 0; i < 1000000; i++)
		iMap.find(accessor, i);
	uint64_t t3 = Timer::getNowTimestamp();
	printf("%s\n%s\n", Timer::Timestamp::delta(t2, t1).toString().c_str(), Timer::Timestamp::delta(t3, t2).toString().c_str());
	return 0;
}
int main()
{
	test();
	return 0;
}