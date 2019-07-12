#include "../sparsepp/spp.h"
using spp::sparse_hash_map;
using namespace std; 
int test()
{
	sparse_hash_map<long, long> m;
	for(int i=0;i<100000;i++)
	{
		for(long j=10;j<100;j++)
		{
			m.insert(std::pair<long,long>(j,j));
			m.erase(j);
		}
	}
}
int main()
{
	test();
}
