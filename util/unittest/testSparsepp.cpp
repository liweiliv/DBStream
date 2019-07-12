#include "../sparsepp/spp.h"
using namespace std; 
int test()
{
	spp::sparse_hash_map<long, long> m;
	for(int i=0;i<100000;i++)
	{
		for(long j=10;j<100;j++)
		{
			m.insert(std::pair<long,long>(j,j));
			m.erase(j);
		}
	}
}
int test1()
{
        spp::sparse_hash_map<long, long> m;
	m.insert(std::pair<long,long>(100,100));
	std::pair<spp::sparse_hash_map<long, long>::iterator,bool> rtv = m.insert(std::pair<long,long>(100,100));
	if(!rtv.second)
	{
		printf("dup\n");
		rtv.first->second = 101;
	}
	printf("%ld\n",m.find(100)->second);

}
int main()
{
	test1();
}
