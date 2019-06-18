#include <thread>
#include <stdio.h>
#include "../linkList.h"
struct s
{
	s *next;
	int id;
};
void t(linkList<s> * l,int id)
{
	for(int i=0;i<100000;i++)
	{
		if(rand()%1)
		{
			s * _s = l->pop();
			if(_s)
				delete _s;
		}
		else
		{
			s *_s = new s;
			l->push(_s);
		}
	}
}
void test()
{
	std::thread tl[10];
	linkList<s> l;
	for(int i=0;i<10;i++)
	{
		tl[i] = std::thread(t,&l,i);
	}
	for(int i=0;i<10;i++)
		tl[i].join();
	while(1)
	{
		s* _s = l.pop();
		if(_s == nullptr)
			break;
		delete _s;
	}
}
int main()
{
	test();
}
