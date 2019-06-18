#include <thread>
#include <stdio.h>
#include "../dualLinkList.h"
struct s
{
	dualLinkListNode node;
	int id;
};
void t(dualLinkList * l,int id)
{
	for(int i=0;i<100000;i++)
	{
		if(rand()%1)
		{
			dualLinkListNode *node = (i&0x01)?l->popLast():l->popFirst();
			if(node)
				delete container_of(node,s,node);
		}
		else
		{
			s *_s = new s;
			l->insert(&_s->node);
		}
	}
}
void test()
{
	std::thread tl[10];
	 dualLinkList l;
	for(int i=0;i<10;i++)
	{
		tl[i] = std::thread(t,&l,i);
	}
	for(int i=0;i<10;i++)
		tl[i].join();
	while(1)
	{
                dualLinkListNode *node = l.popLast();
                if(node)
                     delete container_of(node,s,node);
		else
			break;

	}
}
int main()
{
	test();
}
