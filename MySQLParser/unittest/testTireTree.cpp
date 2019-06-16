/*
 * testTireTree.cpp
 *
 *  Created on: 2018年11月6日
 *      Author: liwei
 */
#include "../tireTree.h"
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
tireTree *tree;
void * r(void * argv)
{
    char str[32] = {0};
    for(int idx=0;idx<10;idx++)
    {
        for(int i=0;i<1000000;i++)
        {
            sprintf(str,"%d",i);
            void * v = tree->find((uint8_t*)str);
            if(v!=NULL)
                assert((unsigned long)v==i);
        }
    }
}
void * w(void* argv)
{
    char str[32] = {0};
    for(int i=1;i<1000000;i++)
    {
        sprintf(str,"%d",i);
        tree->insert((uint8_t*)str,(void*)(uint64_t)i);
        void * v = tree->find((uint8_t*)str);
        assert(v!=NULL&&(unsigned long)v==i);
    }
}
int main()
{
    pthread_t rt[2];
    pthread_t wt;
    tree = new tireTree;
    pthread_create(&wt,NULL,w,NULL);
    for(int i=0;i<sizeof(rt)/sizeof(pthread_t);i++)
        pthread_create(&rt[i],NULL,r,NULL);
    pthread_join(wt,NULL);
    for(int i=0;i<sizeof(rt)/sizeof(pthread_t);i++)
        pthread_join(rt[i],NULL);
    for(tireTree::iterator iter=tree->begin();iter.valid();iter.next())
    {
        printf("%s,%lu\n",(const char*)iter.key(),(uint64_t)iter.value());
        assert(atol((const char*)iter.key())==(uint64_t)iter.value());
    }
   // assert(prev==1000000-1);
    delete tree;
    return 0;
}



