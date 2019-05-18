/*
 * testMetaDataCollection.cpp
 *
 *  Created on: 2019年4月22日
 *      Author: liwei
 */
#include <stdio.h>
#include "../metaDataCollection.h"
int test()
{
	META::metaDataCollection m("utf8");
	m.processDDL("create  database test",1);
	m.processDDL("create table test.t1 (a int primary key)",2);
	printf("%s\n",m.get("test","t1",2)->toString().c_str());
	m.processDDL("alter table teset.t1 add column b varchar(20)",3);
	printf("%s\n",m.get("test","t1",3)->toString().c_str());
	return 0;
}
int main()
{
	test();
}


