/*
 * testMetaDataCollection.cpp
 *
 *  Created on: 2019年4月22日
 *      Author: liwei
 */
#include <stdio.h>
#include "../../sqlParser/sqlParserUtil.h"
#include "stdio.h"
#include "../../util/stackLog.h"
#include "../metaDataCollection.h"
#ifdef OS_WIN
#define mysqlFuncLib "libmysqlParserFuncs.dll"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "libmysqlParserFuncs.so"
#endif
int test()
{
	initStackLog();
	initKeyWords();
	META::metaDataCollection m("utf8");
	m.initSqlParser("./sqlParser/ParseTree",mysqlFuncLib);
	m.processDDL("create  database test",1);
	m.processDDL("create table test.t1 (a int primary key)",2);
	printf("%s\n",m.get("test","t1",2)->toString().c_str());
	m.processDDL("alter table test.t1 add column b varchar(20)",3);
	printf("%s\n",m.get("test","t1",3)->toString().c_str());
	return 0;
}
int main()
{
	test();
}


