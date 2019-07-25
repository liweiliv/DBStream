/*
 * testMysqlSql.cpp
 *
 *  Created on: 2019年4月27日
 *      Author: liwei
 */
#include "../../sqlParser.h"
#include "../../sqlParserUtil.h"
#include "../../../meta/metaChangeInfo.h"
#include "stdio.h"
#include "../../../util/stackLog.h"
#ifdef OS_WIN
#pragma comment(lib,"lib\\sqlParser.lib")
#define mysqlFuncLib "..\\lib\\mysqlParserFuncs.dll"
#define mysqlParserTree "..\\..\\..\\..\\sqlParser\\ParseTree"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
#define mysqlParserTree "sqlParser/ParseTree"
#endif
int testSql(SQL_PARSER::sqlParser * parser,const char * dbname,const char * sql)
{
	SQL_PARSER::handle * h = nullptr;
	SQL_PARSER::parseValue v = parser->parse(h, dbname,sql);
	if(v!=SQL_PARSER::OK)
	{
		if(h!=nullptr)
			delete h;
		printf("parser sql %s failed\n",sql);
		return -1;
	}
	META::metaChangeInfo * m = static_cast<META::metaChangeInfo*>(h->userData);
	m->print();
	delete h;
	return 0;
}
int main()
{
#ifdef OS_WIN
	SetDllDirectory(".\\lib");
#endif
	initStackLog();
	initKeyWords();
	SQL_PARSER::sqlParser parser;
	if(0!=parser.LoadFuncs(mysqlFuncLib))
	{
		printf("load funcs from lib :%s failed\n",mysqlFuncLib);
		return -1;
	}
	
	if(0!=parser.LoadParseTreeFromFile(mysqlParserTree))
	{
		printf("load parse tree from file :%s failed\n", mysqlParserTree);
		return -1;
	}
	//const char * sql = "create table test.test1 (a int primary key ,b char (200),c varchar(200))";
	//testSql(&parser,"alter table test.t1 add column a int unsignedd");
	//testSql(&parser,"create table a (c1 int primary key,c2 char(20))");
	testSql(&parser,"test","alter table t2 add column(c1 int,c2 char(20),c3 varchar(20)),add unique key uk1 (c1 ,c2)");
	destroyStackLog();
}


