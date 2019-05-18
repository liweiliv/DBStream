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
#define mysqlFuncLib "../lib/libmysqlParserFuncs.dll"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "../lib/libmysqlParserFuncs.so"
#endif
int testCreateTable(SQL_PARSER::sqlParser * parser)
{
	SQL_PARSER::handle * h = nullptr;
	const char * sql = "create table test.test1 (a int primary key ,b char (200),c varchar(200))";
	SQL_PARSER::parseValue v = parser->parse(h,sql);
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
	initStackLog();
	initKeyWords();
	SQL_PARSER::sqlParser parser;
	if(0!=parser.LoadFuncs(mysqlFuncLib))
	{
		printf("load funcs from lib :%s failed\n",mysqlFuncLib);
		return -1;
	}
	if(0!=parser.LoadParseTreeFromFile("../sqlParser/ParseTree"))
	{
		printf("load parse tree from file :%s failed\n","../sqlParser/ParseTree");
		return -1;
	}

	testCreateTable(&parser);
	destroyStackLog();
}


