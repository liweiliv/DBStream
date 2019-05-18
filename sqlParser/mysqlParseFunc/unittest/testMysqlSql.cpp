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
#define mysqlFuncLib "libmysqlParserFuncs.dll"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "libmysqlParserFuncs.so"
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
	parser.LoadFuncs(mysqlFuncLib);

	parser.LoadParseTreeFromFile("./sqlParser/ParseTree");
	testCreateTable(&parser);
	destroyStackLog();
}


