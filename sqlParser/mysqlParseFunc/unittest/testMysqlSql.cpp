/*
 * testMysqlSql.cpp
 *
 *  Created on: 2019年4月27日
 *      Author: liwei
 */
#include "sqlParser/sqlParser.h"
#include "sqlParser/sqlParserUtil.h"
#include "stdio.h"
#include "util/stackLog.h"
#ifdef OS_WIN
#pragma comment(lib,"sqlParser.lib")
#define mysqlFuncLib "mysqlParserFuncs.dll"
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
	testSql(&parser,"test","alter table aaa change d c int");
	testSql(&parser, "test", "CREATE TABLE IF NOT EXISTS `ACT_RU_VARIABLE` ( `ID_` VARCHAR(128) NOT NULL, `REV_` DECIMAL(65, 30), `TYPE_` VARCHAR(510) NOT NULL, `NAME_` VARCHAR(510) NOT NULL, `EXECUTION_ID_` VARCHAR(128), `PROC_INST_ID_` VARCHAR(128), `TASK_ID_` VARCHAR(128), `BYTEARRAY_ID_` VARCHAR(128), `DOUBLE_` DECIMAL(20, 2), `LONG_` DECIMAL(19, 0), `TEXT_` VARCHAR(4000), `TEXT2_` VARCHAR(4000), CONSTRAINT `SYS_C0015278` PRIMARY KEY(`ID_`) ) CHARACTER SET utf8mb4");
	destroyStackLog();
}


