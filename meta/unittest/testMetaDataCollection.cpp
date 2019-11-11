/*
 * testMetaDataCollection.cpp
 *
 *  Created on: 2019年4月22日
 *      Author: liwei
 */
#include <stdio.h>
#include "sqlParser/sqlParser.h"
#include "sqlParser/sqlParserUtil.h"
#include "stdio.h"
#include "util/stackLog.h"
#include "meta/ddl.h"
#include "meta/metaDataCollection.h"
#ifdef OS_WIN
#include <windows.h>
#define mysqlFuncLib "mysqlParserFuncs.dll"
#define mysqlParserTree "D:\\git\\DBStream\\sqlParser\\ParseTree"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
#define mysqlParserTree "sqlParser/ParseTree"
#endif
#include <iostream>
class metaShell
{
	SQL_PARSER::sqlParser parser;
	META::metaDataCollection m;
	std::string usedDb;
	void parserDdl(const char* ddl)
	{
		SQL_PARSER::handle* h = nullptr;
		parser.parse(h, usedDb.empty() ? nullptr : usedDb.c_str(), ddl);
		if (h != nullptr && !h->dbName.empty())
		{
			if (m.isDataBaseExist(h->dbName.c_str()))
				usedDb = h->dbName;
			else
			{
				LOG(ERROR) << "unkown database:" << h->dbName;
			}
		}
		if (h != nullptr)
			delete h;
	}
public:
	metaShell():m("utf8")
	{
	}
	int init()
	{
		if (0 != m.initSqlParser(mysqlParserTree, mysqlFuncLib) || 0 != parser.LoadFuncs(mysqlFuncLib) || 0 != parser.LoadParseTreeFromFile(mysqlParserTree))
		{
			return -1;
		}
		return 0;
	}
	void shell()
	{
		std::string sql;
		int idx = 0;
		while (true)
		{
			std::string tmp;
			std::cin >> tmp;
			if (tmp.c_str()[tmp.size() - 1] == ';')
			{
				sql.append(" ").append(tmp);
				parserDdl(sql.c_str());
				m.processDDL(sql.c_str(), usedDb.empty() ? nullptr : usedDb.c_str(), idx++);
				sql.clear();
			}
			else
				sql.append(" ").append(tmp);
		}
	}
};
#define FAILED 		{printf("test %s failed @%d\n", __FUNCTION__, __LINE__);return -1;}
int test()
{
	META::metaDataCollection m("utf8");
	META::metaDataCollection m1("utf8");
	if (0 != m.initSqlParser(mysqlParserTree, mysqlFuncLib) || 0 != m1.initSqlParser(mysqlParserTree, mysqlFuncLib))
		FAILED;
	m.processDDL("create database test",nullptr,1);
	m1.processDDL("create database test", nullptr, 1);

	m.processDDL("create table test.test (a int primary key,b char(20),c char(20))", nullptr, 2);
	m1.processDDL("create table test.test (a int primary key,b char(20),c char(20))", nullptr, 2);
	if (*m1.get("test", "test") != *m.get("test", "test"))
		FAILED;
	m.processDDL("create table test.test2 like test.test",nullptr,3);
	m1.processDDL("create table test.test2 (a int primary key,b char(20),c char(20))", nullptr, 3);
	if (*m1.get("test", "test2") != *m.get("test", "test2"))
		FAILED;
	m.processDDL("alter table test.test2 add column d text first", nullptr, 4);
	m1.processDDL("drop table test.test2", nullptr, 5);
	m1.processDDL("create table test.test2 (d text,a int primary key,b char(20),c char(20))", nullptr, 6);
	printf("%s\n", m.get("test", "test2")->toString().c_str());
	printf("%s\n", m1.get("test", "test2")->toString().c_str());

	if (*m.get("test", "test2") != *m1.get("test", "test2"))
		FAILED;
	return 0;
}
int main()
{
#ifdef OS_WIN
	SetDllDirectory("..\\lib");
#endif
	initKeyWords();
	test();
	metaShell shell;
	if (0 != shell.init())
		return -1;
	shell.shell();
	return 0;
}


