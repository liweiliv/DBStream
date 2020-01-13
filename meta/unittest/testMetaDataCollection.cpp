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
	m1.processDDL("create table TABLE_0("
		"`C_0` varchar(181),"
		"`C_1` varchar(223),"
		"`C_2` varchar(109),"
		"`C_3` varchar(98),"
		"`C_4` varchar(99),"
		"`C_5` varchar(48),"
		"`C_6` varchar(71),"
		"`C_7` varchar(138),"
		"`C_8` datetime,"
		"`C_9` double,"
		"`C_10` datetime,"
		"`C_11` float,"
		"`C_12` varchar(73),"
		"`C_13` varchar(221),"
		"`C_14` varchar(193),"
		"`C_15` char(192),"
		"`C_16` varchar(207),"
		"`C_17` varchar(152),"
		"`C_18` varchar(176),"
		"`C_19` varbinary(204),"
		"`C_20` float,"
		"`C_21` double,"
		"`C_22` varchar(194),"
		"`C_23` varchar(210),"
		"`C_24` varchar(94),"
		"`C_25` datetime,"
		"`C_26` bigint(38),"
		"`C_27` double,"
		"`C_28` varchar(102),"
		"`C_29` varchar(56),"
		"`C_30` char(133),"
		"`ID` bigint(38),"
		"primary key(ID),"
		"unique key nullUK1(C_25, C_29, C_10))", "test", 6);

	printf("%s\n", m.get("test", "test2")->toString().c_str());
	printf("%s\n", m1.get("test", "test2")->toString().c_str());

	if (*m.get("test", "test2") != *m1.get("test", "test2"))
		FAILED;
	m.processDDL("create table test.tm1(a int primary key,b char(20),c int,d int,unique key uk1(b,c))", nullptr, 7);
	m.processDDL("alter table test.tm1 modify column d bigint first", nullptr, 8);
	m1.processDDL("create table test.tm1(d bigint,a int primary key,b char(20),c int,unique key uk1(b,c))", nullptr, 9);
	printf("%s\n", m.get("test", "tm1")->toString().c_str());
	printf("%s\n", m1.get("test", "tm1")->toString().c_str());
	if (*m.get("test", "tm1") != *m1.get("test", "tm1"))
		FAILED;

	m.processDDL("create table test.tm2(a int primary key,b char(20),c int,d int unique key,unique key uk1(b,c))", nullptr, 10);
	m.processDDL("alter table test.tm2 modify column d bigint", nullptr, 11);
	m1.processDDL("create table test.tm2(a int primary key,b char(20),c int,d bigint unique key,unique key uk1(b,c))", nullptr, 12);
	printf("%s\n", m.get("test", "tm2")->toString().c_str());
	printf("%s\n", m1.get("test", "tm2")->toString().c_str());
	if (*m.get("test", "tm2") != *m1.get("test", "tm2"))
		FAILED;

	m.processDDL("create table test.tm3(a int primary key,b char(20),c int,d int unique key,e int,unique key uk1(b,c))", nullptr, 14);
	m.processDDL("alter table test.tm3 modify column d bigint after e", nullptr, 15);
	m1.processDDL("create table test.tm3(a int primary key,b char(20),c int,e int,d bigint unique key,unique key uk1(b,c))", nullptr, 16);
	printf("%s\n", m.get("test", "tm3")->toString().c_str());
	printf("%s\n", m1.get("test", "tm3")->toString().c_str());
	if (*m.get("test", "tm3") != *m1.get("test", "tm3"))
		FAILED;


	m.processDDL("create table test.tc1(a int primary key,b char(20),c int,d int,unique key uk1(b,c))", nullptr, 17);
	m.processDDL("alter table test.tc1 change column d d1 bigint first", nullptr, 18);
	m1.processDDL("create table test.tc1(d1 bigint,a int primary key,b char(20),c int,unique key uk1(b,c))", nullptr, 19);
	printf("%s\n", m.get("test", "tc1")->toString().c_str());
	printf("%s\n", m1.get("test", "tc1")->toString().c_str());
	if (*m.get("test", "tc1") != *m1.get("test", "tc1"))
		FAILED;

	m.processDDL("create table test.tc2(a int primary key,b char(20),c int,d int unique key,unique key uk1(b,c))", nullptr, 20);
	m.processDDL("alter table test.tc2 change column d d1 bigint", nullptr, 21);
	m1.processDDL("create table test.tc2(a int primary key,b char(20),c int,d1 bigint unique key,unique key uk1(b,c))", nullptr, 22);
	printf("%s\n", m.get("test", "tc2")->toString().c_str());
	printf("%s\n", m1.get("test", "tc2")->toString().c_str());
	if (*m.get("test", "tc2") != *m1.get("test", "tc2"))
		FAILED;

	m.processDDL("create table test.tc3(a int primary key,b char(20),c int,d int unique key,e int,unique key uk1(b,c))", nullptr, 24);
	m.processDDL("alter table test.tc3 change column d d1 bigint after e", nullptr, 25);
	m1.processDDL("create table test.tc3(a int primary key,b char(20),c int,e int,d1 bigint unique key,unique key uk1(b,c))", nullptr, 26);
	printf("%s\n", m.get("test", "tc3")->toString().c_str());
	printf("%s\n", m1.get("test", "tc3")->toString().c_str());
	if (*m.get("test", "tc3") != *m1.get("test", "tc3"))
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


