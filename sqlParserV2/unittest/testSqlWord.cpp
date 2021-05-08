#include "sqlParserV2/sqlParser.h"
#include "sqlParserV2/lex.h"
//#include "sqlParserV2/p1.h"
#include "sqlParserV2/p2.h"
int test()
{
	SQL_PARSER::sqlParser p;
	SQL_PARSER::sqlParserStack stack;
	SQL_PARSER::token* t = nullptr;
	p.setDoubleQuoteCanBeString(true);
	p.setIdentifierQuote('`');
	char sql1[] = "a +b *(a+c+m('1',1234))";
	char* pos = sql1;
	if (!dsCheck(p.matchExpression(&stack, t, nullptr, pos)))
	{
		return -1;
	}
	char sql2[] = "-123456";
	pos = sql2;
	if (!dsCheck(p.matchToken(&stack, t, pos,false, true)))
	{
		return -1;
	}
	char sql3[] = "-12.34e56";
	pos = sql3;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql4[] = "+12.34e-56";
	pos = sql4;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql5[] = "+12.34567890";
	pos = sql5;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql6[] = "'abcdefg123456\\'\\\\'";
	pos = sql6;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql7[] = "\"abcdefg123456\\\"\\\\\"";
	pos = sql7;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql8[] = "abcd_1";
	pos = sql8;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	char sql9[] = "`123ads$%^&*(   ` ";
	pos = sql9;
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	return 0;
}
int test1()
{
	SQL_PARSER::lex l;
	if (!dsCheck(l.loadFromFile("D:\\git\\DBStream\\sqlParserV2\\sql\\mysql\\sql"))) 
	//if (!dsCheck(l.loadFromFile("D:\\git\\DBStream\\sqlParserV2\\unittest\\testSql")))
	//if (!dsCheck(l.loadFromFile("sqlParserV2/unittest/testSql")))
	//if (!dsCheck(l.loadFromFile("sqlParserV2/sql/mysql/sql"))) 
		LOG(ERROR)<<getLocalStatus().toString();
	if (!dsCheck(l.optimize()))
		LOG(ERROR) << getLocalStatus().toString();
	l.generateCode("p1f.h", "p1", "p1.h");
	return 0;
}
int test2()
{
	SQL_PARSER::lex l;
	if (!dsCheck(l.loadFromFile("sqlParserV2/unittest/testSql")))
		LOG(ERROR) << getLocalStatus().toString();
	if (!dsCheck(l.optimize()))
		LOG(ERROR) << getLocalStatus().toString();

	SQL_PARSER::lex lr;
	if (!dsCheck(lr.loadFromFile("sqlParserV2/unittest/testSql_expect")))
		LOG(ERROR) << getLocalStatus().toString();
	if (!dsCheck(lr.optimize()))
		LOG(ERROR) << getLocalStatus().toString();
	if (!l.compare(lr))
	{
		return -1;
	}
	return 0;
}
int test4()
{
	SQL_PARSER::lex l;
	if (!dsCheck(l.loadFromFile("D:\\git\\DBStream\\sqlParserV2\\unittest\\testSql1")))
		LOG(ERROR) << getLocalStatus().toString();
	if (!dsCheck(l.optimize()))
		LOG(ERROR) << getLocalStatus().toString();
	l.generateCode("p2f.h", "p2", "p2.h");
	return 0;
}
int test5()
{
	SQL_PARSER::p2 p;
	SQL_PARSER::sqlHandle h;
	char sql[] = "int createTable(int a, char b,string c){int m = a + b;return m;}";
	char* pos = sql;
	if(!dsCheck(p.parse(&h, pos, nullptr)))
		LOG(ERROR) << getLocalStatus().toString();
	return 0;
}
#if 0
int test3()
{
	SQL_PARSER::p1 p;
	SQL_PARSER::sqlHandle h;
	char sql[] = "alter table /*11234*/a add column b";
	char* pos = sql;
	p.parse(&h, pos, nullptr);
	return 0;
}
#endif
int main()
{
	test4();
}
