#include "sqlParserV2/sqlParser.h"
#include "sqlParserV2/lex.h"
int test()
{
	SQL_PARSER::sqlParser p;
	SQL_PARSER::sqlParserStack stack;
	SQL_PARSER::token* t = nullptr;
	p.setDoubleQuoteCanBeString(true);
	p.setIdentifierQuote('`');
	const char* pos = "a +b *(a+c+m('1',1234))";
	if (!dsCheck(p.matchExpression(&stack, t, nullptr, pos)))
	{
		return -1;
	}
	pos = "-123456";
	if (!dsCheck(p.matchToken(&stack, t, pos,false, true)))
	{
		return -1;
	}
	pos = "-12.34e56";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "+12.34e-56";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "+12.34567890";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "'abcdefg123456\\'\\\\'";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "\"abcdefg123456\\\"\\\\\"";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "abcd_1";
	if (!dsCheck(p.matchToken(&stack, t, pos, false, true)))
	{
		return -1;
	}
	pos = "`123ads$%^&*(   ` ";
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
		LOG(ERROR)<<getLocalStatus().toString();
	if (!dsCheck(l.optimize()))
		LOG(ERROR) << getLocalStatus().toString();



	return 0;
}
int test2()
{
	SQL_PARSER::lex l;
	if (!dsCheck(l.loadFromFile("D:\\git\\DBStream\\sqlParserV2\\unittest\\testsql")))
		LOG(ERROR) << getLocalStatus().toString();
	if (!dsCheck(l.optimize()))
		LOG(ERROR) << getLocalStatus().toString();

	SQL_PARSER::lex lr;
	if (!dsCheck(lr.loadFromFile("D:\\git\\DBStream\\sqlParserV2\\unittest\\testsql_expect")))
		LOG(ERROR) << getLocalStatus().toString();
	if (!dsCheck(lr.optimize()))
		LOG(ERROR) << getLocalStatus().toString();
	if (!l.compare(lr))
	{
		return -1;
	}
	return 0;
}
int main()
{
	test2();
}