#include "stdio.h"
#include "sqlParser/sqlWord.h"
#include "sqlParser/operationInfo.h"
int testSQLCharWord()
{
	SQL_PARSER::handle h;
	for (char idx = 1; idx < 127; idx++)
	{
		char str[2] = { 0 };
		str[0] = idx;
		const char* s = &str[0];
		SQL_PARSER::SQLCharWord word(false, str);
		SQL_PARSER::SQLValue* value;
		if (idx == ' ' || idx == '\t' || idx == '\n' ||idx=='\r')
			continue;
		if ((value = word.match(&h, s,true)) == NOT_MATCH_PTR ||static_cast<SQL_PARSER::SQLCharValue*>(value)->value!= word.m_word||s!=&str[1])
		{
			printf("test %s failed @%d\n", __FUNCTION__,__LINE__);
			return -1;
		}
		else if(value!=nullptr)
			delete value;
	}
	return 0;
}
#define MATCH_ASSERT_SUCCESS(sql) do{	\
	_sql = sql;value=nullptr;\
	if ((value = word.match(&h, _sql,true)) == NOT_MATCH_PTR)\
	{\
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__);\
		return -1;\
	}}while(0);
#define MATCH_ASSERT_FAIL(sql) do{	\
	_sql = sql;value=nullptr;\
	if ((value = word.match(&h, _sql,true)) != NOT_MATCH_PTR)\
	{\
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__);\
		if(value!=nullptr)delete value;\
		return -1;\
	}}while(0);
int testSQLNameWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLNameWord word(false);
	SQL_PARSER::SQLValue* value;

	MATCH_ASSERT_FAIL("\"name");
	MATCH_ASSERT_FAIL("`name");
	MATCH_ASSERT_FAIL("'name");
	MATCH_ASSERT_FAIL("'name\"");
	MATCH_ASSERT_FAIL("'name`");
	MATCH_ASSERT_FAIL("\"name'");
	MATCH_ASSERT_FAIL("\"name'");
	MATCH_ASSERT_FAIL("`name'");
	MATCH_ASSERT_FAIL("`name\"");
	MATCH_ASSERT_SUCCESS("`name`");
	delete value;
	MATCH_ASSERT_SUCCESS("'name'");
	delete value;
	MATCH_ASSERT_SUCCESS("\"name\"");
	delete value;
	MATCH_ASSERT_SUCCESS("name");
	delete value;
	return 0;
}
#define checkTable(value,db,tb) do{\
	if(value->type!=SQL_PARSER::SQLValueType::TABLE_NAME_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(db==nullptr&&!static_cast<SQL_PARSER::SQLTableNameValue*>(value)->database.empty())\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	else if(static_cast<SQL_PARSER::SQLTableNameValue*>(value)->database!=db)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(tb==nullptr&&!static_cast<SQL_PARSER::SQLTableNameValue*>(value)->table.empty())\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	else if(static_cast<SQL_PARSER::SQLTableNameValue*>(value)->table!=tb)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while (0);

int testSQLTableNameWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLTableNameWord word(false,false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("\"name\".\"name1\"");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`.`name1`");
	checkTable(value, "name", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("'name'.'name1'");
	checkTable(value, "name", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("name.name1");
	checkTable(value, "name", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("\"name1\" ");
	checkTable(value, "", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("'name1' ");
	checkTable(value, "", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("`name1` ");
	checkTable(value, "", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("`name1'` ");
	checkTable(value, "", "name1'");
	delete value;

	MATCH_ASSERT_SUCCESS("'name1'` ");
	checkTable(value, "", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("'name1'` ");
	checkTable(value, "", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("`name1 ` ");
	checkTable(value, "", "name1 ");
	delete value;

	MATCH_ASSERT_SUCCESS("`name`.name1 ");
	checkTable(value, "name", "name1");
	delete value;

	MATCH_ASSERT_FAIL("`name`.`name1 ");
	MATCH_ASSERT_FAIL("'name`.`name1` ");
	MATCH_ASSERT_FAIL("'name ");
	MATCH_ASSERT_FAIL("\"name ");
	return 0;
}
int testSQLTableNameWord1()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLTableNameWord word(false, false, '"');
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("\"name\".\"name1\"");
	checkTable(value, "name", "name1");
	delete value;

	MATCH_ASSERT_SUCCESS("\"name\".name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.\"name1\"");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name");
	checkTable(value, "","name");
	delete value;
	MATCH_ASSERT_SUCCESS("\"name\"");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_SUCCESS("\"name\"");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_FAIL("`name`.`name1`");
	MATCH_ASSERT_FAIL("\"name\".`name1`");
	MATCH_ASSERT_FAIL("\"name\".'name1'");
	MATCH_ASSERT_FAIL("'name1'");
	MATCH_ASSERT_FAIL("`name1`");
	return 0;
}
int testSQLTableNameWord2()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLTableNameWord word(false, false, '\'');
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("'name'.'name1'");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("'name'.name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.'name1'");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_SUCCESS("'name'");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_SUCCESS("'name'");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_FAIL("`name`.`name1`");
	MATCH_ASSERT_FAIL("'name'.`name1`");
	MATCH_ASSERT_FAIL("'name'.\"name1\"");
	MATCH_ASSERT_FAIL("\"name1\"");
	MATCH_ASSERT_FAIL("`name1`");
	return 0;
}
int testSQLTableNameWord3()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLTableNameWord word(false, false, '`');
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("`name`.`name1`");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`.name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.`name1`");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name.name1");
	checkTable(value, "name", "name1");
	delete value;
	MATCH_ASSERT_SUCCESS("name");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`");
	checkTable(value, "", "name");
	delete value;
	MATCH_ASSERT_FAIL("'name'.`name1`");
	MATCH_ASSERT_FAIL("\"name\".`name1`");
	MATCH_ASSERT_FAIL("`name`.\"name1\"");
	MATCH_ASSERT_FAIL("\"name1\"");
	MATCH_ASSERT_FAIL("'name1'");
	return 0;
}
#define checkColumn(value,db,tb,col) do{\
	if(value->type!=SQL_PARSER::SQLValueType::COLUMN_NAME_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(db==nullptr&&!static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->database.empty())\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	else if(static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->database!=db)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(tb==nullptr&&!static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->table.empty())\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	else if(static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->table!=tb)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(col==nullptr&&!static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->columnName.empty())\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	else if(static_cast<SQL_PARSER::SQLColumnNameValue*>(value)->columnName!=col)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while (0);
int testSQLColumnNameWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLColumnNameWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("`name`.`name1`.`name2`");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1`.`name2`");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name2`");
	checkColumn(value, "", "", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`.'name1'.`name2`");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name`.name1.`name2`");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name.name1.name2");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name2");
	checkColumn(value, "", "", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1`.name2");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name1.name2");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1'`.name2");
	checkColumn(value, "", "name1'", "name2");
	delete value;
	MATCH_ASSERT_FAIL("`name1'.name2");
	MATCH_ASSERT_SUCCESS("`name`.`name1.`name2`");
	checkColumn(value,"", "name", "name1.");
	delete value;
	MATCH_ASSERT_FAIL("`name2");
	MATCH_ASSERT_FAIL("`name2`.");
	return 0;
}
int testSQLColumnNameWord1()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLColumnNameWord word(false,'`');
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("`name`.`name1`.`name2`");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1`.`name2`");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name2`");
	checkColumn(value, "", "", "name2");
	delete value;
	MATCH_ASSERT_FAIL("`name`.'name1'.`name2`");
	MATCH_ASSERT_SUCCESS("`name`.name1.`name2`");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name.name1.name2");
	checkColumn(value, "name", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name2");
	checkColumn(value, "", "", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1`.name2");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("name1.name2");
	checkColumn(value, "", "name1", "name2");
	delete value;
	MATCH_ASSERT_SUCCESS("`name1'`.name2");
	checkColumn(value, "", "name1'", "name2");
	delete value;
	MATCH_ASSERT_FAIL("`name1'.name2");
	MATCH_ASSERT_SUCCESS("`name`.`name1.`name2`");
	checkColumn(value, "", "name", "name1.");
	delete value;
	MATCH_ASSERT_FAIL("`name2");
	MATCH_ASSERT_FAIL("`name2`.");
	return 0;
}
#define checkArray_(svalue,str) do{\
	if(svalue->type!=SQL_PARSER::SQLValueType::STRING_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(str==nullptr&&!static_cast<SQL_PARSER::SQLStringValue*>(svalue)->size!=0)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(strlen(str)!=static_cast<SQL_PARSER::SQLStringValue*>(svalue)->size||memcmp(static_cast<SQL_PARSER::SQLStringValue*>(svalue)->value,str,static_cast<SQL_PARSER::SQLStringValue*>(svalue)->size)!=0)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while(0);
#define checkArray(svalue,str) do{\
	checkArray_(svalue,str);\
	delete svalue;\
}while(0);
int testSQLArrayWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLArrayWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("\"qwerty123456\"");
	checkArray(value, "qwerty123456");
	MATCH_ASSERT_SUCCESS("\"���Ĳ���123\"");
	checkArray(value, "���Ĳ���123");
	MATCH_ASSERT_SUCCESS("'qwerty123456'");
	checkArray(value, "qwerty123456");
	MATCH_ASSERT_SUCCESS("'���Ĳ���123'");
	checkArray(value, "���Ĳ���123");
	MATCH_ASSERT_SUCCESS("'qwe\\'���Ĳ���123'");
	checkArray(value, "qwe'���Ĳ���123");
	MATCH_ASSERT_SUCCESS("'qwe\\\"���Ĳ���123'");
	checkArray(value, "qwe\"���Ĳ���123");
	MATCH_ASSERT_SUCCESS("'qwerty123456~`{}:;/.,<>&*))^%$#'");
	checkArray(value, "qwerty123456~`{}:;/.,<>&*))^%$#");
	MATCH_ASSERT_SUCCESS("\"qwerty123456~`{}:;/.,<>&*))^%$#\"");
	checkArray(value, "qwerty123456~`{}:;/.,<>&*))^%$#");
	MATCH_ASSERT_FAIL("\"dwader");
	MATCH_ASSERT_FAIL("\"dwad\\\"er");
	MATCH_ASSERT_FAIL("\"dwad\\\"er'");
	MATCH_ASSERT_FAIL("'dwad\\'er");
	MATCH_ASSERT_FAIL("'dwad\\'er\"");
	MATCH_ASSERT_FAIL("dwad");
	return 0;
}
int testSQLStringWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLStringWord word(false,"asdf123");
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("asdf123");
	delete value;
	MATCH_ASSERT_SUCCESS("ASdf123");
	delete value;
	MATCH_ASSERT_SUCCESS("ASdf123 sda");
	delete value;
	MATCH_ASSERT_SUCCESS("ASdf123(");
	delete value;
	MATCH_ASSERT_SUCCESS("ASdf123,");
	delete value;
	MATCH_ASSERT_FAIL("ASdf12 3");
	return 0;
}
#define checkInt(value,num) do{\
	if(value->type!=SQL_PARSER::SQLValueType::INT_NUMBER_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(num!=static_cast<SQL_PARSER::SQLIntNumberValue*>(value)->number)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while(0);
int testSQLIntNumberWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLIntNumberWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_FAIL("");

	MATCH_ASSERT_SUCCESS("123456790");
	checkInt(value, 123456790);
	delete value;
	MATCH_ASSERT_SUCCESS("-123456790");
	checkInt(value, -123456790);
	delete value;
	MATCH_ASSERT_SUCCESS("-123456790 346");
	checkInt(value, -123456790);
	delete value;
	MATCH_ASSERT_SUCCESS("-123456790-");
	checkInt(value, -123456790);
	delete value;
	MATCH_ASSERT_SUCCESS("+123456790");
	checkInt(value, 123456790);
	delete value;
	MATCH_ASSERT_FAIL("0.123456");
	MATCH_ASSERT_FAIL("-123456.1");
	MATCH_ASSERT_FAIL("123456dwa");
	return 0;
}
#define checkFloat(value,num) do{\
	if(value->type!=SQL_PARSER::SQLValueType::FLOAT_NUMBER_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(abs(num - static_cast<SQL_PARSER::SQLFloatNumberValue*>(value)->number)>abs(num/10000))\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while(0);
int testSQLFloatNumberWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLFloatNumberWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_FAIL("");
	MATCH_ASSERT_SUCCESS("123.214");
	checkFloat(value, 123.214);
	delete value;
	MATCH_ASSERT_SUCCESS("+123.214");
	checkFloat(value, 123.214);
	delete value;
	MATCH_ASSERT_SUCCESS("-123.214");
	checkFloat(value, -123.214);
	delete value;
	MATCH_ASSERT_SUCCESS("123.2e12");
	checkFloat(value, 123.2e12);
	delete value;
	MATCH_ASSERT_SUCCESS("123.2e-12");
	checkFloat(value, 123.2e-12);
	delete value;
	MATCH_ASSERT_SUCCESS("-123.2e-12");
	checkFloat(value, -123.2e-12);
	delete value;
	MATCH_ASSERT_SUCCESS("-123.2e12");
	checkFloat(value, -123.2e12);
	delete value;
	return 0;
}
int testSQLAnyStringWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLAnyStringWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("asdf123 ");
	checkArray(value, "asdf123");

	MATCH_ASSERT_SUCCESS("asdf123");
	checkArray(value, "asdf123");
	MATCH_ASSERT_SUCCESS("asdf123,");
	checkArray(value, "asdf123");
	MATCH_ASSERT_FAIL("\"asdf123\" ");
	MATCH_ASSERT_FAIL("table");
	MATCH_ASSERT_FAIL("create");
	MATCH_ASSERT_FAIL("DROP");
	MATCH_ASSERT_FAIL(",");
	return 0;
}
int testSQLBracketsWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLBracketsWord word(false);
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("(asdf123) ");
	checkArray(value, "asdf123");
	MATCH_ASSERT_SUCCESS("(a(sdf1)23) ");
	checkArray(value, "a(sdf1)23");
	MATCH_ASSERT_SUCCESS("((a)(sdf1)23) ");
	checkArray(value, "(a)(sdf1)23");
	MATCH_ASSERT_SUCCESS("(asdf1((2)3)) ");
	checkArray(value, "asdf1((2)3)");
	MATCH_ASSERT_FAIL("(a(sdf123)() ");
	MATCH_ASSERT_FAIL("asdf1)23");
	return 0;
}
#define checkOpt(value,op) do{\
	if(value->type!=SQL_PARSER::SQLValueType::OPERATOR_TYPE)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
	if(static_cast<SQL_PARSER::SQLOperatorValue*>(value)->opera!=op)\
	{\
			printf("test %s failed @%d\n", __FUNCTION__, __LINE__); \
			return -1; \
	}\
}while(0);

int testSQLOperatorWord()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLOperatorWord word(false);
	SQL_PARSER::SQLValue* value;
	for (int idx = 0; idx < SQL_PARSER::NOT_OPERATION; idx++)
	{
		char sign[10] = { 0 };
		strcpy(sign, SQL_PARSER::operationInfos[idx].signStr);
		MATCH_ASSERT_SUCCESS(sign);
		checkOpt(value, SQL_PARSER::operationInfos[idx].type);
		delete value;
	}
	MATCH_ASSERT_FAIL("qw");
	return 0;
}
#define CHECK_EXP_FIELD_COUNT(exp,cnt)do{\
	if (exp->count != cnt)\
	{\
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__);\
		return -1;\
	}\
}while(0);
int testSQLWordExpressions()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLWordExpressions word(false,false,'`');
	SQL_PARSER::SQLValue* value;
	SQL_PARSER::SQLExpressionValue* exp = nullptr;
	MATCH_ASSERT_FAIL("(a+b)/c+");
	MATCH_ASSERT_FAIL("(a+b");
	MATCH_ASSERT_FAIL("(a+b)+c+(d");
	MATCH_ASSERT_FAIL("(a+b)>c");
	MATCH_ASSERT_SUCCESS("(a+b)+c+d)");
	if (_sql[0] != ')')
	{
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__);
		return -1;
	}
	delete value;

	MATCH_ASSERT_SUCCESS("a+b/c");
	exp = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
	CHECK_EXP_FIELD_COUNT(exp, 5);
	checkColumn(exp->valueStack[0], "", "", "a");
	checkColumn(exp->valueStack[1], "", "", "b");
	checkColumn(exp->valueStack[2], "", "", "c");
	checkOpt(exp->valueStack[3], SQL_PARSER::DIVISION);
	checkOpt(exp->valueStack[4], SQL_PARSER::PLUS);
	delete value;

	MATCH_ASSERT_SUCCESS("(a+m.b)/c");
	exp = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
	CHECK_EXP_FIELD_COUNT(exp, 5);
	checkColumn(exp->valueStack[0], "", "", "a");
	checkColumn(exp->valueStack[1], "", "m", "b");
	checkOpt(exp->valueStack[2], SQL_PARSER::PLUS);
	checkColumn(exp->valueStack[3], "", "", "c");
	checkOpt(exp->valueStack[4], SQL_PARSER::DIVISION);
	delete value;

	MATCH_ASSERT_SUCCESS("a+(5+(a+m.b)/c)*1.2+((23*u-(-24.2*d)))");
	exp = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
	CHECK_EXP_FIELD_COUNT(exp, 19);
	checkColumn(exp->valueStack[0], "", "", "a");
	checkInt(exp->valueStack[1], 5);
	checkColumn(exp->valueStack[2], "", "", "a");
	checkColumn(exp->valueStack[3], "", "m", "b");
	checkOpt(exp->valueStack[4], SQL_PARSER::PLUS);
	checkColumn(exp->valueStack[5], "", "", "c");
	checkOpt(exp->valueStack[6], SQL_PARSER::DIVISION);
	checkOpt(exp->valueStack[7], SQL_PARSER::PLUS);
	checkFloat(exp->valueStack[8], 1.2);
	checkOpt(exp->valueStack[9], SQL_PARSER::MULTIPLE);
	checkOpt(exp->valueStack[10], SQL_PARSER::PLUS);
	checkInt(exp->valueStack[11], 23);
	checkColumn(exp->valueStack[12], "", "", "u");
	checkOpt(exp->valueStack[13], SQL_PARSER::MULTIPLE);
	checkFloat(exp->valueStack[14], -24.2);
	checkColumn(exp->valueStack[15], "", "", "d");
	checkOpt(exp->valueStack[16], SQL_PARSER::MULTIPLE);
	checkOpt(exp->valueStack[17], SQL_PARSER::SUBTRACT);
	checkOpt(exp->valueStack[18], SQL_PARSER::PLUS);
	delete value;


	MATCH_ASSERT_SUCCESS("(a+b)/c-(123*d)");
	exp = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
	CHECK_EXP_FIELD_COUNT(exp, 9);
	checkColumn(exp->valueStack[0], "", "", "a");
	checkColumn(exp->valueStack[1], "", "", "b");
	checkOpt(exp->valueStack[2], SQL_PARSER::PLUS);
	checkColumn(exp->valueStack[3], "", "", "c");
	checkOpt(exp->valueStack[4], SQL_PARSER::DIVISION);
	checkInt(exp->valueStack[5], 123);
	checkColumn(exp->valueStack[6], "", "", "d");
	checkOpt(exp->valueStack[7], SQL_PARSER::MULTIPLE);
	checkOpt(exp->valueStack[8], SQL_PARSER::SUBTRACT);
	delete value;

	MATCH_ASSERT_SUCCESS("(a+b)/c+func(\"dwad\",b.c)");
	exp = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
	CHECK_EXP_FIELD_COUNT(exp, 7);
	checkColumn(exp->valueStack[0], "", "", "a");
	checkColumn(exp->valueStack[1], "", "", "b");
	checkOpt(exp->valueStack[2], SQL_PARSER::PLUS);
	checkColumn(exp->valueStack[3], "", "", "c");
	checkOpt(exp->valueStack[4], SQL_PARSER::DIVISION);
	if (exp->valueStack[5]->type != SQL_PARSER::SQLValueType::FUNCTION_TYPE)
	{
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__); 
		return -1;
	}
	SQL_PARSER::SQLFunctionValue* f = static_cast<SQL_PARSER::SQLFunctionValue*>(exp->valueStack[5]);
	if (f->argvs.size() != 2)
	{
		printf("test %s failed @%d\n", __FUNCTION__, __LINE__);
		return -1;
	}
	std::list<SQL_PARSER::SQLValue*>::iterator iter = f->argvs.begin();

	checkArray_((*iter), "dwad");
	iter++;
	checkColumn((*iter), "", "b", "c");
	checkOpt(exp->valueStack[6], SQL_PARSER::PLUS);

	delete value;
	return 0;
}
int testSQLWordFunction()
{
	const char* _sql;
	SQL_PARSER::handle h;
	SQL_PARSER::SQLWordFunction word(false, '`');
	SQL_PARSER::SQLValue* value;
	MATCH_ASSERT_SUCCESS("f1(a,b,(A+B))");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(a,\"b\",(A+B))");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(a,\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(123456,\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(123456+35,\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(-123456+35,\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(-0.123456+35,\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(f2(123,t),\"b\",(A+B)/C)");
	delete value;
	MATCH_ASSERT_SUCCESS("f1(f2(123,t),f3(1+f2(213,f3(21)),f4(dw,3)),(A+B)/C)");
	delete value;
	MATCH_ASSERT_FAIL("f(a,b,c(d,1)");
	MATCH_ASSERT_FAIL("fa,b,c(d,1))");
	return 0;

}
int main()
{
	initKeyWords();
	if (0 != testSQLCharWord())
		return -1;
	if (0 != testSQLNameWord())
		return -1;
	if (0 != testSQLTableNameWord()||0!= testSQLTableNameWord1()||0!= testSQLTableNameWord2()||0!= testSQLTableNameWord3())
		return -1;
	if (0 != testSQLColumnNameWord()||0!= testSQLColumnNameWord1())
		return -1;
	if (0 != testSQLArrayWord())
		return -1;
	if (0 != testSQLStringWord())
		return -1;
	if (0 != testSQLIntNumberWord())
		return -1;
	if (0 != testSQLFloatNumberWord())
		return -1;
	if (0 != testSQLAnyStringWord())
		return -1;
	if (0 != testSQLBracketsWord())
		return -1;
	if (0 != testSQLOperatorWord())
		return -1;
	if (0 != testSQLWordExpressions())
		return -1;
	if (0 != testSQLWordFunction())
		return -1;
	return 0;
}
