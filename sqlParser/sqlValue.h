#pragma once
#include <string>
#include <list>
#include "operationInfo.h"
namespace SQL_PARSER {
	class SQLValue {
	public:
		enum SQLValueType {
			NUMBER_TYPE,
			STRING_TYPE,
			NAME_TYPE,
			TABLE_NAME_TYPE,
			COLUMN_NAME_TYPE,
			FUNCTION_TYPE,
			EXPRESSION_TYPE
		};
		SQLValueType type;
		uint32_t ref;
		SQLValue(SQLValueType type) :type(type), ref(0){}
		virtual ~SQLValue() {}
	};
	class SQLStringValue :public SQLValue {
	public:
		std::string value;
		SQLStringValue(SQLValueType type) :SQLValue(type) {}
	};
	class SQLDBNameValue :public SQLValue {
	public:
		std::string database;
		SQLDBNameValue() :SQLValue(NAME_TYPE) {}
	};
	class SQLTableNameValue :public SQLValue {
	public:
		std::string database;
		std::string table;
		SQLTableNameValue() :SQLValue(TABLE_NAME_TYPE) {}
	};
	class SQLColumnNameValue :public SQLValue {
	public:
		std::string database;
		std::string table;
		std::string columnName;
		SQLColumnNameValue() :SQLValue(COLUMN_NAME_TYPE) {}
	};
	class SQLFunctionValue :public SQLValue {
	public:
		std::string funcName;
		std::list<SQLValue*> argvs;
		SQLFunctionValue() :SQLValue(FUNCTION_TYPE) {}
		~SQLFunctionValue()
		{
			for (std::list<SQLValue*>::iterator iter = argvs.begin(); iter != argvs.end(); iter++)
				delete* iter;
		}
	};
	class SQLExpressionValue :public SQLValue {
	public:
		SQLValue *rightValue;
		OPERATOR opt;
		SQLValue *leftValue;
		SQLExpressionValue() :SQLValue(EXPRESSION_TYPE), rightValue(nullptr), leftValue(nullptr){}
		~SQLExpressionValue()
		{
			if (rightValue)
				delete rightValue;
			if (leftValue)
				delete leftValue;
		}
	};
}
