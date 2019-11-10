#pragma once
#include <string>
#include <list>
#include <stack>
#include <assert.h>
#include "operationInfo.h"
namespace SQL_PARSER {
	enum SQLValueType {
		OPERATOR_TYPE,
		INT_NUMBER_TYPE,
		FLOAT_NUMBER_TYPE,
		STRING_TYPE,
		CHAR_TYPE,
		LIST_TYPE,
		NAME_TYPE,
		TABLE_NAME_TYPE,
		COLUMN_NAME_TYPE,
		FUNCTION_TYPE,
		EXPRESSION_TYPE,
		MAX_TYPE
	};
	class SQLValue {
	public:

		SQLValueType type;
		uint32_t ref;
		SQLValue(SQLValueType type) :type(type), ref(0){}
		virtual ~SQLValue() {}
	};
	class SQLIntNumberValue :public SQLValue
	{
	public:
		int64_t number;
		SQLIntNumberValue(int64_t number):SQLValue(INT_NUMBER_TYPE), number(number){}
	};
	class SQLFloatNumberValue :public SQLValue
	{
	public:
		double number;
		SQLFloatNumberValue(double number) :SQLValue(FLOAT_NUMBER_TYPE), number(number) {}
	};
	class SQLOperatorValue :public SQLValue {
	public:
		OPERATOR opera;
		SQLOperatorValue(const char* op) :SQLValue(OPERATOR_TYPE)
		{
			opera = parseOperation(op);
		}
		SQLOperatorValue(OPERATOR opera) :SQLValue(OPERATOR_TYPE), opera(opera)
		{}
	};
	class SQLValueList :public SQLValue {
	public:
		std::list<SQLValue*> values;
		SQLValueList() :SQLValue(LIST_TYPE) {}
		~SQLValueList()
		{
			for (std::list<SQLValue*>::iterator iter = values.begin(); iter != values.end(); iter++)
				delete* iter;
		}
	};
	class SQLCharValue :public SQLValue {
	public:
		char value;
		SQLCharValue() :SQLValue(CHAR_TYPE), value(0) {}
	};
	class SQLStringValue :public SQLValue {
	public:
		char* value;
		uint32_t size;
		uint32_t volumn;
		bool quote;
		SQLStringValue(SQLValueType type) :SQLValue(type), value(nullptr),size(0), volumn(0), quote(false){}
		inline const char* assign(const char* src, uint32_t size)
		{
			if (!quote && volumn > size)
			{
				memcpy(value, src, size);
				value[size] = '\0';
			}
			else
			{
				quote = false;
				if (value != nullptr)
					free(value);
				value = (char*)malloc(size + 1);
				memcpy(value, src, size);
				value[size] = '\0';
				volumn = this->size = size;
			}
			return value;
		}
		~SQLStringValue()
		{
			if (!quote && value != nullptr)
				free(value);
		}
	};
	class SQLNameValue :public SQLValue {
	public:
		std::string name;
		SQLNameValue() :SQLValue(NAME_TYPE) {}
	};
	class SQLTableNameValue :public SQLValue {
	public:
		std::string database;
		std::string table;
		std::string alias;
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
		SQLValue** valueStack;
		uint16_t count;
		SQLExpressionValue() :SQLValue(EXPRESSION_TYPE), valueStack(nullptr), count(0){}
		~SQLExpressionValue()
		{
			if (valueStack != nullptr)
			{
				for (int idx = 0; idx < count; idx++)
				{
					if (valueStack[idx]!=nullptr&&--valueStack[idx]->ref <= 0)
						delete valueStack[idx];
				}
				delete[]valueStack;
			}
		}
		bool check()
		{
			if (valueStack == nullptr || count == 0)
				return true;
			int lValueCount = 0;
			for (int idx = 0; idx < count; idx++)
			{
				if (valueStack[idx]->type == SQLValueType::OPERATOR_TYPE)
				{
					if (!operationInfos[static_cast<SQLOperatorValue*>(valueStack[idx])->opera].hasLeftValues)
					{
						if (lValueCount < 1)
							return false;
						continue;
					}
					else
					{
						if (lValueCount < 2)
							return false;
						lValueCount--;
					}
				}
				else
					lValueCount++;
			}
			if (lValueCount != 1)
				return false;
			else
				return true;
		}
	};
}
