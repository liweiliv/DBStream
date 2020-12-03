#pragma once
#include <string>
#include <list>
#include <stack>
#include <assert.h>
#include "operationInfo.h"
namespace SQL_PARSER {
	enum class SQLValueType {
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
	struct stringValue {
		bool quote;
		const char* name;
		uint32_t size;
		stringValue():name(nullptr),size(0),quote(true){}
		inline void assign(const char* name, uint32_t size,bool quote = true)
		{
			if (!quote && name != nullptr)
				delete[](char*)name;
			this->name = name;
			this->size = size;
			this->quote = quote;
		}
		inline stringValue& operator=(const stringValue& v)
		{
			if (!quote && name != nullptr)
				delete[](char*)name;
			this->name = v.name;
			this->size = v.size;
			quote = true;
			return *this;
		}
		~stringValue()
		{
			if (!quote && name != nullptr)
				delete[](char*)name;
		}
		inline std::string& toString()
		{
			return std::string(name, size);
		}
	};
	class SQLValue {
	public:
		SQLValueType type;
		int32_t ref;
		SQLValue(SQLValueType type) :type(type), ref(0) {}
		virtual ~SQLValue() {}
	};
	class SQLIntNumberValue :public SQLValue
	{
	public:
		int64_t number;
		SQLIntNumberValue(int64_t number) :SQLValue(SQLValueType::INT_NUMBER_TYPE), number(number) {}
	};
	class SQLFloatNumberValue :public SQLValue
	{
	public:
		double number;
		SQLFloatNumberValue(double number) :SQLValue(SQLValueType::FLOAT_NUMBER_TYPE), number(number) {}
	};
	class SQLOperatorValue :public SQLValue {
	public:
		OPERATOR opera;
		SQLOperatorValue(const char* op) :SQLValue(SQLValueType::OPERATOR_TYPE)
		{
			opera = parseOperation(op);
		}
		SQLOperatorValue(OPERATOR opera) :SQLValue(SQLValueType::OPERATOR_TYPE), opera(opera)
		{}
	};
	class SQLValueList :public SQLValue {
	public:
		std::list<SQLValue*> values;
		SQLValueList() :SQLValue(SQLValueType::LIST_TYPE) {}
		~SQLValueList()
		{
			for (std::list<SQLValue*>::iterator iter = values.begin(); iter != values.end(); iter++)
				delete* iter;
		}
	};
	class SQLCharValue :public SQLValue {
	public:
		char value;
		SQLCharValue() :SQLValue(SQLValueType::CHAR_TYPE), value(0) {}
	};
	class SQLStringValue :public SQLValue {
	public:
		stringValue value;
		SQLStringValue(SQLValueType type) :SQLValue(type), value(){}
		inline const char* assign(const char* src, uint32_t size)
		{
			value.assign(src, size);
			return src;
		}
		~SQLStringValue()
		{
		}
	};
	class SQLNameValue :public SQLValue {
	public:
		stringValue name;
		SQLNameValue() :SQLValue(SQLValueType::NAME_TYPE) {}
	};
	class SQLTableNameValue :public SQLValue {
	public:
		stringValue database;
		stringValue table;
		stringValue alias;
		SQLTableNameValue() :SQLValue(SQLValueType::TABLE_NAME_TYPE) {}
	};
	class SQLColumnNameValue :public SQLValue {
	public:
		stringValue database;
		stringValue table;
		stringValue columnName;
		SQLColumnNameValue() :SQLValue(SQLValueType::COLUMN_NAME_TYPE) {}
	};
	constexpr static int MAX_FUNC_ARGV_COUNT = 32;
	class SQLFunctionValue :public SQLValue {
	public:
		stringValue funcName;
		SQLValue* argvs[MAX_FUNC_ARGV_COUNT];
		uint8_t argvCount;
		SQLFunctionValue() :SQLValue(SQLValueType::FUNCTION_TYPE) {}
		~SQLFunctionValue()
		{
			for (int i = 0; i < argvCount; argvCount++)
				delete argvs[i];
			argvCount = 0;
		}
	};
	class SQLExpressionValue :public SQLValue {
	public:
		SQLValue** valueStack;
		uint16_t count;
		SQLExpressionValue() :SQLValue(SQLValueType::EXPRESSION_TYPE), valueStack(nullptr), count(0) {}
		~SQLExpressionValue()
		{
			if (valueStack != nullptr)
			{
				for (int idx = 0; idx < count; idx++)
				{
					if (valueStack[idx] != nullptr && --valueStack[idx]->ref <= 0)
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
