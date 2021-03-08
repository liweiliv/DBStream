#pragma once
#include <stdint.h>
#include "util/winDll.h"
#include "util/winString.h"
#include "util/status.h"
#include "str.h"
namespace SQL_PARSER
{
	constexpr static bool KEY_CHAR[256] = {
		true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
		true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
		true, false, true, false, false, true, true, true, true, true, true, true, true, true, true, true,
		false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true,
		false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
		false, false, false, false, false, false, false, false, false, false, false, true, false, false, true, false,
		true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
		false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, false 
	};
	enum class tokenType {
		literal,
		keyword,
		identifier,
		specialCharacter,
		symbol
	};
	DLL_EXPORT struct token {
		tokenType type;
		str value;
		DLL_EXPORT bool compare(const token& t);
	};

	enum class literalType {
		INT_NUMBER,
		FLOAT_NUMBER,
		NUMBER_VALUE,
		CHARACTER_STRING,
		NATIONAL_CHARACTER_STRING,
		BIT_STRING,
		HEX_STRING,
		TIME,
		TIMESTAMP,
		DATE,
		INTERVAL,
		BOOLEAN,
		FUNCTION,
		EXPRESSION,
		ANY_STRING,
		ALL_VALUE,
		UNKNOWN
	};
	struct literal :public token
	{
		literalType lType;
	};
	struct generalLiteral :public literal
	{

	};
	struct expression :public literal
	{
		bool booleanOrValue;
		uint32_t count;
		token* valueList[1];
	};
	struct function :public literal
	{
		str name;
		uint32_t count;
		token* argv[1];
	};

	struct cast :public literal
	{
		inline static bool match(literal& l, const char*& pos)
		{

		}
	};

	struct operationInfo;
	struct operatorSymbol :public token {
		const operationInfo* op;
	};
	



	struct keyWord :public token
	{
	};

	struct identifier :public token
	{
		char count;
		str identifiers[3];
		inline void init(const char * pos,uint32_t size)
		{
			type = tokenType::identifier;
			value.assign(nullptr, 0);
			count = 1;
			identifiers[0].assign(pos, size);
		}
		inline void add(const char* pos, uint32_t size)
		{
			identifiers[count++].assign(pos, size);
		}
	};
}
