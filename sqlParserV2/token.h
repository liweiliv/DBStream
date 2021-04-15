#pragma once
#include <stdint.h>
#include "util/arena.h"
#include "util/winDll.h"
#include "util/winString.h"
#include "util/status.h"
#include "str.h"
#include "separator.h"
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


	struct operationInfo;
	struct operatorSymbol :public token {
		const operationInfo* op;
	};

	struct keyWord :public token
	{
		static bool match(const char* s, const char* d, uint16_t size)
		{
			if (strncasecmp(s, d, size) != 0)
				return false;
			if (s[size] == '\0' || isSeparator(s + size) || KEY_CHAR[(uint8_t)s[size]])
				return true;
			else
				return false;
		}
	};

	struct identifier :public token
	{
		uint8_t count;
		str identifiers[3];
		inline void init(const char* pos, uint32_t size)
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
		static bool matchNonDelimitedIdentifier(token*& t, char quote, const char*& pos, leveldb::Arena* arena, bool funcName)
		{
			const char* start = pos;
			char c = *pos;
			//first char must be a-z or U+0080 .. U+FFFF
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) && c < 0x80)
				return false;
			//a-z A-Z 0-9 $#_
			while ((c = *pos) && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '1' && c <= '9') || c == '$' || c == '#' || c == '_' || c > 0x80))
				pos++;
			if (funcName)
			{
				t = (token*)arena->AllocateAligned(sizeof(token));
				t->value.assign(start, pos - start);
			}
			else
			{
				if (t == nullptr)
				{
					t = (identifier*)arena->AllocateAligned(sizeof(identifier));
					static_cast<identifier*>(t)->init(start, pos - start);
				}
				else
				{
					static_cast<identifier*>(t)->add(start, pos - start);
				}
			}
			return true;
		}
		static bool matchDelimitedIdentifier(token*& t, char quote, const char*& pos, leveldb::Arena* arena)
		{
			if (*pos != quote)
				return false;
			pos++;
			const char* start = pos;
			bool backslash = false;
			while (*pos != '\0')
			{
				if (unlikely(*pos == '\\'))
					backslash = !backslash;
				else if (*pos == quote)
				{
					if (likely(!backslash))
					{
						if (t == nullptr)
						{
							t = (identifier*)arena->AllocateAligned(sizeof(identifier));
							static_cast<identifier*>(t)->init(start, pos - start);
						}
						else
						{
							static_cast<identifier*>(t)->add(start, pos - start);
						}
						pos++;
						return true;
					}
					else
						backslash = false;
				}
				pos++;
			}
			return false;
		}

		static bool matchIdentifier(token*& t, char quote, const char*& pos, leveldb::Arena* arena, bool funcName)
		{
			for (char i = 0; i < 3; i++)
			{
				if (*pos == quote)
				{
					if (!matchDelimitedIdentifier(t, quote, pos, arena))
						return false;
				}
				else
				{
					if (!matchNonDelimitedIdentifier(t, quote, pos, arena, funcName))
						return false;
				}
				if (*pos != '.')
					break;
			}
			return true;
		}
		static inline identifier* match(const char*& sql, char quote, leveldb::Arena* arena)
		{
			char c = *sql;
			if (c != quote && (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) && c < 0x80))
				return nullptr;
			token* t = nullptr;
			matchIdentifier(t, quote, sql, arena, false);
			return static_cast<identifier*>(t);
		}

		std::string toString()
		{
			std::string s;
			for (int i = 0; i < count; i++)
			{
				if (i != 0)
					s.append(".");
				s.append(identifiers[i].toString());
			}
			return s;
		}
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
	constexpr static const char* const literalTypeStr[] = {
		"INT NUMBER",
		"FLOAT NUMBER",
		"NUMBER"
		"CHARACTER STRING",
		"NATIONAL CHARACTER STRING",
		"BIT STRING",
		"HEX STRING",
		"TIME",
		"TIMESTAMP",
		"DATE",
		"INTERVAL",
		"BOOLEAN",
		"FUNCTION",
		"EXPRESSION",
		"ANY STRING",
		"ALL VALUE",
		"UNKNOWN"
	};
	struct literal :public token
	{
		literalType lType;
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

}
