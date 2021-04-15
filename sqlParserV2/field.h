#pragma once
#include "token.h"
#include "meta/columnType.h"
#include "util/winString.h"
#include "charsetConvert.h"
namespace SQL_PARSER {
	struct field {
		META::COLUMN_TYPE type;
	};
	struct stringField :public field
	{
		uint32_t length;
		uint8_t charset;
		bool ref;
		union {
			char str[8];
			const char* pos;
		};
		static inline int stringCompare(const char* src, uint32_t srcLength, const char* dest, uint32_t destLength, bool casesensitive)
		{
			if (srcLength < destLength)
			{
				int r = casesensitive ? memcmp(src, dest, srcLength) : strncasecmp(src, dest, srcLength);
				return r == 0 ? -1 : r;
			}
			else if (srcLength > destLength)
			{
				int r = casesensitive ? memcmp(src, dest, destLength) : strncasecmp(src, dest, destLength);
				return r == 0 ? 1 : r;
			}
			else
			{
				return casesensitive ? memcmp(src, dest, srcLength) : strncasecmp(src, dest, srcLength);
			}
		}
		inline DS compare(const stringField& d, bool casesensitive, int& result)
		{
			if (charset != d.charset)
			{
				dsReturn(globalCharsetConvert.compare((CHARSET)charset, getStr(), length, (CHARSET)d.charset, d.getStr(), d.length, casesensitive, result));
			}
			else
			{
				result = stringCompare(getStr(), length, d.getStr(), d.length, casesensitive);
				dsOk();
			}
		}
		stringField* convert(uint8_t charset)
		{
			uint32_t charLength = 0;
			if (!dsCheck(charsetConvert::strCharLength((CHARSET)charset, getStr(),length, charLength)))
			{
				resetStatus();
				return nullptr;
			}
			stringField* dest = (stringField*)malloc(sizeof(stringField) + charLength * charsets[charset].byteSizePerChar);
			int destCharLength = globalCharsetConvert.convert((CHARSET)this->charset, getStr(), length, (CHARSET)charset, dest->str, charLength * charsets[charset].byteSizePerChar + 1, dest->length);
			if (destCharLength < 0)
			{
				free(dest);
				return nullptr;
			}
			dest->type = META::COLUMN_TYPE::T_STRING;
			dest->charset = charset;
			dest->ref = false;
			return dest;
		}
		inline const char* getStr() const
		{
			return ref ? pos : str;
		}
	};

	struct binaryField :public field {
		uint32_t length;
		bool ref;
		union {
			char bin[8];
			const char* pos;
		};
		inline const char* getValue() const
		{
			return ref ? pos : bin;
		}
	};

	struct funcField :public field {
		uint8_t value;
	};

	struct 	uint8Field :public field {
		uint8_t value;
	};
	struct 	int8Field :public field {
		int8_t value;
	};
	struct 	uint16Field :public field {
		uint16_t value;
	};
	struct 	int16Field :public field {
		int16_t value;
	};
	struct 	uint32Field :public field {
		uint32_t value;
	};
	struct 	int32Field :public field {
		int32_t value;
	};
	struct 	uint64Field :public field {
		uint64_t value;
	};
	struct 	int64Field :public field {
		int64_t value;
	};

}