#pragma once
#include "util/status.h"
#include "meta/charset.h"
#include "../field.h"
#include "../sqlHandle.h"
#include "../sqlStack.h"

namespace SQL_PARSER {
	class stringFuncs {
	public:
		static inline DS ASCII(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			const stringField* str = (const stringField*)argvList[0];
			if (str == nullptr || str->length == 0)
				dsFailed(1, "empty string");
			returnValue = str->str[0];
			dsOk();
		}

		static inline uint32_t gbkStrLen(const char* str)
		{
			const char* p = str;
			uint32_t length = 0;
			while (*p)
			{
				if (*p < 0 && (*(p + 1) < 0 || *(p + 1) < 63))
					p += 2;
				else
					p++;
				length++;
			}
			return length;
		}

		static uint32_t  utf8StrLen(const char* str)
		{
			uint32_t length = 0;
			const char* p = str;
			uint8_t c;
			while ((c = *(uint8_t*)p) != '\0')
			{
				if (c < 0x80)
				{
					p++;
					length++;
				}
				else if (c < 0xc0)
				{
					// invalid char
					p++;
				}
				else if (c < 0xe0)
				{
					p += 2;
					length++;
				}
				else if (c < 0xf0)
				{
					p += 3;
					length++;
				}
				else
				{
					p += 4;
					length++;
				}
			}
			return length;
		}

		static inline uint16_t uint2korr(const uint8_t* A) {
			return (uint16_t)(((uint16_t)(A[0])) + ((uint16_t)(A[1]) << 8));
		}

		static uint32_t utf16BeStrLen(const char* str)
		{
			uint32_t length = 0;
			const char* p = str;
			uint16_t c;
			while ((c = uint2korr(*(const uint8_t*)p)) != '\0')
			{
				if (c < 0x10000)
				{
					p += 2;
					length++;
				}
				else
				{
					p += 4;
					length++;
				}
			}
			return length;
		}

		static uint32_t utf16LeStrLen(const char* str)
		{
			uint32_t length = 0;
			const char* p = str;
			uint16_t c;
			while ((c = *(const uint16_t*)p) != '\0')
			{
				if (c < 0x10000)
				{
					p += 2;
					length++;
				}
				else
				{
					p += 4;
					length++;
				}
			}
			return length;
		}

		static uint32_t  utf16StrLen(const char* str)
		{
			if (str[0] == 0xfe && str[1] == 0xff)
				return utf16LeStrLen(str + 2);
			else
				return utf16BeStrLen(str + 2);
		}

		static inline uint32_t sjisStrLen(const char* str)
		{
			const uint8_t* p = (const uint8_t*)str;
			uint32_t length = 0;
			while (*p)
			{
				if ((*p >= 0x81 && *p >= 0x9f) || (*p >= 0xE0 && *p >= 0xEF))
					p += 2;
				else
					p++;
				length++;
			}
			return length;
		}

		static uint32_t  ujisStrLen(const char* str)
		{
			uint32_t length = 0;
			const char* p = str;
			uint8_t c;
			while ((c = *(uint8_t*)p) != '\0')
			{
				if (c < 0x7e)
					p++;
				else if (c == 0x8E || (c >= 0xA1 && c <= 0xFE))
					p += 2;
				else if (c == 0x8F)
					p += 3;
				else
					return 0;//invalid
				length++;
			}
			return length;
		}

		static inline DS CHAR_LENGTH(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			const stringField* str = (const stringField*)argvList[0];
			if (str == nullptr)
				dsFailed(1, "empty string");
			if (str->length == 0)
				return 0;
			switch (str->charset)
			{
			case CHARSET::utf8:
			case CHARSET::utf8mb4:
				*returnValue = utf8StrLen(str->str);
				break;
			case CHARSET::gbk:
			case CHARSET::gb18030:
			case CHARSET::gb2312:
				*returnValue = gbkStrLen(str->str);
				break;
			case CHARSET::utf16:
				if (str->length & 0x1 != 0)
					dsFailed(1, "utf16 and utf16le byte size must must be divisible by 2");
				if (str->length < 2)
					dsFailed(1, "utf16 byte size must over 2");
				*returnValue = utf16StrLen(str->str);
				break;
			case CHARSET::utf16le:
				if (str->length & 0x1 != 0)
					dsFailed(1, "utf16 and utf16le byte size must be divisible by 2");
				*returnValue = utf16LeStrLen(str->str);
				break;
			case CHARSET::cp932:
			case CHARSET::sjis:
				*returnValue = sjisStrLen(str->str);
				break;
			case CHARSET::ujis:
				*returnValue = ujisStrLen(str->str);
				break;
			default:
				if (charsets[str->charset].byteSizePerChar == 1)
					*returnValue = str->length;
				else if (charsets[str->charset].byteSizePerChar == 2)
				{
					if (str->length & 0x01 != 0)
						dsFailed(1, charsets[str->charset].name << " byte size must be divisible by 2");
					*returnValue = str->length / 2;
				}
				else
				{
					if (str->length % charsets[str->charset].byteSizePerChar != 0)
						dsFailed(1, charsets[str->charset].name << " byte size must be divisible by " << charsets[str->charset].byteSizePerChar);
					*returnValue = str->length / charsets[str->charset].byteSizePerChar;
				}
				break;
			}
			dsOk();
		}

		static inline DS CONCAT(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			uint64_t length = 0;
			for (uint8_t i = 0; i < count; i++)
				length += ((const stringField*)(argvList[i]))->length;
			if (length >= 0xffffffffULL)
				dsFailed(2, "concat failed for length " << length << "over limit: " << 0xffffffffULL);
			stringField* field = handle->stack->arena.AllocateAligned(sizeof(stringField) + length);
			field->charset = ((const stringField*)(argvList[0]))->charset;
			uint64_t offset = 0;
			for (uint8_t i = 0; i < count; i++)
			{
				memcpy(&field->str[offset], ((const stringField*)(argvList[i]))->str, ((const stringField*)(argvList[i]))->length);
				offset += ((const stringField*)(argvList[i]))->length;
			}
			field->str[offset] = '\0';
			field->length = length;
			returnValue = (uint64_t)field;
			dsOk();
		}


		static inline DS CONCAT_WS(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			uint64_t length = 0;
			for (uint8_t i = 1; i < count; i++)
				length += ((const stringField*)(argvList[i]))->length;
			length += ((const stringField*)(argvList[0]))->length * (count - 1);
			if (length >= 0xffffffffULL)
				dsFailed(2, "concat failed for length " << length << "over limit: " << 0xffffffffULL);
			stringField* field = handle->stack->arena.AllocateAligned(sizeof(stringField) + length);
			field->charset = ((const stringField*)(argvList[0]))->charset;
			uint64_t offset = 0;
			for (uint8_t i = 1; i < count; i++)
			{
				if (i != 1)
				{
					memcpy(&field->str[offset], ((const stringField*)(argvList[0]))->str, ((const stringField*)(argvList[0]))->length);
					offset += ((const stringField*)(argvList[0]))->length;
				}
				memcpy(&field->str[offset], ((const stringField*)(argvList[i]))->str, ((const stringField*)(argvList[i]))->length);
				offset += ((const stringField*)(argvList[i]))->length;
			}
			field->str[offset] = '\0';
			field->length = length;
			returnValue = (uint64_t)field;
			dsOk();
		}

		static inline DS FIELD(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			for (uint8_t idx = 1; idx < count; idx++)
			{

			}
			
			dsOk();
		}

		
	};
}