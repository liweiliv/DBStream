#pragma once
#include <iconv.h>
#include "meta/charset.h"
#include "util/winString.h"
#include "util/status.h"
namespace SQL_PARSER
{
	class charsetConvert
	{
	private:
		iconv_t m_converts[CHARSET::MAX_CHARSET][CHARSET::MAX_CHARSET];
	public:
		charsetConvert()
		{
			memset(m_converts, 0, sizeof(m_converts));
			for (int i = 0; i < CHARSET::MAX_CHARSET; i++)
			{
				if (i != CHARSET::utf8)
				{
					m_converts[i][CHARSET::utf8] = iconv_open("UTF-8", charsets[i].name);
					m_converts[CHARSET::utf8][i] = iconv_open(charsets[i].name, "UTF-8");
				}
			}
		}

		inline size_t convert(CHARSET srcType, const char* src, size_t length, CHARSET destType, char* dest, size_t destVolumn, uint32_t& destSize) const
		{
			size_t convCharCount = 0;
			iconv_t cv = m_converts[srcType][destType];
			if (cv == nullptr)
			{
				iconv_t scv = m_converts[srcType][CHARSET::utf8];
				iconv_t dcv = m_converts[CHARSET::utf8][destType];
				if (scv == nullptr || dcv == nullptr)
					return -1;
				char tmpBuf[1024];
#if defined OS_LINUX
				char* srcPos = (char*)src;
#else
				const char* srcPos = src;
#endif
				char* destPos = dest;
				size_t srcRemain = length;
				size_t destRemain = destVolumn;
				do
				{
					if (srcRemain > 0 && destRemain <= 0)
						return -1;
					size_t tmpRemain = sizeof(tmpBuf);
					char* tmpWPos = tmpBuf;
					int charCount = iconv(scv, &srcPos, &srcRemain, &tmpWPos, &tmpRemain);
					if (charCount < 0)
						return -1;
#if defined OS_LINUX
					char* tmpRPos = srcPos;
#else
					const char* tmpRPos = srcPos;
#endif
					tmpRemain = sizeof(tmpBuf) - tmpRemain;
					int destCharCount = iconv(dcv, &tmpRPos, &srcRemain, &destPos, &destRemain);
					if (destCharCount < 0)
						return -1;
					else if (destCharCount == 0 && (tmpRemain > 0 && *tmpRPos != '\0'))
						return -2;
					convCharCount += destCharCount;
				} while (*srcPos != '\0');
				destSize = destPos - dest;
				if (destSize < destRemain)
					*destPos = '\0';
				return convCharCount;
			}
			else
			{
#if defined OS_LINUX
				char* srcPos = (char*)src;
#else
				const char* srcPos = src;
#endif
				char* destPos = dest;
				size_t srcRemain = length;
				size_t destRemain = destVolumn;
				int charCount = iconv(cv, &srcPos, &srcRemain, &destPos, &destRemain);
				if (charCount < 0)
					return -1;
				else if ((srcRemain > 0 && *srcPos != '\0'))
					return -2;
				destSize = destPos - dest;
				return charCount;
			}
		}
		static inline uint32_t gbkStrlen(const char* str)
		{
			const char* p = str;
			uint32_t length = 0;
			while (*p)
			{
				if (*p < 0 && (*(p + 1) < 0 || *(p + 1) < 63))
				{
					str++;
					p += 2;
				}
				else
				{
					p++;
				}
				length++;
			}
			return length;
		}
		static uint32_t  utf8Strlen(const char* str)
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

		static uint32_t  utf16Strlen(const char* str)
		{
			uint32_t length = 0;
			const char* p = str;
			uint8_t c = *p;
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

		static uint32_t utf16BeStrlen(const char* str)
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
		static uint32_t utf16leStrlen(const char* str)
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

		static inline DS strCharLength(CHARSET charset,const char * str,uint32_t length, uint32_t& returnValue)
		{
			if (str == nullptr)
				dsFailed(1, "empty string");
			if (length == 0)
				return 0;
			switch (charset)
			{
			case CHARSET::utf8:
			case CHARSET::utf8mb4:
				returnValue = utf8Strlen(str);
				break;
			case CHARSET::ascii:
			case CHARSET::binary:
				returnValue = length;
				break;
			case CHARSET::gbk:
			case CHARSET::gb18030:
			case CHARSET::gb2312:
				returnValue = gbkStrlen(str);
				break;
			case CHARSET::utf16:
				if ((length & 0x1) != 0)
					dsFailed(1, "utf16 and utf16le byte size must be even number");
				if (length < 2)
					dsFailed(1, "utf16 byte size must over 2");
				returnValue = (length - 2) >> 1;
				break;
			case CHARSET::utf16le:
				if ((length & 0x1) != 0)
					dsFailed(1, "utf16 and utf16le byte size must be even number");
				returnValue = utf16leStrlen(str);
				break;
			default:
				returnValue = length / charsets[charset].byteSizePerChar;
				break;
			}
			dsOk();
		}
		DS compareByUtf8(const char* src, size_t srcLength, CHARSET destType, const char* dest, size_t destLength, bool casesensitive, int& result) const;
		DS compare(CHARSET srcType, const char* src, size_t srcLength, CHARSET destType,const char* dest, size_t destLength, bool casesensitive, int& result) const;
	};
	extern charsetConvert globalCharsetConvert;
}
