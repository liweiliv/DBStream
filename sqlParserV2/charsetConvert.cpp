#include "util/min.h"
#include "charsetConvert.h"
#include "field.h"
namespace SQL_PARSER {
	charsetConvert globalCharsetConvert;
	DS charsetConvert::compareByUtf8(const char* src, size_t srcLength, CHARSET destType, const char* dest, size_t destLength, bool casesensitive, int& result) const
	{
		if (destType == CHARSET::utf8)
		{
			result = stringField::stringCompare(src, srcLength, dest, destLength, casesensitive);
			dsOk();
		}
		else
		{
			iconv_t dcv = m_converts[CHARSET::utf8][destType];
			if (dcv == nullptr)
				dsFailed(-1, "can not convert " << charsets[destType].name << " to utf8");
			char tmpBuf[1024];
			const char* srcPos = src;
#if defined OS_LINUX
			char* destPos = (char*)dest;
#else
			const char* destPos = dest;
#endif 
			size_t srcRemain = srcLength;
			size_t destRemain = destLength;
			do
			{
				if (srcRemain > 0 && destRemain <= 0)
				{
					result = 1;
					dsOk();
				}
				size_t tmpRemain = sizeof(tmpBuf);
				char* tmpWPos = tmpBuf;
				int charCount = iconv(dcv, &destPos, &destRemain, &tmpWPos, &tmpRemain);
				if (charCount < 0)
					dsFailed(-1, "can not convert " << charsets[destType].name << " to utf8");
				int cl = std::min<size_t>(srcRemain, sizeof(tmpBuf) - tmpRemain);
				result = stringField::stringCompare(srcPos, cl, dest, cl, casesensitive);
				if (result != 0)
					dsOk();
				if (srcRemain < sizeof(tmpBuf) - tmpRemain)
				{
					result = -1;
					dsOk();
				}
				else if (srcRemain == sizeof(tmpBuf) - tmpRemain)
				{
					if (destRemain == 0)
						result = 0;
					else
						result = -1;
					dsOk();
				}
				else
				{
					srcPos += sizeof(tmpBuf) - tmpRemain;
					srcRemain -= sizeof(tmpBuf) - tmpRemain;
				}
			} while (*destPos != '\0');
			if (srcRemain > 0)
				result = 1;
			else if (srcRemain == 0)
				result = 0;
			else
				result = -1;
			dsOk();
		}
	}

	DS charsetConvert::compare(CHARSET srcType, const char* src, size_t srcLength, CHARSET destType, const char* dest, size_t destLength, bool casesensitive, int& result) const
	{
		if (srcType == destType)
		{
			result = stringField::stringCompare(src, srcLength, dest, destLength, casesensitive);
			dsOk();
		}
		else
		{
			if (srcType == CHARSET::utf8)
			{
				dsReturn(compareByUtf8(src, srcLength, destType, dest, destLength, casesensitive, result));
			}
			else if (destType == CHARSET::utf8)
			{
				dsReturnIfFailed(compareByUtf8(dest, destLength, srcType, src, srcLength, casesensitive, result));
				result = -result;
				dsOk();
			}
			else
			{
				iconv_t scv = m_converts[srcType][CHARSET::utf8];
				if (scv != nullptr)
					dsFailed(-1, "can not convert " << charsets[srcType].name << " to utf8");
				iconv_t dcv = m_converts[destType][CHARSET::utf8];
				if (dcv != nullptr)
					dsFailed(-1, "can not convert " << charsets[destType].name << " to utf8");
				char srcTmp[1024];
				char destTmp[1024];
#if defined OS_LINUX
				char* srcPos = (char*)src;
				char* destPos = (char*)dest;
#else 
				const char* srcPos = src;
				const char* destPos = dest;
#endif 
				size_t srcRemain = srcLength;
				size_t destRemain = destLength;
				size_t prevRemain = 0;

				int srcCharCount;
				int destCharCount;
				do
				{
					char* srcTmpPos = srcTmp;
					char* destTmpPos = destTmp;
					size_t srcTmpRemain = sizeof(srcTmp);
					size_t destTmpRemain = sizeof(destTmp);
					if (prevRemain > 0)
					{
						srcTmpRemain -= prevRemain;
						srcTmpPos += prevRemain;
					}
					else if (prevRemain <= 0)
					{
						destTmpRemain += prevRemain;
						destTmpPos -= prevRemain;
					}

					if (srcRemain > 0)
					{
						srcCharCount = iconv(scv, &srcPos, &srcRemain, &srcTmpPos, &srcTmpRemain);
						if (srcCharCount < 0)
							dsFailed(-1, "can not convert " << charsets[srcType].name << " to utf8");
					}

					if (destRemain > 0)
					{
						destCharCount = iconv(dcv, &destPos, &destRemain, &destTmpPos, &destTmpRemain);
						if (destCharCount < 0)
							dsFailed(-1, "can not convert " << charsets[destType].name << " to utf8");
					}

					int cl = std::min<size_t>(sizeof(srcTmp) - srcTmpRemain, sizeof(destTmp) - destTmpRemain);
					result = stringField::stringCompare(srcTmp, cl, destTmp, cl, casesensitive);
					if (result != 0)
						dsOk();
					prevRemain = (sizeof(srcTmp) - srcTmpRemain) - (sizeof(destTmp) - destTmpRemain);
					if (srcRemain == 0)
					{
						if (prevRemain == 0 && destRemain == 0)
							dsOk();
						else if (prevRemain < 0 || destRemain >0)
						{
							result = -1;
							dsOk();
						}
					}
					if (destRemain == 0)
					{
						if (prevRemain > 0 || srcRemain > 0)
						{
							result = 1;
							dsOk();
						}
					}
				} while (*srcPos != '\0' || *destPos != '\0');
				dsOk();
			}
		}
	}
}
