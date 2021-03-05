#pragma once
namespace SQL_PARSER {
	static inline bool isSeparator(const char * pos)
	{
		char c = *pos;
		if (c == ' ' || c == '\n' || c == '\t')
			return true;
		if (c <= 0x29)
		{
			if (c == 0x20 || c == 0x9 || c == 0xA || c == 0xB || c == 0xC || c == 0xD || c == 0xA0)
				return true;
		}
		return false;
	}
}