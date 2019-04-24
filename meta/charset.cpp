#include "charset.h"
#include <string.h>
#ifdef OS_WIN
#include "../util/winString.h"
#endif // OS_WIN

struct charsetStringInfo {
	const char * nameString;
	CHARSET charset;
};
constexpr static charsetStringInfo sortedCharsetInfo[MAX_CHARSET] = {
{"armscii8",armscii8},
{"ascii",ascii},
{"big5",big5},
{"binary",binary},
{"cp1250",cp1250},
{"cp1251",cp1251},
{"cp1256",cp1256},
{"cp1257",cp1257},
{"cp850",cp850},
{"cp852",cp852},
{"cp866",cp866},
{"cp932",cp932},
{"dec8",dec8},
{"eucjpms",eucjpms},
{"euckr",euckr},
{"gb18030",gb18030},
{"gb2312",gb2312},
{"gbk",gbk},
{"geostd8",geostd8},
{"greek",greek},
{"hebrew",hebrew},
{"hp8",hp8},
{"keybcs2",keybcs2},
{"koi8r",koi8r},
{"koi8u",koi8u},
{"latin1",latin1},
{"latin2",latin2},
{"latin5",latin5},
{"latin7",latin7},
{"macce",macce},
{"macroman",macroman},
{"sjis",sjis},
{"swe7",swe7},
{"tis620",tis620},
{"ucs2",ucs2},
{"ujis",ujis},
{"utf16",utf16},
{"utf16le",utf16le},
{"utf32",utf32},
{"utf8",utf8},
{"utf8mb4",utf8mb4}
};
const charsetInfo* getCharset(const char * name)
{
	int16_t s = 0, e = MAX_CHARSET - 1,m;
	while (s <= e && s < MAX_CHARSET)
	{
		m = (s + e) / 2;
		int8_t c = strcasecmp(sortedCharsetInfo[m].nameString, name);
		if (c == 0)
			return &charsets[sortedCharsetInfo[m].charset];
		else if (c > 0)
			e = m - 1;
		else
			s = m + 1;
	}
	return nullptr;
}
