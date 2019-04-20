#include "charset.h"
#include <string.h>
#define OS_WIN
#ifdef OS_WIN
#include "../util/winString.h"
#endif // OS_WIN

constexpr charsetInfo charsets[] = {
	{ "big5", 2 ,big5} ,
	{"dec8", 1,dec8} ,
	{"cp850", 1,cp850} ,
	{"hp8", 1,hp8},
	{"koi8r", 1,koi8r},
	{"latin1", 1,latin1},
	{"latin2", 1,latin2},
	{"swe7", 1,swe7},
	{"ascii", 1,ascii},
	{"ujis", 3,ujis},
	{"sjis", 2,sjis},
	{"hebrew", 1,hebrew},
	{"tis620", 1,tis620},
	{"euckr", 2,euckr},
	{"koi8u", 1,koi8u},
	{"gb2312", 2,gb2312},
	{"greek", 1,greek},
	{"cp1250", 1,cp1250},
	{"gbk", 2,gbk},
	{"latin5", 1,latin5},
	{"armscii8", 1,armscii8},
	{"utf8", 3,utf8},
	{"ucs2", 2,ucs2},
	{"cp866", 1,cp866},
	{"keybcs2", 1,keybcs2},
	{"macce", 1,macce},
	{"macroman", 1,macroman},
	{"cp852", 1,cp852},
	{"latin7", 1,latin7},
	{"utf8mb4", 4,utf8mb4},
	{"cp1251", 1,cp1251},
	{"utf16", 4,utf16},
	{"utf16le", 4,utf16le},
	{"cp1256", 1,cp1256},
	{"cp1257", 1,cp1257},
	{"utf32", 4,utf32},
	{"binary", 1,binary},
	{"geostd8", 1,geostd8},
	{"cp932", 2,cp932},
	{"eucjpms", 3,eucjpms},
	{"gb18030", 4,gb18030}
};
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
