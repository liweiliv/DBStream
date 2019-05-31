#pragma once
/*
 * charset.h
 *
 *  Created on: 2018年12月5日
 *      Author: liwei
 */
#include <stdint.h>
#include "../util/winDll.h"
struct charsetInfo {
	const char * name;
	uint8_t byteSizePerChar;
	uint32_t id;
};
#define NO_CHARSET_ID 0xffffu
enum CHARSET {
	big5,
	dec8,
	cp850,
	hp8,
	koi8r,
	latin1,
	latin2,
	swe7,
	ascii,
	ujis,
	sjis,
	hebrew,
	tis620,
	euckr,
	koi8u,
	gb2312,
	greek,
	cp1250,
	gbk,
	latin5,
	armscii8,
	utf8,
	ucs2,
	cp866,
	keybcs2,
	macce,
	macroman,
	cp852,
	latin7,
	utf8mb4,
	cp1251,
	utf16,
	utf16le,
	cp1256,
	cp1257,
	utf32,
	binary,
	geostd8,
	cp932,
	eucjpms,
	gb18030,
	MAX_CHARSET
};

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
const charsetInfo* DLL_EXPORT getCharset(const char * name);
