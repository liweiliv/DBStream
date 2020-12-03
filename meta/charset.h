#pragma once
/*
 * charset.h
 *
 *  Created on: 2018年12月5日
 *      Author: liwei
 */
#include <stdint.h>
#include "util/winDll.h"
struct charsetInfo {
	const char * name;
	uint8_t nameSize;
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
	{ "big5", 4,2 ,big5} ,
	{"dec8", 4,1,dec8} ,
	{"cp850", 5,1,cp850} ,
	{"hp8", 3,1,hp8},
	{"koi8r", 5,1,koi8r},
	{"latin1", 6,1,latin1},
	{"latin2", 6,1,latin2},
	{"swe7", 4,1,swe7},
	{"ascii", 5,1,ascii},
	{"ujis", 4,3,ujis},
	{"sjis", 4,2,sjis},
	{"hebrew", 6,1,hebrew},
	{"tis620", 6,1,tis620},
	{"euckr", 5,2,euckr},
	{"koi8u", 5,1,koi8u},
	{"gb2312", 6,2,gb2312},
	{"greek", 5,1,greek},
	{"cp1250", 6,1,cp1250},
	{"gbk", 3,2,gbk},
	{"latin5",6, 1,latin5},
	{"armscii8",8, 1,armscii8},
	{"utf8", 4,3,utf8},
	{"ucs2", 4,2,ucs2},
	{"cp866", 5,1,cp866},
	{"keybcs2", 7,1,keybcs2},
	{"macce", 5,1,macce},
	{"macroman",8, 1,macroman},
	{"cp852", 5,1,cp852},
	{"latin7", 6,1,latin7},
	{"utf8mb4", 7,4,utf8mb4},
	{"cp1251", 6,1,cp1251},
	{"utf16", 5,4,utf16},
	{"utf16le", 7,4,utf16le},
	{"cp1256", 6,1,cp1256},
	{"cp1257", 6,1,cp1257},
	{"utf32", 5,4,utf32},
	{"binary", 6,1,binary},
	{"geostd8", 7,1,geostd8},
	{"cp932", 5,2,cp932},
	{"eucjpms",7, 3,eucjpms},
	{"gb18030", 7,4,gb18030}
};
DLL_EXPORT const charsetInfo* getCharset(const char * name, uint32_t size);
DLL_EXPORT const charsetInfo* getCharset(const char* name);

