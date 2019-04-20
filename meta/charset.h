#pragma once
/*
 * charset.h
 *
 *  Created on: 2018年12月5日
 *      Author: liwei
 */
#include <stdint.h>
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
extern charsetInfo charsets[MAX_CHARSET];
const charsetInfo* getCharset(const char * name);
