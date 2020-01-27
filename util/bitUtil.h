#pragma once
#include <stdint.h>
static inline uint8_t highPosOfUchar(uint8_t c)
{
	if ((c & 0xf0) != 0)
	{
		if ((c & 0x80) == 0x80)
			return 8;
		if ((c & 0x40) == 0x40)
			return 7;
		if ((c & 0x20) == 0x20)
			return 6;
		if ((c & 0x10) == 0x10)
			return 5;
	}
	else if (c != 0)
	{
		if ((c & 0x8) == 0x8)
			return 4;
		if ((c & 0x4) == 0x4)
			return 3;
		if ((c & 0x2) == 0x2)
			return 2;
		if ((c & 0x1) == 0x1)
			return 1;
	}
	else
		return 0;
}
static inline uint8_t highPosOfUShort(uint16_t s)
{
	if (((uint8_t*)&s)[1] != 0)
	{
		return 8 + highPosOfUchar(((uint8_t*)&s)[1]);
	}
	else
	{
		return highPosOfUchar(((uint8_t*)&s)[0]);
	}
}
static inline uint8_t highPosOfUint(uint32_t i)
{
	if (((uint16_t*)&i)[1] != 0)
	{
		return 16 + highPosOfUShort(((uint16_t*)&i)[1]);
	}
	else
	{
		return highPosOfUShort(((uint16_t*)&i)[0]);
	}
}
static inline uint8_t highPosOfULong(uint64_t l)
{
	if (((uint32_t*)&l)[1] != 0)
	{
		return 32 + highPosOfUint(((uint32_t*)&l)[1]);
	}
	else
	{
		return highPosOfUint(((uint32_t*)&l)[0]);
	}
}