#pragma once
#include <stdint.h>
#include <string.h>
class hex
{
public:
	constexpr static char HEX_CHAR_MAP[] = { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F' };
	inline static bool hexCharToInt(char c, uint8_t& to)
	{
		if (c >= '0' && c <= '9')
			to = (uint8_t)(c - '0');
		else if (c >= 'A' && c <= 'F')
			to = 10 + (uint8_t)(c - 'A');
		else if (c >= 'a' && c <= 'f')
			to = 10 + (uint8_t)(c - 'a');
		else
			return false;
		return true;
	}

	inline static bool hex2Char(const char* buf, uint8_t& to)
	{
		uint8_t c = 0;
		if (!hexCharToInt(buf[0], c))
			return false;
		to = c << 4;
		if (!hexCharToInt(buf[1], c))
			return false;
		to |= c;
		return true;
	}

	template<class T>
	inline static bool hex2Int(const char* buf, uint8_t size, T& v)
	{
		v = 0;
		uint8_t end = size / 2;
		for (uint8_t i = 0; i < end; i += 2)
		{
			uint8_t c;
			if (!hex2Char(buf + i, c))
				return false;
			v = (v << 8) | ((T)c);
		}
		return true;
	}

	template<class T>
	inline static void int2Hex(T i, uint8_t size, char* buf)
	{
		buf[size * 2] = '\0';
		for (char j = size - 1; j >= 0; j--)
		{
			unsigned char c = ((unsigned char*)&i)[j];
			buf[j * 2] = HEX_CHAR_MAP[c >> 4];
			buf[j * 2 + 1] = HEX_CHAR_MAP[c & 0xf];
		}
	}

	template<class T>
	inline static void int2Hex(T i, char* buf)
	{
		return int2Hex(i, sizeof(T), buf);
	}

	inline static bool hex2Bytes(const char* buf, uint32_t size, char* dest)
	{
		uint32_t end = size / 2;
		for (uint32_t i = 0; i < end; i += 2)
		{
			if (!hex2Char(buf + i, ((uint8_t*)dest)[i]))
				return false;
		}
		dest[end] = '\0';
		return true;
	}

	inline static void bytes2Hex(const char* bytes, uint32_t size, char* dest)
	{
		for (uint32_t i = 0; i < size; i++)
			int2Hex(bytes[i], dest + i * 2);
		dest[size * 2 + 1] = '\0';
	}

	inline static void bytes2Hex(const char* bytes, char* dest)
	{
		uint32_t size = strlen(bytes);
		for (uint32_t i = 0; i < size; i++)
			int2Hex(bytes[i], dest + i * 2);
		dest[size * 2 + 1] = '\0';
	}
};