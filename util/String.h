/*
 * String.h
 *
 *  Created on: 2019年7月24日
 *      Author: liwei
 */
#pragma once
#include <string>
#include "itoaSse.h"
#include "dtoa.h"
#include "hex.h"
class String :public std::string {
public:
	String() :std::string() {}
	String(const char* data) :std::string(data) {}
	String(const char* data, size_t size) :std::string(data, size) {}
	String(const std::string& s) :std::string(s) {}
	String(const String& s) :std::string(s) {}
	String& operator=(const String& s)
	{
		assign(s);
		return *this;
	}
	String operator<<(const std::string& s)
	{
		return (std::string)(*this) + s;
	}
	String& append(const std::string& s)
	{
		std::string::append(s);
		return *this;
	}
	String& append(String& s)
	{
		std::string::append(s);
		return *this;
	}
	String operator<<(const char* s)
	{
		return (std::string)(*this) + s;
	}
	String& append(const char* s)
	{
		std::string::append(s);
		return *this;
	}
	String& append(const char* s, size_t size)
	{
		std::string::append(s, size);
		return *this;
	}
	String operator<<(uint64_t i)
	{
		char buf[32];
		u64toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(uint64_t i)
	{
		char buf[32];
		u64toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(int64_t i)
	{
		char buf[32];
		i64toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(int64_t i)
	{
		char buf[32];
		i64toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(uint32_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(uint32_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(int32_t i)
	{
		char buf[32];
		i32toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(int32_t i)
	{
		char buf[32];
		i32toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(uint16_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(uint16_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(int16_t i)
	{
		char buf[32];
		i32toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(int16_t i)
	{
		char buf[32];
		i32toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(uint8_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		return (std::string)(*this) + buf;
	}
	String& append(uint8_t i)
	{
		char buf[32];
		u32toa_sse2(i, buf);
		std::string::append(buf);
		return *this;
	}
	String operator<<(int8_t i)
	{
		char buf[2];
		buf[0] = i;
		buf[1] = '\0';
		return (std::string)(*this) + buf;
	}
	String& append(int8_t i)
	{
		char buf[2];
		buf[0] = i;
		buf[1] = '\0';
		std::string::append(buf);
		return *this;
	}
	String operator<<(float i)
	{
		char buf[33];
		my_fcvt_compact(i, buf, nullptr);
		return (std::string)(*this) + buf;
	}
	String& append(float i)
	{
		char buf[33];
		my_fcvt_compact(i, buf, nullptr);
		std::string::append(buf);
		return *this;
	}
	String operator<<(double i)
	{
		char buf[48];
		my_fcvt_compact(i, buf, nullptr);
		return (std::string)(*this) + buf;
	}
	String& append(double i)
	{
		char buf[48];
		my_fcvt_compact(i, buf, nullptr);
		std::string::append(buf);
		return *this;
	}

	template<class T>
	String& appendHex(T i, uint8_t size)
	{
		char buf[size * 2 + 1];
		hex::int2Hex(i, size, buf);
		return append(buf);
	}

	template<class T>
	String& appendHex(T i)
	{
		char buf[sizeof(T) * 2 + 1];
		hex::int2Hex(i, buf);
		return append(buf);
	}

	String& appendHexString(const char* str, uint32_t length)
	{
		char* buf = new char[length * 2 + 1];
		hex::bytes2Hex(str, length, buf);
		append(buf);
		delete[]buf;
		return *this;
	}

	String& appendHexString(const char* str)
	{
		return appendHexString(str, strlen(str));
	}

	bool endWith(const char* str)
	{
		size_t strSize = strlen(str);
		if (strSize > size())
			return false;
		return memcmp(c_str() + size() - strSize, str, strSize) == 0;
	}

	bool endWith(const std::string& str)
	{
		size_t strSize = str.size();
		if (strSize > size())
			return false;
		return memcmp(c_str() + size() - strSize, str.c_str(), strSize) == 0;
	}

	bool startWith(const char* str)
	{
		size_t strSize = strlen(str);
		if (strSize > size())
			return false;
		return memcmp(c_str(), str, strSize) == 0;
	}

	bool startWith(const std::string& str)
	{
		size_t strSize = str.size();
		if (strSize > size())
			return false;
		return memcmp(c_str(), str.c_str(), strSize) == 0;
	}

	std::vector<String> split(const char* s, size_t strSize)
	{
		std::vector<String> v;
		for (const char* start = c_str(), *pos = strstr(start, s);;)
		{
			if (pos == nullptr)
			{
				v.push_back(String(start));
				break;
			}
			else
			{
				v.push_back(String(start, pos - start));
				start = pos + strSize;
				pos = strstr(start, s);
			}
		}
		return v;
	}

	std::vector<String> split(const char* s)
	{
		return split(s, strlen(s));
	}

	std::vector<String> split(const std::string& s)
	{
		return split(s.c_str(), s.size());
	}

	template<typename T>
	bool getInt(T& v)
	{
		bool sign = true;
		v = 0;
		const char* s = c_str();
		if (*s == '-')
		{
			s++;
			sign = false;
		}
		char c;
		while ((c = *s++) != '\0')
		{
			if (c <= '9' && *c = '0')
				v = v * 10 + c - '0';
			else
			{
				v = 0;
				return false;
			}
		}
		if (!sign)
			v = -v;
		return true;
	}
};