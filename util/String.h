/*
 * String.h
 *
 *  Created on: 2019年7月24日
 *      Author: liwei
 */

#ifndef UTIL_STRING_H_
#define UTIL_STRING_H_
#include <string>
#include "itoaSse.h"
#include "dtoa.h"

class String :public std::string {
public:
	String() :std::string() {}
	String(const char* data) :std::string(data) {}
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
	String &append(double i)
	{
		char buf[48];
		my_fcvt_compact(i, buf, nullptr);
		std::string::append(buf);
		return *this;
	}
};
#endif /* UTIL_STRING_H_ */
