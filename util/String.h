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
namespace std{
	class String :public string{
	public:
		String():string(){}
		String(const char* data):string(data){}
		String(const string &s):string(s){}
		String(const String &s):string(s){}
		String& operator=(const String &s)
		{
			assign(s);
			return *this;
		}
		String operator<<(const string s)
		{
			return (string)(*this)+s;
		}
		String operator<<(const char *s)
		{
			return (string)(*this)+s;
		}
		String operator<<(uint64_t i)
		{
			char buf[32];
			u64toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(int64_t i)
		{
			char buf[32];
			i64toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(uint32_t i)
		{
			char buf[32];
			u32toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(int32_t i)
		{
			char buf[32];
			i32toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(uint16_t i)
		{
			char buf[32];
			u32toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(int16_t i)
		{
			char buf[32];
			i32toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(uint8_t i)
		{
			char buf[32];
			u32toa_sse2(i,buf);
			return (string)(*this)+buf;
		}
		String operator<<(int8_t i)
		{
			char buf[2];
			buf[0] = i;
			buf[1] = '\0';
			return (string)(*this)+buf;
		}
		String operator<<(float i)
		{
			char buf[33];
			my_fcvt_compact(i,buf,nullptr);
			return (string)(*this)+buf;
		}
		String operator<<(double i)
		{
			char buf[48];
			my_fcvt_compact(i,buf,nullptr);
			return (string)(*this)+buf;
		}
	};

}




#endif /* UTIL_STRING_H_ */
