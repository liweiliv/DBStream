#pragma once
#include <stdint.h>
#include "util/status.h"
#include "client.h"
#include "errorCode.h"
namespace AUTH {
	constexpr static uint32_t PASSWORD_HASH_SIZE = 32;//sha-256
	constexpr static uint32_t MAX_USER_SIZE = 128;

	static 	inline void xorStr(const char* s, const char* d, char* r, uint32_t size)
	{
		uint32_t iSize = size & (~0x3);
		uint32_t i = 0;
		for (; i < iSize; i += 4)
			*(uint32_t*)&r[i] = (*(const uint32_t*)&s[i]) ^ (*(const uint32_t*)&d[i]);
		for (; i < size; i++)
			r[i] = s[i] ^ d[i];
	}
	static inline dsStatus& checkUser(const char* user, uint16_t size)
	{
		if (user == nullptr || size > MAX_USER_SIZE)
			dsFailedAndLogIt(errorCode::INVALID_USER, "invalid user", WARNING);
		dsOk();
	}
	static inline dsStatus& checkPasswordHash(const char* pwdHash, uint16_t size)
	{
		if (pwdHash == nullptr || size != PASSWORD_HASH_SIZE)
			dsFailedAndLogIt(errorCode::INVALID_PASSWORD, "invalid password", WARNING);
		dsOk();
	}
	static inline dsStatus& checkPassword(const char* pwd, uint16_t size)
	{
		dsOk();
	}
}