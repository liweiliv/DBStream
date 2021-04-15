#pragma once
#include "util.h"
#include "util/status.h"
#include "crypt/sha-2/sha-256.h"
namespace AUTH
{
	class client
	{
	private:
		char m_salt[PASSWORD_HASH_SIZE];
		char m_key[PASSWORD_HASH_SIZE];
	public:
		client()
		{
			memset(m_salt, 0, sizeof(m_key));
			memset(m_key, 0, sizeof(m_key));
		}
		~client() {}
		const char* getKey()
		{
			return m_key;
		}
		DS setSalt(const char * salt,uint32_t size)
		{
			if (salt == nullptr || size != PASSWORD_HASH_SIZE)
				dsFailedAndLogIt(errorCode::INVALID_SALT, "invalid salt", WARNING);
			memcpy(m_salt, salt, size);
			dsOk();
		}
		DS generateResp(const char* password, uint32_t passwordSize,uint8_t resp[PASSWORD_HASH_SIZE])
		{
			dsReturnIfFailed(checkPassword(password, passwordSize));
			uint8_t hash1Pwd[PASSWORD_HASH_SIZE] = { 0 }, hash2Pwd[PASSWORD_HASH_SIZE] = { 0 };
			calc_sha_256(hash1Pwd, password, passwordSize);
			calc_sha_256(hash2Pwd, hash1Pwd, sizeof(hash1Pwd));
			xorStr((const char*)hash2Pwd, m_salt, m_key, PASSWORD_HASH_SIZE);
			xorStr((const char*)hash1Pwd, m_key, (char*)resp, PASSWORD_HASH_SIZE);
			dsOk();
		}
	};
}