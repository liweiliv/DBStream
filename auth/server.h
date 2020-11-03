#pragma once
#include <string>
#include <mutex>
#include "tbb/concurrent_hash_map.h"
#include "glog/logging.h"
#include "util/file.h"
#include "util/status.h"
#include "hash.h"
#include "util/random.h"
#include "crypt/sha-2/sha-256.h"
#include "errorCode.h"
#include "util.h"
namespace AUTH
{
	typedef tbb::concurrent_hash_map<std::string, std::string, hashWrap> userMap;
	struct serverHandle
	{
		std::string userName;
		char salt[PASSWORD_HASH_SIZE];
		char key[PASSWORD_HASH_SIZE];
		serverHandle(const char* user, uint16_t size) :userName(user, size)
		{
			memset(salt, 0, sizeof(salt));
			memset(key, 0, sizeof(key));
		}
	};
	class server
	{
	public:

	private:
		userMap m_userMap;
		uint32_t m_maxUserLength;
		uint32_t m_maxPwdHashLength;
		leveldb::Random m_random;
	private:
		dsStatus& userExist(const char* user, uint16_t size)
		{
			dsReturnIfFailed(checkUser(user, size));
			userMap::const_accessor accessor;
			std::string u(user, size);
			if (m_userMap.find(accessor, u))
				dsOk();
			else
				dsFailed(errorCode::USER_NOT_EXIST_OR_PASSWORD_NOT_MATCH, "");
		}
		dsStatus& userNotExist(const char* user, uint16_t size)
		{
			dsReturnIfFailed(checkUser(user, size));
			userMap::const_accessor accessor;
			std::string u(user, size);
			if (!m_userMap.find(accessor, u))
				dsOk();
			else
				dsFailed(errorCode::USER_EXIST, "");
		}
	public:
		dsStatus& load(const char* path)
		{
			dsOk();
		}
		dsStatus& registerUser(const char* user, uint16_t userSize, const char* password, uint16_t passwordSize)
		{
			dsReturnIfFailed(checkUser(user, userSize));
			dsReturnIfFailed(checkPassword(password, passwordSize));

			uint8_t hash1Pwd[PASSWORD_HASH_SIZE] = { 0 }, hash2Pwd[PASSWORD_HASH_SIZE] = { 0 };
			calc_sha_256(hash1Pwd, password, passwordSize);
			calc_sha_256(hash2Pwd, hash1Pwd, sizeof(hash1Pwd));

			dsReturnIfFailed(userNotExist(user, userSize));
			if (!m_userMap.insert(std::pair<std::string, std::string>(std::string(user, userSize), std::string((char*)hash2Pwd, sizeof(hash2Pwd)))))
				dsFailed(errorCode::USER_EXIST, "user exist");
			dsOk();
		}
		dsStatus& authReq(serverHandle& handle)
		{
			dsReturnIfFailed(checkUser(handle.userName.c_str(), handle.userName.size()));
			for (int i = 0; i < sizeof(handle.salt); i += sizeof(uint32_t))
				*(uint32_t*)&handle.salt[i] = m_random.Next();
			dsOk();
		}
		dsStatus& authLogin(serverHandle& handle, const char* resp, uint32_t respSize)
		{
			dsReturnIfFailed(checkUser(handle.userName.c_str(), handle.userName.size()));
			dsReturnIfFailed(checkPasswordHash(resp, respSize));
			userMap::const_accessor accessor;
			if (!m_userMap.find(accessor, handle.userName))
				dsFailed(errorCode::USER_NOT_EXIST_OR_PASSWORD_NOT_MATCH, "");
			std::string pwdHash2 = accessor->second;
			//generate key
			xorStr(pwdHash2.c_str(), handle.salt, handle.key, PASSWORD_HASH_SIZE);
			char pwdHash1FromClient[PASSWORD_HASH_SIZE] = { 0 };
			uint8_t pwdHash2FromClient[PASSWORD_HASH_SIZE] = { 0 };
			xorStr(resp,  handle.key, pwdHash1FromClient, PASSWORD_HASH_SIZE);
			calc_sha_256(pwdHash2FromClient, pwdHash1FromClient, PASSWORD_HASH_SIZE);
			if (memcmp(pwdHash2FromClient, pwdHash2.c_str(), PASSWORD_HASH_SIZE) != 0)
				dsFailed(errorCode::USER_NOT_EXIST_OR_PASSWORD_NOT_MATCH, "");
			dsOk();
		}
	};
}