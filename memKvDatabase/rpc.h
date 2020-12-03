#pragma once
#include "auth/util.h"
#include "row.h"
namespace KVDB {
#pragma pack(1)
	enum rpcType {
		AUTH_REQ,
		AUTH_SALT,
		AUTH_DATA,
		AUTH_RESP,
		CLOSE_REQ,
		CLOSE_RESP,
		SQL,
		ROW_CHANGE_DATA,
		PREPARE_STETEMENT_SQL,
		PREPARE_STETEMENT_ROW_DATA,
		QUERY_RESULT,
		EXCUTE_RESULT,
		GET_META_REQ,
		GET_META_RESP
	};
	struct rpcHead
	{
		uint32_t size;
		uint8_t flag;
		uint8_t type;
		uint32_t rpcId;
		rpcHead() :size(0),flag(0),type(0), rpcId(0){}
		rpcHead(uint32_t size, uint8_t flag, uint8_t type):size(size),flag(flag),type(type), rpcId(0){}
	};
	struct rpcClient :public rpcHead {
		uint32_t token;
		rpcClient(uint32_t size, uint8_t flag, uint8_t type, uint32_t token) :rpcHead(size, flag, type), token(token) {}
		rpcClient() :rpcHead(), token(0) {}
	};
	struct authReq :public rpcHead {
		char user[AUTH::MAX_USER_SIZE];
		authReq(const char* userName, uint32_t userNameSize) :rpcHead(sizeof(rpcHead) + (AUTH::MAX_USER_SIZE >= userNameSize ? userNameSize : AUTH::MAX_USER_SIZE),0, AUTH_REQ)
		{
			memcpy(user, userName, AUTH::MAX_USER_SIZE >= userNameSize ? userNameSize : AUTH::MAX_USER_SIZE);
		}
		uint16_t getUserSize() const
		{
			return size - offsetof(authReq, user);
		}
	};
	struct authSendSalt :public rpcHead {
		char salt[AUTH::PASSWORD_HASH_SIZE];
		authSendSalt(const char * salt) :rpcHead(sizeof(authSendSalt),0,AUTH_SALT)
		{
			memcpy(this->salt, salt, AUTH::PASSWORD_HASH_SIZE);
		}
	};
	struct authData :public rpcHead {
		char key[AUTH::PASSWORD_HASH_SIZE];
		authData(const char* key) :rpcHead(sizeof(authData), 0, AUTH_DATA )
		{
			memcpy(this->key, key, AUTH::PASSWORD_HASH_SIZE);
		}
		uint16_t getKeySize() const
		{
			return size - offsetof(authData, key);
		}
	};
	struct authResp :public rpcClient {
		uint8_t success;
		authResp(bool success, uint32_t token) :rpcClient(sizeof(authResp),0,AUTH_RESP, token), success(success)
		{
		}
	};
	struct closeReq :public rpcClient {
		closeReq(uint32_t token) :rpcClient(sizeof(closeReq),0,CLOSE_REQ,token)
		{
		}
	};
	struct closeResp :public rpcClient {
		uint32_t code;
		char message[256];
		closeResp(uint32_t token, uint32_t code,const char * msg,uint32_t msgSize) :rpcClient(offsetof(closeResp,message) + sizeof(message) > msgSize? msgSize: sizeof(message),0,CLOSE_RESP,token)
		{
			memcpy(message, msg, sizeof(message) > msgSize ? msgSize : sizeof(message));
		}
	};

	struct sqlMessage :public rpcClient {
		char sql[1];
		inline void init(uint32_t token,const char* sqlStr, uint32_t sqlSize)
		{
			size = offsetof(sqlMessage,sql) + sqlSize;
			flag = 0;
			type = SQL;
			this->token = token;
			memcpy(sql, sqlStr, sqlSize);
		}
	};
	struct rowChangeData :public rpcClient {
		uint32_t rowCount;
		uint32_t columnCount;
		uint32_t dbNameOffset;
		uint32_t tableNameOffset;
		uint32_t columnNamesOffset;
		uint32_t dataOffset;
		char data[1];
		inline const char* getDbName()
		{
			return ((const char*)(&size)) + dbNameOffset;
		}
		inline const char* getTableName()
		{
			return ((const char*)(&size)) + tableNameOffset;
		}
		inline const char* getColumn(uint32_t columnId)
		{
			if (columnId >= columnCount)
				return nullptr;
			const uint32_t* columnNameOffset = (const uint32_t*)(((const char*)(&size)) + columnNamesOffset);
			return ((const char*)(&size)) + columnNameOffset[columnId];
		}
	};
	struct prepareStatment :public rpcClient {
		char sql[1];
		inline void init(uint32_t token, const char* sqlStr, uint32_t sqlSize)
		{
			size = offsetof(sqlMessage, sql) + sqlSize;
			flag = 0;
			type = PREPARE_STETEMENT_SQL;
			this->token = token;
			memcpy(sql, sqlStr, sqlSize);
		}
	};

	struct prepareStatmentData :public rpcClient {
		uint32_t count;
		char data[1];
	};

	struct queryResult :public rpcHead {
		uint64_t tableId;
		uint32_t count;
		char data[1];
	};

	struct excuteResult :public rpcHead {
		uint8_t success;
		char message[256];
	};

	struct getMetaReq :public rpcClient {
		uint64_t tableId;
	};
	struct getMetaResp :public rpcHead {
		uint8_t success;
		char data[1];
	};

#pragma pack()
};