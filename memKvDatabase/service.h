#pragma once
#include <stdint.h>
#include "tbb/concurrent_hash_map.h"
#include "clientHandle.h"
#include "errorCode.h"
#include "auth/server.h"
#include "util/status.h"
#include "util/arrayQueue.h"
#include "rpc.h"
#include "sqlParser/sqlParser.h"
namespace KVDB {
	class service {
	private:
		class hashWrap {
		public:
			inline bool equal(const int32_t s, const int32_t d) const
			{
				return s == d;
			}
			inline size_t hash(const int32_t& s) const
			{
				std::hash<int32_t> h;
				return h(s);
			}
		};
		tbb::concurrent_hash_map<int32_t, clientHandle*, hashWrap > m_clients;
		arrayQueue<clientHandle*> m_taskQueue;
		AUTH::server * m_auth;

	private:
		DS closeConnect(clientHandle* handle, errorCode code,const char * message)
		{
			LOG(WARNING) << "close connect:" << handle->m_uid << " @ " << handle->m_ip << ":" << handle->m_port <<
				",user:" << (handle->getAuthHandle() == nullptr ? "" : handle->getAuthHandle()->userName) <<
				",code:" << (int)code << (message == nullptr ? "" : message);
			//todo
			m_clients.erase(handle->m_uid);
			delete handle;
			dsOk();
		}
		DS auth(clientHandle* handle, const rpcHead* rpc)
		{
			switch (rpc->type)
			{
			case AUTH_REQ:
			{
				if (handle->getAuthHandle() != nullptr)
					dsReturn(closeConnect(handle, AUTH_FAILED, "wrong auth status"));
				const authReq* req = static_cast<const authReq*>(rpc);
				AUTH::serverHandle* authInfo = new AUTH::serverHandle(req->user, req->getUserSize());
				if (!dsCheck(m_auth->authReq(*authInfo)))
					dsReturn(closeConnect(handle, AUTH_FAILED, getLocalStatus().errMessage.c_str()));
				handle->setAuthHandle(authInfo);
				authSendSalt* salt = new authSendSalt(authInfo->salt);
				//todo
				handle->m_preProcessResp.push(salt);
				m_taskQueue.pushWithCond(handle);
				dsOk();
			}
			case AUTH_DATA:
			{
				if (handle->getAuthHandle() == nullptr)
					dsReturn(closeConnect(handle, AUTH_FAILED, "wrong auth status"));
				const authData* data = static_cast<const authData*>(rpc);
				if (!dsCheck(m_auth->authLogin(*handle->getAuthHandle(), data->key, data->getKeySize())))
					dsReturn(closeConnect(handle, AUTH_FAILED, getLocalStatus().errMessage.c_str()));
				handle->m_token = rand();
				authResp* resp = new authResp(true, handle->m_token);
				//todo
				handle->m_preProcessResp.push(resp);
				m_taskQueue.pushWithCond(handle);
				dsOk();
			}
			default:
				dsFailedAndLogIt(INNER_ERROR, "illegal rpc type:" + rpc->type, ERROR);
			}
		}
		DS processSql(clientHandle* handle, const rpcHead* rpc)
		{
			dsOk();
		}
		DS processPrepareStatmentSql(clientHandle* handle, const prepareStatment* rpc)
		{
			dsOk();
		}
		DS processPrepareStatmentData(clientHandle* handle, const prepareStatmentData* rpc)
		{
			dsOk();
		}
		DS processGetMetaReq(clientHandle* handle, const getMetaReq* rpc)
		{
			dsOk();
		}
	public:
		DS excute(uint32_t id, const char* cmd)
		{
			clientHandle* handle;
			tbb::concurrent_hash_map<int32_t, clientHandle*, hashWrap >::const_accessor accessor;
			if (!m_clients.find(accessor, id))
				dsFailedAndLogIt(errorCode::DO_NOT_CONNECT_TO_DATABASE, "do not connect to database", WARNING);
			handle = accessor->second;

			if (unlikely(((const rpcHead*)cmd)->type <= AUTH_RESP))
				dsReturn(auth(handle, (const rpcHead*)cmd));

			const rpcClient* rpc = (const rpcClient*)cmd;

			if (rpc->type > AUTH_RESP && handle->m_uid != id || handle->m_token != rpc->token)
				dsFailedAndLogIt(errorCode::DO_NOT_CONNECT_TO_DATABASE, "do not connect to database", WARNING);

			switch (rpc->type)
			{
			case SQL:
				dsReturn(processSql(handle, (const sqlMessage*)(cmd)));
			case PREPARE_STETEMENT_SQL:
				dsReturn(processPrepareStatmentSql(handle, (const prepareStatment*)(cmd)));
			case PREPARE_STETEMENT_ROW_DATA:
				dsReturn(processPrepareStatmentData(handle, (const prepareStatmentData*)(cmd)));
			case GET_META_REQ:
				dsReturn(processGetMetaReq(handle, (const getMetaReq*)(cmd)));
			default:
				dsReturn(closeConnect(handle, ILLEGAL_MESSAGE, "illegal message type"));
			};
		}
	};
}