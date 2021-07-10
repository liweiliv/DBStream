#pragma once
#include <stdint.h>
#include <string>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include "auth/client.h"
#include "util/arrayQueue.h"
namespace AUTH {
	class serverHandle;
}
namespace RPC{
	struct DMLRecord;
}
namespace KVDB {
	class transaction;
	struct rowChange;
	struct version;
	struct rpcHead;
	class clientHandle {
	private:
		AUTH::serverHandle* m_authHandle;
	public:
		uint32_t m_uid;
		uint32_t m_token;
		uint64_t m_txnId;
		std::string m_currentDatabase;
		std::string m_ip;
		uint32_t m_port;
		transaction * m_trans;
		rowChange* m_change;
		uint32_t m_sendRpcId;
		uint32_t m_recvRpcId;

		std::mutex m_lock;
		std::condition_variable m_cond;

		volatile bool m_running;
		volatile bool m_ready;
		volatile bool m_busy;
		arrayQueue<rpcHead *> m_preProcessResp;
		char m_resultDefaultBuffer[2048];
		const RPC::DMLRecord* m_result;


	public:
		AUTH::serverHandle* getAuthHandle()
		{
			return m_authHandle;
		}
		void setAuthHandle(AUTH::serverHandle* handle)
		{
			m_authHandle = handle;
		}
		void copyResult(const version ** v);
		inline void resetReady()
		{
			m_ready = false;
		}
		void wait()
		{
			while (m_running)
			{
				if (m_ready)
					break;
				std::unique_lock<std::mutex> lock(m_lock);
				auto t = std::chrono::system_clock::now();
				t += std::chrono::milliseconds(100);
				if (m_cond.wait_until(lock, t) == std::cv_status::timeout)
					continue;
			}
		}
		void weakUp()
		{
			m_ready = true;
			m_cond.notify_all();
		}
	};
}