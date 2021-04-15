#pragma once
#include <map>
#include <thread>
#include "occi.h"
#include "util/config.h"
#include "util/status.h"
#include "util/winDll.h"
#include "dataSource/dataSourceConf.h"
namespace DATA_SOURCE
{
	class oracleConnect {
	public:
		struct nodeInfo {
			std::string sid;
			int threadId;
			bool isOpen;
			std::string enabled;
			nodeInfo() :threadId(1), isOpen(true) {}
			nodeInfo(const nodeInfo& n) :sid(n.sid), threadId(n.threadId), isOpen(n.isOpen), enabled(n.enabled) {}
			nodeInfo& operator=(const nodeInfo& n)
			{
				sid = n.sid;
				threadId = n.threadId;
				isOpen = n.isOpen;
				enabled = n.enabled;
				return *this;
			}
		};

	private:
		oracle::occi::Environment* m_env;
		std::string m_host;
		std::string m_port;
		std::string m_user;
		std::string m_password;
		std::string m_serviceName;
		std::string m_sid;
		std::mutex m_lock;
		std::map<int, nodeInfo> m_allNodes;
		bool m_isScanIp;
	public:
		DLL_EXPORT oracleConnect(config* conf);
		DLL_EXPORT ~oracleConnect();
	private:
		DLL_EXPORT DS connect(oracle::occi::Connection*& conn, const std::string& sid, const std::string& serviceName);
	public:
		DLL_EXPORT void closeStmt(oracle::occi::Connection* conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		DLL_EXPORT void close(oracle::occi::Connection*& conn);

		DLL_EXPORT void close(oracle::occi::Connection*& conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		DLL_EXPORT bool isRac();

		DLL_EXPORT DS init();

		DLL_EXPORT DS connect(oracle::occi::Connection*& conn);

		DLL_EXPORT DS connectByNodeId(int threadId, oracle::occi::Connection*& conn);

		DLL_EXPORT static bool errorRecoverable(int errorCode);
	};

}
