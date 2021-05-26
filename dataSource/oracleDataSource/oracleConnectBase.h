#pragma once
#include <string>
#include "util/config.h"
#include "util/status.h"
#include "util/winDll.h"
#include "dataSource/dataSourceConf.h"
namespace DATA_SOURCE
{
constexpr static auto GET_THREAD_INFO_SQL = "select THREAD#, STATUS, ENABLED, INSTANCE from V$THREAD";

	class oracleConnectBase {
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
	protected:
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
		DLL_EXPORT oracleConnectBase(config* conf)
		{
			m_host = conf->get(SECTION, HOST);
			m_port = conf->get(SECTION, PORT);
			m_user = conf->get(SECTION, USER);
			m_password = conf->get(SECTION, PASSWORD);
			m_serviceName = conf->get(SECTION, SERVICE_NAME);
			m_sid = conf->get(SECTION, SID);
		}
		virtual DS init() = 0;
		DLL_EXPORT bool isRac()
		{
			return m_allNodes.size() > 1;
		}
		DLL_EXPORT static bool errorRecoverable(int errorCode)
		{
			switch (errorCode)
			{
			case 28:
			case 106:
			case 107:
			case 115:
			case 587:
			case 1090:
			case 1092:
			case 1093:
			case 1094:
			case 17002:
				return true;
			default:
				return false;
			}
		}
	};
}