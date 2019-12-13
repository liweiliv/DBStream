#pragma once
#include "occi.h"
#include "util/config.h"
#include "dataSource/dataSourceConf.h"
#include "glog/logging.h"
namespace DATA_SOURCE
{
	class oracleConnect {
	private:
		oracle::occi::Environment* m_env;
		config* m_conf;
		std::mutex m_lock;
	public:
		oracleConnect(config* conf):m_env(nullptr),m_conf(conf)
		{
			m_env = oracle::occi::Environment::createEnvironment(oracle::occi::Environment::DEFAULT);
		}
		~oracleConnect()
		{
			if (m_env != nullptr)
				delete m_env;
		}
		oracle::occi::Connection* connect()
		{
			std::string netkvPair = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)";
			netkvPair.append("(HOST=").append(m_conf->get(SECTION, std::string(CONN_SECTION).append(HOST).c_str())).append(")");
			netkvPair.append("(PORT=").append(m_conf->get(SECTION, std::string(CONN_SECTION).append(PORT).c_str())).append(")");
			std::string serviceName = m_conf->get(SECTION, std::string(CONN_SECTION).append(SERVICE_NAME).c_str());
			if (!serviceName.empty())
			{
				netkvPair.append("(CONNECT_DATA=(SERVICE_NAME=").append(serviceName).append("))");
			}
			else
			{
				std::string sid = m_conf->get(SECTION, std::string(CONN_SECTION).append(SID).c_str());
				if (!sid.empty())
				{
					netkvPair.append("(CONNECT_DATA=(SID=").append(sid).append("))");
				}
				else
				{
					LOG(ERROR) << "sid or service name must set in oracle datasource config";
					return nullptr;
				}
			}
			netkvPair.append(")");
			std::lock_guard<std::mutex> lg(m_lock);
			try {
				return m_env->createConnection(m_conf->get(SECTION, std::string(CONN_SECTION).append(USER).c_str()),
					m_conf->get(SECTION, std::string(CONN_SECTION).append(PASSWORD).c_str()),
					netkvPair
				);
			}
			catch (oracle::occi::SQLException e) {
				LOG(ERROR) << "create connect to oracle failed for:" << e.getErrorCode << "," << e.getMessage();
				return nullptr;
			}
		}
	};

}