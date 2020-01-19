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
		oracleConnect(config* conf) :m_env(nullptr), m_conf(conf)
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
			netkvPair.append("(HOST=").append(m_conf->get(SECTION, HOST).c_str()).append(")");
			netkvPair.append("(PORT=").append(m_conf->get(SECTION, PORT).c_str()).append(")");
			std::string serviceName = m_conf->get(SECTION, SERVICE_NAME);
			if (!serviceName.empty())
			{
				netkvPair.append("(CONNECT_DATA=(SERVICE_NAME=").append(serviceName).append("))");
			}
			else
			{
				std::string sid = m_conf->get(SECTION, SID);
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
			for(int retry = 10;retry>=0;retry--)
			{
				try 
				{
					return m_env->createConnection(m_conf->get(SECTION, USER),
						m_conf->get(SECTION, PASSWORD),
						netkvPair
					);
				}
				catch (oracle::occi::SQLException e) 
				{
					LOG(ERROR) << "create connect to oracle failed for:" << e.getErrorCode << "," << e.getMessage();
					if (!errorRecoverable(e.getErrorCode()))
						return nullptr;
				}
			}
			return nullptr;
		}

		static bool errorRecoverable(int errorCode)
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