#pragma once
#include <map>
#include <thread>
#include "occi.h"
#include "util/config.h"
#include "util/status.h"
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
		oracleConnect(config* conf);
		~oracleConnect();
	private:
		dsStatus& connect(oracle::occi::Connection*& conn, const std::string& sid, const std::string& serviceName);
	public:
		void closeStmt(oracle::occi::Connection* conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		void close(oracle::occi::Connection*& conn);

		void close(oracle::occi::Connection*& conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		bool isRac();

		dsStatus& init();

		dsStatus& connect(oracle::occi::Connection*& conn);

		dsStatus& connectByNodeId(int threadId, oracle::occi::Connection*& conn);

		static bool errorRecoverable(int errorCode);
	};

}
