#include <map>
#include <thread>
#include <glog/logging.h>
#include "occi.h"
#include "util/config.h"
#include "util/status.h"
#include "dataSource/dataSourceConf.h"
#include "occiConnect.h"
namespace DATA_SOURCE
{
	DLL_EXPORT occiConnect::occiConnect(config* conf) :oracleConnectBase(conf), m_env(nullptr)
	{
	}

	DLL_EXPORT occiConnect::~occiConnect()
	{
		if (m_env != nullptr)
			delete m_env;
	}
	DLL_EXPORT DS occiConnect::connect(oracle::occi::Connection*& conn, const std::string& sid, const std::string& serviceName)
	{
		conn = nullptr;
		std::string errInfo;
		std::string netkvPair = "(DESCRIPTION=(ADDRESS=";
		netkvPair.append("(PROTOCOL=tcp)");
		netkvPair.append("(HOST=").append(m_host).append(")");
		netkvPair.append("(PORT=").append(m_port).append(")");
		netkvPair.append(")");
		if (!serviceName.empty())
		{
			netkvPair.append("(CONNECT_DATA=(SERVICE_NAME=").append(serviceName).append("))");
		}
		else
		{
			if (!sid.empty())
			{
				netkvPair.append("(CONNECT_DATA=(SID=").append(sid).append("))");
			}
			else
			{
				dsFailedAndLogIt(1, "sid or service name must set in oracle datasource config", ERROR);
			}
		}
		netkvPair.append(")");
		std::lock_guard<std::mutex> lg(m_lock);
		for (int retry = 10; retry >= 0; retry--)
		{
			try
			{
				conn = m_env->createConnection(m_user.c_str(), m_password.c_str(), netkvPair.c_str());
				dsOk();
			}
			catch (oracle::occi::SQLException e)
			{
				LOG(ERROR) << "create connect to oracle " << netkvPair << " failed for:" << e.what();
				if (!errorRecoverable(e.getErrorCode()))
					dsFailedAndLogIt(1, "create connect to oracle " << netkvPair << " failed for:" << e.what(), ERROR);
				errInfo = e.what();
				std::this_thread::sleep_for(std::chrono::seconds(1));
			}
		}
		dsFailedAndLogIt(1, "create connect to oracle " << netkvPair << " failed for retry too many times, last error:" << errInfo, ERROR);
	}
	DLL_EXPORT void occiConnect::closeStmt(oracle::occi::Connection* conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs)
	{
		if (rs != nullptr)
		{
			stmt->closeResultSet(rs);
			rs = nullptr;
		}
		if (stmt != nullptr)
		{
			conn->terminateStatement(stmt);
			stmt = nullptr;
		}
	}

	DLL_EXPORT void occiConnect::close(oracle::occi::Connection*& conn)
	{
		if (conn != nullptr)
		{
			m_env->terminateConnection(conn);
			conn = nullptr;
		}
	}

	DLL_EXPORT void occiConnect::close(oracle::occi::Connection*& conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs)
	{
		closeStmt(conn, stmt, rs);
		close(conn);
	}

	DLL_EXPORT DS occiConnect::init()
	{
		oracle::occi::Connection* conn = nullptr;
		oracle::occi::Statement* stmt = nullptr;
		oracle::occi::ResultSet* rs = nullptr;
		try
		{
			m_env = oracle::occi::Environment::createEnvironment(oracle::occi::Environment::DEFAULT);
			dsReturnIfFailed(connect(conn));
			stmt = conn->createStatement();
			oracle::occi::ResultSet* rs = stmt->executeQuery(GET_THREAD_INFO_SQL);
			if (rs == nullptr || !rs->next())
			{
				close(conn, stmt, rs);
				dsFailedAndLogIt(-1, "unexpect empty result when select data from V$THREAD", ERROR);
			}
			do {
				nodeInfo node;
				node.threadId = rs->getInt(1);
				node.isOpen = rs->getString(2).compare("OPEN") == 0;
				node.enabled = rs->getString(3);
				node.sid = rs->getString(4);
				m_allNodes.insert(std::pair<int, nodeInfo>(node.threadId, node));
			} while (rs->next());
			close(conn, stmt, rs);
			if (m_allNodes.size() > 1)
			{
				for (std::map<int, nodeInfo>::const_iterator iter = m_allNodes.begin(); iter != m_allNodes.end(); iter++)
				{
					if (!dsCheck(connectByNodeId(iter->first, conn)))
					{
						LOG(WARNING) << "connect to node:" << iter->first << " by sid:" << iter->second.sid << " failed, this is not scan ip, or can not connect to some nodes direct";
						resetStatus();
						dsOk();
					}
					close(conn);
				}
				m_isScanIp = true;
			}
			dsOk();
		}
		catch (oracle::occi::SQLException e)
		{
			close(conn, stmt, rs);
			dsFailedAndLogIt(1, "select data from V$THREAD failed for" << e.what(), ERROR);
		}
	}

	DLL_EXPORT DS occiConnect::connect(oracle::occi::Connection*& conn)
	{
		dsReturn(connect(conn, m_sid, m_serviceName));
	}

	DLL_EXPORT DS occiConnect::connectByNodeId(int threadId, oracle::occi::Connection*& conn)
	{
		std::map<int, nodeInfo>::const_iterator iter = m_allNodes.find(threadId);
		if (iter == m_allNodes.end())
			dsFailedAndLogIt(1, "threadId:" << threadId << " not exist", ERROR);
		dsReturn(connect(conn, iter->second.sid, ""));
	}


}
