#include "ociConnect.h"
namespace DATA_SOURCE
{

	DLL_EXPORT ociConnect::ociConnect(config* conf) :oracleConnectBase(conf), m_envp(nullptr)
	{
	}

	DLL_EXPORT ociConnect::~ociConnect()
	{
		if (m_envp != nullptr)
		{
			OCIHandleFree((dvoid*)m_envp, (ub4)OCI_HTYPE_ENV);
		}
	}

	DS ociConnect::createEnv()
	{
		if (m_envp == nullptr)
		{
			std::lock_guard<std::mutex> lock(m_lock);
			if (m_envp == nullptr)
			{
				if (OCIEnvNlsCreate(&m_envp, OCI_OBJECT, (dvoid*)0,
					(dvoid * (*)(dvoid*, size_t)) 0,
					(dvoid * (*)(dvoid*, dvoid*, size_t))0,
					(void (*)(dvoid*, dvoid*)) 0,
					(size_t)0, (dvoid**)0, 0, 0))
				{
					dsFailedAndLogIt(1, "OCIEnvCreate() failed", ERROR);
				}
			}
		}
		dsOk();
	}

	DLL_EXPORT DS ociConnect::init()
	{
		dsReturnIfFailed(createEnv());
		oci* conn = nullptr;
		dsReturnIfFailed(connect(conn));
		if (!dsCheck(conn->openStmt()))
		{
			delete conn;
			dsReturn(getLocalStatus().code);
		}
		if (OCIStmtPrepare(conn->stmtp, conn->errp, (const text*)GET_THREAD_INFO_SQL, (ub4)strlen(GET_THREAD_INFO_SQL), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT) != OCI_SUCCESS)
		{
			int32_t errcode;
			uint8_t errBuf[512] = { 0 };
			OCIErrorGet(conn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
			delete conn;
			dsFailedAndLogIt(1, "call OCIStmtPrepare failed for " << errcode << "," << (char*)errBuf, ERROR);
		}
		OCIDefine* defnp[4] = { 0 };
		int threadId = 0;
		oratext status[7] = { 0 };
		oratext enabled[9] = { 0 };
		oratext instance[17] = { 0 };


		OCIDefineByPos(conn->stmtp, &defnp[0], conn->errp, (ub4)1, &threadId, 4, SQLT_INT, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);
		OCIDefineByPos(conn->stmtp, &defnp[1], conn->errp, (ub4)2, status, sizeof(status), SQLT_STR, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);
		OCIDefineByPos(conn->stmtp, &defnp[2], conn->errp, (ub4)3, enabled, sizeof(enabled), SQLT_STR, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);
		OCIDefineByPos(conn->stmtp, &defnp[3], conn->errp, (ub4)4, instance, sizeof(instance), SQLT_STR, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);

		if (OCIStmtExecute(conn->svcp, conn->stmtp, conn->errp, (ub4)1, (ub4)0, (OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT) != OCI_SUCCESS)
		{
			int32_t errcode;
			uint8_t errBuf[512] = { 0 };
			OCIErrorGet(conn->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
			delete conn;
			dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << errcode << "," << (char*)errBuf, ERROR);
		}

		do
		{
			nodeInfo node;
			node.threadId = threadId;
			node.isOpen = strcmp((const char*)status, "OPEN") == 0;
			node.enabled = (const char*)enabled;
			node.sid = (const char*)instance;
			m_allNodes.insert(std::pair<int, nodeInfo>(node.threadId, node));
		} while (OCIStmtFetch2(conn->stmtp, conn->errp, 1, OCI_FETCH_NEXT, 1, OCI_DEFAULT) == OCI_SUCCESS);
		delete conn;
		conn = nullptr;
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
				delete conn;
				conn = nullptr;
			}
			m_isScanIp = true;
		}
		dsOk();
	}

	DLL_EXPORT DS ociConnect::connect(oci*& conn, const std::string& sid, const std::string& serviceName)
	{
		oci* ocip = new oci;

		std::string netkvPair = "(DESCRIPTION=(ADDRESS=";
		netkvPair.append("(PROTOCOL=tcp)");
		netkvPair.append("(HOST=").append(m_host).append(")");
		netkvPair.append("(PORT=").append(m_port).append(")");
		netkvPair.append(")");
		if (!m_serviceName.empty())
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
				dsFailedAndLogIt(-1, "sid or service name must set in oracle datasource config", ERROR);
			}
		}
		netkvPair.append(")");

		if (m_envp == nullptr)
		{
			std::lock_guard<std::mutex> lock(m_lock);
			if (m_envp == nullptr)
			{
				if (OCIEnvNlsCreate(&m_envp, OCI_OBJECT, (dvoid*)0,
					(dvoid * (*)(dvoid*, size_t)) 0,
					(dvoid * (*)(dvoid*, dvoid*, size_t))0,
					(void (*)(dvoid*, dvoid*)) 0,
					(size_t)0, (dvoid**)0, 0, 0))
				{
					dsFailedAndLogIt(1, "OCIEnvCreate() failed", ERROR);
				}
			}
		}
		ocip->envp = m_envp;
		if (OCIHandleAlloc((dvoid*)ocip->envp, (dvoid**)&ocip->errp,
			(ub4)OCI_HTYPE_ERROR, (size_t)0, (dvoid**)0))
		{
			dsFailedAndLogIt(1, "OCIHandleAlloc(OCI_HTYPE_ERROR) failed", ERROR);
		}
		/* Logon to database */
		sword rtv = 0;
		if (OCI_SUCCESS != (rtv = OCILogon(ocip->envp, ocip->errp, &ocip->svcp, (const uint8_t*)m_user.c_str(),
			m_user.size(), (const uint8_t*)m_password.c_str(), m_password.size(), (const uint8_t*)netkvPair.c_str(), netkvPair.size())))
		{
			if (rtv == OCI_ERROR)
			{
				int32_t errcode;
				uint8_t errBuf[512] = { 0 };
				OCIErrorGet(ocip->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
				dsFailedAndLogIt(1, "logon to oracle failed for:" << errcode << ":" << (const char*)errBuf, ERROR);
			}
			else
			{
				dsFailedAndLogIt(1, "logon to oracle failed for:" << rtv, ERROR);
			}
		}

		/* allocate the server handle */
		if (OCI_SUCCESS != (rtv = OCIHandleAlloc((dvoid*)ocip->envp, (dvoid**)&ocip->srvp,
			OCI_HTYPE_SERVER, (size_t)0, (dvoid**)0)))
		{
			OCIServerDetach(ocip->srvp, ocip->errp, OCI_DEFAULT);
			OCIHandleFree((dvoid*)ocip, (ub4)OCI_HTYPE_ERROR);

			dsFailedAndLogIt(1, "OCIHandleAlloc(OCI_HTYPE_ERROR) failed", ERROR);
		}
		conn = ocip;
		dsOk();
	}

	DLL_EXPORT DS ociConnect::connect(oci*& conn)
	{
		dsReturn(connect(conn, m_sid, m_serviceName));
	}

	DLL_EXPORT DS ociConnect::connectByNodeId(int threadId, oci*& conn)
	{
		std::map<int, nodeInfo>::const_iterator iter = m_allNodes.find(threadId);
		if (iter == m_allNodes.end())
			dsFailedAndLogIt(-1, "threadId:" << threadId << " not exist", ERROR);
		dsReturn(connect(conn, iter->second.sid, ""));
	}
}