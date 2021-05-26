#pragma once
#include "oci.h"
#include "oracleConnectBase.h"
namespace DATA_SOURCE
{
	class ociConnect :public oracleConnectBase
	{
	public:
		struct oci
		{
			OCIEnv* envp;                                   /* Environment handle */
			OCIError* errp;                                         /* Error handle */
			OCIServer* srvp;                                        /* Server handle */
			OCISvcCtx* svcp;                                       /* Service handle */
			OCIStmt* stmtp;
			oci() :envp(nullptr), errp(nullptr), srvp(nullptr), svcp(nullptr), stmtp(nullptr)
			{

			}
			~oci()
			{
				if (stmtp != nullptr)
					OCIStmtRelease(stmtp, errp, nullptr, 0, OCI_DEFAULT);
				if (srvp != nullptr)
					OCIHandleFree((dvoid*)svcp, (ub4)OCI_HTYPE_SVCCTX);
				if (svcp != nullptr)
					OCILogoff(svcp, errp);
				if (errp != nullptr)
					OCIHandleFree((dvoid*)errp, (ub4)OCI_HTYPE_ERROR);
			}
			DS openStmt()
			{
				if (stmtp == nullptr)
				{
					if (OCI_SUCCESS != OCIHandleAlloc((dvoid*)envp, (dvoid**)&stmtp,
						(ub4)OCI_HTYPE_STMT, (size_t)0, (dvoid**)0))
					{
						dsFailedAndLogIt(1, "call OCIHandleAlloc to create stmt failed", ERROR);
					}
				}
				dsOk();
			}
			DS setFetchSize(uint32_t fetchSize)
			{
				if (stmtp == nullptr)
					dsFailedAndLogIt(1, "stmt has been closed", ERROR);
				if (OCI_SUCCESS != (OCIAttrSet(stmtp, OCI_HTYPE_STMT, &fetchSize, 4, OCI_ATTR_PREFETCH_ROWS, errp)))
				{
					int32_t errcode;
					uint8_t errBuf[512] = { 0 };
					OCIErrorGet(envp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
					dsFailedAndLogIt(1, "setFetchSize failed for:" << errcode << ":" << (const char*)errBuf, ERROR);
				}
				dsOk();
			}
			void closeStmt()
			{
				if (stmtp != nullptr)
					OCIStmtRelease(stmtp, errp, nullptr, 0, OCI_DEFAULT);
				stmtp = nullptr;
			}
		};
	private:
		OCIEnv* m_envp;
	private:
		DS createEnv();
		DLL_EXPORT DS connect(oci*& conn, const std::string& sid, const std::string& serviceName);
	public:
		DLL_EXPORT ociConnect(config* conf);
		DLL_EXPORT ~ociConnect();
		DLL_EXPORT DS init();
		DLL_EXPORT DS connect(oci*& conn);
		DLL_EXPORT DS connectByNodeId(int threadId, oci*& conn);

	};
}