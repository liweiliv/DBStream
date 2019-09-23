#pragma once
#include <string>
#include "messageWrap.h"
#include "util/winDll.h"
#include "util/config.h"
#include "replicatorConf.h"
namespace REPLICATOR
{
	class applier {
	protected:
		config *conf;
		int m_errno;
		std::string m_error;

		int m_id;
		bool m_batchCommit;
		bool m_dryRun;
		bool m_printSql;
		std::string m_txnDatabase;
		std::string m_txnTable;
	public:
		applier(uint32_t id,config* conf):conf(conf),m_errno(0),m_id(id)
		{
			m_batchCommit = conf->get(SECTION, "batchCommit", "true") == "true";
			m_dryRun = conf->get(SECTION, "batchDryrun", "true") == "true";
			m_printSql = conf->get(SECTION, "printSql", "true") == "true";
			m_txnDatabase = conf->get(SECTION, "txnDataBase", "");

			m_txnTable = conf->get(SECTION, "txnTable", "");
			if (!m_txnTable.empty())
			{
				char idStrBuf[32] = { 0 };
				sprintf(idStrBuf, "_%u", m_id);
				m_txnTable.append(idStrBuf);
			}
		}
		virtual ~applier() {}
		virtual int reconnect() = 0;
		virtual int apply(transaction* t) = 0;
		int getErrno()
		{
			return m_errno;
		}
		const std::string& getError()
		{
			return m_error;
		}
	};
	extern "C" DLL_EXPORT  applier* instance(int id, config* conf);
}
