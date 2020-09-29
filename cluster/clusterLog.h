#pragma once
#include <stdint.h>
#include <string>
#include <map>
#include "cluster.h"
#include "rpc.h"
#include "clusterLogFile.h"
#include "config.h"
namespace CLUSTER
{
	DLL_EXPORT class clusterLog {
	private:
		cluster* m_cluster;
		logIndexInfo m_logIndex;
		logIndexInfo m_commitLogIndex;
		uint64_t m_commited;
		uint64_t m_applied;
		clusterLogFile* m_currentLogFile;
		clusterLogConfig& m_logManagerConfig;
		logConfig& m_logConfig;
		std::map<logIndexInfo, clusterLogFile*> m_files;

		DLL_EXPORT dsStatus& rollback(const logIndexInfo& logIndex);
		DLL_EXPORT dsStatus& createNewFile(const logIndexInfo& logIndex);
		DLL_EXPORT clusterLogFile* find(const logIndexInfo& logIndex);
	public:
		clusterLog(cluster* c, clusterLogConfig& logManagerConfig, logConfig& logConfig) :m_cluster(c), m_logIndex(0, 0), m_commitLogIndex(0, 0), m_commited(0)
			, m_applied(0), m_currentLogFile(nullptr), m_logManagerConfig(logManagerConfig), m_logConfig(logConfig)
		{}
		~clusterLog()
		{
			close();
		}
		DLL_EXPORT void close();
		DLL_EXPORT dsStatus& init(const logIndexInfo& lastCommited);
		DLL_EXPORT dsStatus& append(const logEntryRpcBase* logEntry);
		DLL_EXPORT class iterator {
		private:
			clusterLog* m_log;
			logIndexInfo m_logIndex;
			clusterLogFile* m_currentLogFile;
			clusterLogFile::iterator m_iter;
		public:
			iterator(clusterLog* log, const logIndexInfo& logIndex) :m_log(log), m_logIndex(logIndex), m_currentLogFile(nullptr), m_iter() {}
			DLL_EXPORT dsStatus& seek(const logIndexInfo& logIndex);
			DLL_EXPORT dsStatus& next(const logEntryRpcBase*& logEntry);
			DLL_EXPORT dsStatus& next(const logEntryRpcBase*& logEntry, uint32_t outTime);
		};
	};
}
