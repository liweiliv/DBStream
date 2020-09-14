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
	class clusterLog {
	private:
		cluster* m_cluster;
		std::string m_logDir;
		logIndexInfo m_logIndex;
		logIndexInfo m_commitLogIndex;
		uint64_t m_commited;
		uint64_t m_applied;
		uint64_t m_maxFileSize;
		uint32_t m_batchSizeForIndex;
		std::string m_logFilePrefix;
		std::string m_logFileSuffix;
		clusterLogFile* m_currentLogFile;
		clusterLogConfig &m_logManagerConfig;
		logConfig& m_logConfig;
		std::map<logIndexInfo, clusterLogFile*> m_files;

		dsStatus& rollback(const logIndexInfo& logIndex);
		dsStatus& createNewFile(const logIndexInfo& logIndex);
		clusterLogFile* find(const logIndexInfo& logIndex);
	public:
		clusterLog(cluster* c, clusterLogConfig& logManagerConfig, logConfig& logConfig):m_cluster(c),m_logManagerConfig(logManagerConfig),m_logConfig(logConfig)
		{}
		~clusterLog()
		{
			close();
		}
		void close()
		{
			for (std::map<logIndexInfo, clusterLogFile*>::iterator iter = m_files.begin(); iter != m_files.end(); iter++)
			{
				clusterLogFile* file = iter->second;
				file->close();
				delete file;
			}
			m_files.clear();
		}
		dsStatus& init()
		{
			//todo
			dsOk();
		}
		dsStatus& append(const logEntryRpcBase* logEntry)
		{
			dsStatus& s = m_currentLogFile->append(logEntry);
			if (!dsCheck(s))
			{
				switch (s.code)
				{
				case errorCode::full:
				{
					dsReturnIfFailed(m_currentLogFile->finish());
					dsReturnIfFailed(createNewFile(logEntry->logIndex));
					dsReturn(append(logEntry));
				}
				case errorCode::rollback:
				{
					dsReturnIfFailed(rollback(logEntry->logIndex));
					dsReturn(append(logEntry));
				}
				default:
					dsReturn(s);
				}
			}
			if (logEntry->logIndex.term > m_logIndex.term)
			{
				if (logEntry->logIndex.term != m_logIndex.term + 1)
				{
					dsFailedAndLogIt(errorCode::prevNotMatch, "",ERROR);
				}
			}
			if (logEntry->leaderCommitIndex > m_commitLogIndex.logIndex)
			{
				//todo
			}
			dsOk();
		}
		class iterator {
		private:
			clusterLog* m_log;
			logIndexInfo m_logIndex;
			clusterLogFile* m_currentLogFile;
			clusterLogFile::iterator m_iter;
		public:
			iterator(clusterLog* log,const logIndexInfo &logIndex) :m_log(log),m_logIndex(logIndex), m_currentLogFile(nullptr), m_iter(){}
			dsStatus& seek(const logIndexInfo& logIndex)
			{
				clusterLogFile * file = m_log->find(logIndex);
				if (file == nullptr)
				{
					dsFailedAndLogIt(errorCode::logIndexNotFound, "can not find" << logIndex.term << "." << logIndex.logIndex, ERROR);
				}
				m_iter.attachToNextLogFile(file);
			}
		};
	};
}
