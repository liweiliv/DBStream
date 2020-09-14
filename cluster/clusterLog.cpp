#include "clusterLog.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "meta/metaData.h"

namespace CLUSTER {
	dsStatus& clusterLog::rollback(const logIndexInfo& logIndex)
	{
		LOG(WARNING) << "cluster log start rollback to " << logIndex.term << "." << logIndex.logIndex;
		if (m_commitLogIndex >= logIndex)
		{
			dsFailedAndLogIt(errorCode::rollbackCommited, "rollback to" << logIndex.term << "." << logIndex.logIndex << ",but commitLogIndex is:" <<
				m_commitLogIndex.term << "." << m_commitLogIndex.logIndex
				, ERROR);
		}
		clusterLogFile* file = find(logIndex);
		if (file == nullptr)
		{
			if (m_files.empty())
				file = m_currentLogFile;
			else
				file = m_files.begin()->second;
			if (file->getBeginLogIndex() > logIndex)
			{
				dsFailedAndLogIt(errorCode::rollbackTooEarlier, "get new logEntry:" << logIndex.term << "." << logIndex.logIndex << " earlier than begin of all exist log files:" << file->getBeginLogIndex().term << "." << file->getBeginLogIndex().logIndex, ERROR);
			}
			else
			{
				dsFailedAndLogIt(errorCode::logIndexNotFound, "get illegal logEntry:" << logIndex.term << "." << logIndex.logIndex << ",need rollback to it,but can not find it", ERROR);
			}
		}
		m_currentLogFile = file;
		for (file = file->getNext(); file != nullptr; file = file->getNext())
		{
			dsReturnIfFailed(file->deleteFile());
			m_files.erase(file->getLogIndex());
			delete file;
		}
		dsReturnIfFailed(m_currentLogFile->rollback(logIndex));
		m_files.erase(m_currentLogFile->getLogIndex());
		m_currentLogFile->setNext(nullptr);
		LOG(WARNING) << "cluster log rollback to " << logIndex.term << "." << logIndex.logIndex << " success";
		dsOk();
	}
	clusterLogFile* clusterLog::find(const logIndexInfo& logIndex)
	{
		if (m_files.empty())
			return nullptr;
		//BeginLogIndex of last log file is less than  logIndex,logIndex must be in last log file,or not exist
		if (m_currentLogFile->getBeginLogIndex() <= logIndex)
			return m_currentLogFile;
		//find last log file which BeginLogIndex less than logIndex
		std::map<logIndexInfo, clusterLogFile*>::const_iterator iter;
		if ((iter = m_files.lower_bound(logIndex)) == m_files.end())
			return nullptr;
		if (iter->second->getBeginLogIndex() > logIndex)
			return nullptr;
		return iter->second;
	}
	dsStatus& clusterLog::createNewFile(const logIndexInfo& logIndex)
	{
		uint64_t fileId = 1;
		if (m_currentLogFile != nullptr)
			fileId = m_currentLogFile->getFileId() + 1;
		String fileName = m_logDir + "/" + m_logFilePrefix + ".";
		fileName.append(fileId).append(".").append(m_logFileSuffix);
		clusterLogFile* file = new clusterLogFile(fileName.c_str(), fileId, m_logConfig);
		if (m_currentLogFile != nullptr)
		{
			dsReturnIfFailed(file->create(m_currentLogFile, logIndex));
			m_files.insert(std::pair<logIndexInfo, clusterLogFile*>(m_currentLogFile->getLogIndex(), m_currentLogFile));
		}
		else
		{
			dsReturnIfFailed(file->create(m_logIndex, logIndex));
		}
		m_currentLogFile = file;
		dsOk();
	}
}