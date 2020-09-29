#include "clusterLog.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "meta/metaData.h"

namespace CLUSTER {
	DLL_EXPORT dsStatus& clusterLog::append(const logEntryRpcBase* logEntry)
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
				dsFailedAndLogIt(errorCode::prevNotMatch, "", ERROR);
			}
		}
		if (logEntry->leaderCommitIndex > m_commitLogIndex.logIndex)
		{
			//	dsReturnIfFailed(m_currentLogFile->writeCurrentBlock());
			m_commitLogIndex.logIndex = m_logIndex.logIndex > logEntry->leaderCommitIndex ? logEntry->leaderCommitIndex : m_logIndex.logIndex;
		}
		dsOk();
	}
	DLL_EXPORT dsStatus& clusterLog::rollback(const logIndexInfo& logIndex)
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
	DLL_EXPORT clusterLogFile* clusterLog::find(const logIndexInfo& logIndex)
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
	DLL_EXPORT dsStatus& clusterLog::createNewFile(const logIndexInfo& logIndex)
	{
		uint64_t fileId = 1;
		if (m_currentLogFile != nullptr)
			fileId = m_currentLogFile->getFileId() + 1;
		String fileName = m_logManagerConfig.getLogDir() + "/" + m_logManagerConfig.getLogFileNamePrefix() + ".";
		fileName.append(fileId).append(".log");
		clusterLogFile* file = new clusterLogFile(fileName.c_str(), fileId, m_logConfig);
		dsReturnIfFailed(file->create(m_currentLogFile, logIndex));
		m_files.insert(std::pair<logIndexInfo, clusterLogFile*>(logIndex, file));
		m_currentLogFile->unUse();
		m_currentLogFile = file;
		dsOk();
	}
	DLL_EXPORT void clusterLog::close()
	{
		if (m_currentLogFile != nullptr)
		{
			m_currentLogFile->writeCurrentBlock();
			m_currentLogFile->unUse();
		}
		for (std::map<logIndexInfo, clusterLogFile*>::iterator iter = m_files.begin(); iter != m_files.end(); iter++)
		{
			clusterLogFile* file = iter->second;
			file->close();
			delete file;
			iter->second = nullptr;
		}
		m_files.clear();
		m_currentLogFile = nullptr;
	}
	DLL_EXPORT dsStatus& clusterLog::init(const logIndexInfo& lastCommited)
	{
		std::map<uint64_t, std::string> files;
		std::string logDir = m_logManagerConfig.getLogDir();
		std::string logPrefix = m_logManagerConfig.getLogFileNamePrefix();
#ifdef OS_WIN
		WIN32_FIND_DATA findFileData;
		std::string findString(logDir);
		findString.append("\\").append(logPrefix).append(".*");
		CreateDirectory(logDir.c_str(), nullptr);
		HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
		if (INVALID_HANDLE_VALUE == hFind && errno != 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "open data dir:" << logDir << " failed,errno:" << errno << "," << strerror(errno), ERROR);
		}
		if (INVALID_HANDLE_VALUE != hFind)
		{
			do
			{
				if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
					continue;
				const char* fileName = findFileData.cFileName;
#endif
#ifdef OS_LINUX
				DIR* dir = opendir(m_logManagerConfig.getLogDir().c_str());
				dirent* file;
				if (dir == nullptr)
				{
					if (0 != mkdir(logDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))
						dsFailedAndLogIt(errorCode::ioError, "create data dir:" << m_logManagerConfig.getLogDir() << " failed,errno:" << errno << "," << strerror(errno), ERROR);
					if(nullptr == (dir = opendir(m_logManagerConfig.getLogDir().c_str())))
						dsFailedAndLogIt(errorCode::ioError, "open data dir:" << m_logManagerConfig.getLogDir() << " failed,errno:" << errno << "," << strerror(errno), ERROR);
				}
				while ((file = readdir(dir)) != nullptr)
				{
					if (file->d_type != 8)
						continue;
					const char* fileName = file->d_name;
#endif
					if (strncmp(fileName, logPrefix.c_str(), logPrefix.size()) != 0)
						continue;
					if (fileName[logPrefix.size()] != '.')
						continue;
					const char* pos = fileName + logPrefix.size() + 1;
					uint64_t id = 0;
					while (*pos <= '9' && *pos >= '0')
					{
						id = id * 10 + *pos - '0';
						pos++;
					}
					if (*pos == '\0')
						files.insert(std::pair<uint64_t, std::string>(id, fileName));
#ifdef OS_WIN
			} while (FindNextFile(hFind, &findFileData));
			FindClose(hFind);
		}
#endif
#ifdef OS_LINUX
		}
		closedir(dir);
#endif
		if (!files.empty())
		{
			uint64_t prev = 0;
			for (std::map<uint64_t, std::string>::iterator iter = files.begin(); iter != files.end(); iter++)
			{
				if (iter->first != prev + 1 && prev != 0)
				{
					dsFailedAndLogIt(errorCode::missingLogFile, "cluster log load from dir:" << logDir << " failed, missing log file:"
						<< logPrefix << "." << (prev + 1), ERROR);
				}
				prev = iter->first;
				clusterLogFile* logFile = new clusterLogFile((logDir + "/" + iter->second).c_str(), iter->first, m_logConfig);
				dsStatus& rtv = logFile->readMetaInfo();
				if (!dsCheck(rtv))
				{
					logFile->close();
					delete logFile;
					dsReturn(rtv);
				}
				logFile->close();
				m_files.insert(std::pair<logIndexInfo, clusterLogFile*>(logFile->getBeginLogIndex(), logFile));
			}
			m_currentLogFile = m_files.rbegin()->second;
			dsReturn(m_currentLogFile->recovery());
		}
		else
		{
			if (lastCommited.term == 0 && lastCommited.logIndex == 0)
			{
				m_currentLogFile = new clusterLogFile((logDir + "/" + logPrefix + ".1.log").c_str(), 1, m_logConfig);
				if (!dsCheck(m_currentLogFile->create({ 1,0, }, { 1,1 })) || !dsCheck(m_currentLogFile->use()))
				{
					m_currentLogFile->close();
					delete m_currentLogFile;
					m_currentLogFile = nullptr;
					dsReturn(getLocalStatus());
				}
				m_currentLogFile->use();
				m_files.insert(std::pair<logIndexInfo, clusterLogFile*>({ 1,1 }, m_currentLogFile));
			}
			else
			{
				dsFailedAndLogIt(errorCode::missingLogFile, "last commited logIndex is :" << lastCommited.term << "." << lastCommited.logIndex << ",not 0.0,but do not find any log file in " << logDir, ERROR);
			}
		}
		dsOk();
	}

	DLL_EXPORT dsStatus& clusterLog::iterator::seek(const logIndexInfo& logIndex)
	{
		clusterLogFile* file = m_log->find(logIndex);
		if (file == nullptr)
		{
			dsFailedAndLogIt(errorCode::logIndexNotFound, "can not find" << logIndex.term << "." << logIndex.logIndex, ERROR);
		}
		dsReturnIfFailed(m_iter.setLogFile(file));
		dsReturnIfFailed(m_iter.search(logIndex));
		m_currentLogFile = file;
		dsOk();
	}

	DLL_EXPORT dsStatus& clusterLog::iterator::next(const logEntryRpcBase*& logEntry, uint32_t outTime)
	{
		logEntry = nullptr;
		do {
			dsStatus &rtv = m_iter.next(logEntry, 10);
			if (unlikely(!dsCheck(rtv)))
			{
				if (rtv.code == errorCode::endOfFile)
				{
					if (m_currentLogFile->getNext() != nullptr)
					{
						dsReturnIfFailed(m_iter.attachToNextLogFile(m_currentLogFile->getNext()));
						m_currentLogFile = m_currentLogFile->getNext();
						continue;
					}
					else
						dsOk();
				}
				else
				{
					dsReturn(rtv);
				}
			}
			if (logEntry == nullptr)
				outTime -= 10;
			else
				dsOk();
		} while (outTime > 0);
		dsOk();
	}
	DLL_EXPORT dsStatus& clusterLog::iterator::next(const logEntryRpcBase*& logEntry)
	{
		logEntry = nullptr;
		do {
			dsStatus &rtv = m_iter.next(logEntry);
			if (unlikely(!dsCheck(rtv)))
			{
				if (rtv.code == errorCode::endOfFile)
				{
					if (m_currentLogFile->getNext() != nullptr)
					{
						dsReturnIfFailed(m_iter.attachToNextLogFile(m_currentLogFile->getNext()));
						m_currentLogFile = m_currentLogFile->getNext();
						continue;
					}
					else
						dsOk();
				}
				else
				{
					dsReturn(rtv);
				}
			}
			else
			{
				dsOk();
			}
		} while (true);
	}
}
