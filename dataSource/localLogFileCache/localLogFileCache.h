#pragma once
#include <stdint.h>
#include <list>
#include <set>
#include <thread>
#include "memoryInfo.h"
#include "util/config.h"
#include "util/timer.h"
#include "memoryInfo.h"
#include "../dataSourceConf.h"
#include "logFile.h"
namespace DATA_SOURCE
{
	class localLogFileCache
	{
	private:
		logFile* m_current;
		logFile* m_begin;
		logFile* m_firstUnFlushed;
		logFile* m_firstNotNeedPurged;

		std::string m_baseDir;

		uint32_t m_maxFileSize;
		bool m_rotateBySize;
		uint32_t m_maxFileTime;
		bool m_rotateByTime;
		uint32_t m_currentFileCreateTime;
		logSeqNo m_purgeToSeq;

		char* m_compressBuf;
		uint32_t m_compressBufSize;

		memoryInfo m_memoryInfo;

		std::thread m_flushAndPurgeThread;
		dsStatus m_errorInfo;

	private:
		void purge()
		{
			if (m_firstNotNeedPurged == nullptr)
				m_firstNotNeedPurged = m_begin;
			if (m_firstNotNeedPurged == nullptr)
				return;
			while (m_firstNotNeedPurged->getFileId() < m_purgeToSeq.fileId && m_firstNotNeedPurged->getNext() != nullptr)
				m_firstNotNeedPurged = m_firstNotNeedPurged->getNext();

			logFile* purgeEnd = m_firstNotNeedPurged;
			if (purgeEnd != nullptr)
			{
				logFile* p = m_begin;
				while (p != purgeEnd)
				{
					logFile* next = p->getNext();
					p->clear(true);
					delete p;
					p = next;
				}
				m_begin = purgeEnd;
			}
			if (m_firstNotNeedPurged != nullptr && m_firstNotNeedPurged->getFileId() <= m_purgeToSeq.fileId)
				m_firstNotNeedPurged->purgeTo(m_purgeToSeq.seqNo);

		}
		DS flush(logFile* begin, bool flushAll, bool flushIndex)
		{
			logFile* unFlushed = begin;
			while (unFlushed != nullptr && (flushAll || m_memoryInfo.flowIsHigh()))
			{
				if (!unFlushed->getMetaInfo().getFlag(META_FLAG::FLUSHED))
					dsReturnIfFailed(unFlushed->flush(m_baseDir.c_str(), m_compressBuf, m_compressBufSize));
				if (flushIndex && unFlushed->getMetaInfo().getFlag(META_FLAG::FINISHED))
					dsReturnIfFailed(unFlushed->flushIndex(m_baseDir.c_str(), m_compressBuf, m_compressBufSize));
				if (unFlushed->getMetaInfo().getFlag(META_FLAG::FLUSHED) && m_firstUnFlushed == unFlushed)
					m_firstUnFlushed = unFlushed->getNext();
				unFlushed = unFlushed->getNext();
			}
			dsOk();
		}

		void flushAndPurgeThread()
		{
			while (m_memoryInfo.isRunning())
			{
				m_memoryInfo.waitFlushCond();
				purge();
				if (!dsCheck(flush(m_firstNotNeedPurged, false, false)))
				{
					m_errorInfo = getLocalStatus();
					LOG(ERROR) << m_errorInfo.toString();
					m_memoryInfo.stop();
					break;
				}
				if (m_memoryInfo.flowIsVeryHigh())
				{
					purge();
					if (!dsCheck(flush(m_begin, false, true)))
					{
						m_errorInfo = getLocalStatus();
						LOG(ERROR) << m_errorInfo.toString();
						m_memoryInfo.stop();
						break;
					}
				}
			}
			if (!dsCheck(flush(m_begin, true, true)))
			{
				m_errorInfo = getLocalStatus();
				LOG(ERROR) << m_errorInfo.toString();
			}
		}
		static int64_t getLogFileId(const char* name)
		{
			int64_t id = 0;
			const char* p = name;
			while (*p <= '9' && *p >= '0')
			{
				id = id * 10 + (*p) - '0';
				p++;
			}
			if (p == name)
				return -1;
			return strcmp(p, ".log") == 0 ? id : -1;
		}
	public:
		localLogFileCache(config* conf) :m_current(nullptr), m_begin(nullptr), m_firstUnFlushed(nullptr), m_firstNotNeedPurged(nullptr),
			m_baseDir(conf->get(SECTION, LOCAL_LOG_FILE_DIR, ".\\localLog")),
			m_maxFileSize(conf->getLong(SECTION, LOCAL_LOG_FILE_SIZE, 0, 0, 4LL * 1024 * 1024)), m_rotateBySize(m_maxFileSize > 0),
			m_maxFileTime(conf->getLong(SECTION, LOCAL_LOG_FILE_ROLL_TIME, 0, 0, 3600 * 24 * 356) * 1000), m_rotateByTime(m_maxFileTime > 0), m_currentFileCreateTime(0),
			m_compressBuf(nullptr), m_compressBufSize(0),
			m_memoryInfo(conf->getLong(SECTION, DEFAULT_LOCAL_LOG_SHARD_SIZE, DEFAULT_SHARED_SIZE, 1024, UINT32_MAX), conf->getLong(SECTION, MAX_LOCAL_CACHE_MEM, DEFAULT_MAX_LOCAL_CACHE_MEM, 1024 * 1024, INT64_MAX))

		{
			if (m_memoryInfo.getDefaultShardSize() > 0)
				m_compressBuf = new char[m_compressBufSize = LZ4_COMPRESSBOUND(m_memoryInfo.getDefaultShardSize())];
			m_purgeToSeq.seqNo = 0;
		}

		DS init(uint64_t purgedSeqNo = 0)
		{
			logSeqNo purgedLogSeqNo;
			purgedLogSeqNo.seqNo = purgedSeqNo;
			std::set<int64_t> fileIds;
#if defined  OS_WIN
			WIN32_FIND_DATA findFileData;
			std::string findString(m_baseDir);
			findString.append("\\*.log");
			CreateDirectory(m_baseDir.c_str(), nullptr);
			HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
			if (INVALID_HANDLE_VALUE == hFind && errno != 0)
			{
				dsFailedAndLogIt(1, "open data dir:" << m_baseDir << " failed,errno:" << errno << "," << strerror(errno), ERROR);
			}
			if (INVALID_HANDLE_VALUE != hFind)
			{
				do
				{
					if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
						continue;
					const char* fileName = findFileData.cFileName;
#elif defined OS_LINUX
			DIR* dir = opendir(m_baseDir.c_str());
			dirent* file;
			if (dir == nullptr)
			{
				if (0 != mkdir(m_baseDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))
					dsFailedAndLogIt(1, "create data dir:" << m_baseDir << " failed, errno:" << errno << "," << strerror(errno), ERROR);
				if (nullptr == (dir = opendir(m_baseDir.c_str())))
					dsFailedAndLogIt(1, "open data dir:" << m_baseDir << " failed, errno:" << errno << "," << strerror(errno), ERROR);
			}
			while ((file = readdir(dir)) != nullptr)
			{
				if (file->d_type != 8)
					continue;
				const char* fileName = file->d_name;
#endif
				int64_t fileId = getLogFileId(fileName);
				if (fileId < 0)
					continue;
				fileIds.insert(fileId);
#ifdef OS_WIN
				} while (FindNextFile(hFind, &findFileData));
				FindClose(hFind);
#endif
			}
		if (fileIds.empty())
		{
			if (m_rotateBySize || m_rotateByTime)
			{
				m_current = new logFile(1, &m_memoryInfo);
				m_firstUnFlushed = m_firstNotNeedPurged = m_begin = m_current;
				m_currentFileCreateTime = m_current->getCreateTime();
			}
			dsOk();
		}


		for (auto iter : fileIds)
		{
			String filePath;
			filePath.append(m_baseDir).append("/").append(iter).append(".log");
			if (purgedLogSeqNo.fileId > iter)
			{
				LOG(INFO) << "purge log file " << filePath;
				remove(filePath.c_str());
				continue;
			}

			logFile* file;
			dsReturnIfFailed(logFile::load(iter, filePath.c_str(), &m_memoryInfo, file));
			if (m_begin == nullptr)
			{
				m_current = m_begin = file;
				if (m_current->getMetaInfo().getFlag(META_FLAG::FINISHED))
				{
					if (m_current->getMetaInfo().getFlag(META_FLAG::FLUSHED))
						m_firstUnFlushed = m_current;
					else
						m_current->getMetaInfo().unsetFlag(META_FLAG::FINISHED);
				}
			}
			else
			{
				if (m_current->getFileId() + 1 != iter)
				{
					if (file->getRawFileId() - m_current->getRawFileId() != iter - m_current->getFileId())
						dsFailedAndLogIt(1, "rawFileId " << m_current->getRawFileId() << " of prev log file: " << m_current->getFileId() << ",and rawFileId " << file->getRawFileId() << " of next log file : " << iter << " not match", ERROR);
					uint32_t fileId = m_current->getFileId() + 1;
					uint32_t srcfileId = m_current->getRawFileId() + 1;
					uint32_t lostCount = iter - m_current->getFileId() - 1;
					LOG(WARNING) << "lost log file " << (m_current->getFileId() + 1) << " to " << (iter - 1);
					for (uint32_t i =0; i < lostCount; i++)
					{
						logFile* l = new logFile(fileId +i, srcfileId+ i, &m_memoryInfo);
						m_current->setNext(l);
						m_current = l;
					}
				}
				m_current->setNext(file);
				m_current = file;
				if (m_current->getMetaInfo().getFlag(META_FLAG::FINISHED))
				{
					if (!m_current->getMetaInfo().getFlag(META_FLAG::FLUSHED))
						m_current->getMetaInfo().unsetFlag(META_FLAG::FINISHED);
					else if (m_firstUnFlushed->getFileId() + 1 == m_current->getFileId())
						m_firstUnFlushed = m_current;
				}
			}
		}
		m_firstNotNeedPurged = m_begin;
		if (!m_current->getMetaInfo().getFlag(META_FLAG::FINISHED))
			dsReturnIfFailed(m_current->loadIndex());
		dsOk();
	}

	inline void recordSetted()
	{
		m_current->recordSetted();
	}

	inline void setPurgeTo(uint64_t seqNo)
	{
		m_purgeToSeq.seqNo = seqNo;
		while (m_firstNotNeedPurged != nullptr && m_purgeToSeq.fileId > m_firstNotNeedPurged->getFileId() && m_firstNotNeedPurged->getNext() != nullptr)
			m_firstNotNeedPurged = m_firstNotNeedPurged->getNext();
	}

	DS createNextLogFile(uint64_t srcLogFileId, logFile *& nextFile)
	{
		nextFile = nullptr;
		if (m_current != nullptr)
		{
			if (srcLogFileId != m_current->getRawFileId() + 1)
				dsFailedAndLogIt(1, "expect srcLogFileId of next file is " << m_current->getRawFileId() + 1 << ", but actully is " << srcLogFileId, ERROR);
			logFile* next = new logFile(m_current->getFileId() + 1, srcLogFileId, &m_memoryInfo);
			m_current->setNext(next);
			m_current = next;
		}
		else
		{
			m_current = new logFile(1, srcLogFileId, &m_memoryInfo);
			m_firstUnFlushed = m_firstNotNeedPurged =  m_begin = m_current;
		}
		nextFile = m_current;
		dsOk();
	}

	inline logEntry* allocNextRecord(uint32_t allocSize)
	{
		logEntry* entry;
		if ((m_rotateBySize && m_current->getSize() >= m_maxFileSize)
			|| (m_rotateByTime && timer::getNowTimestamp() - m_current->getCreateTime() >= m_maxFileTime)
			|| unlikely((entry = m_current->allocNextRecord(allocSize)) == nullptr))
		{
			logFile* next = new logFile(m_current == nullptr ? 1 : m_current->getFileId() + 1, &m_memoryInfo);
			entry = next->allocNextRecord(allocSize);
			m_current->setNext(next);
			m_current = next;
		}
		return entry;
	}

	void start()
	{
		m_memoryInfo.start();
		m_flushAndPurgeThread = std::thread([this]()-> void{
			flushAndPurgeThread();
		});
	}

	void stop()
	{
		m_memoryInfo.stop();
		m_flushAndPurgeThread.join();
	}


	class iterator
	{
	private:
		localLogFileCache* cache;
		logFile::iterator fileIter;
	public:
		iterator(localLogFileCache* cache) :cache(cache), fileIter(nullptr)
		{
		}

		DS seek(uint64_t seqNo, logEntry*& e)
		{
			logSeqNo seq;
			seq.seqNo = seqNo;
			logFile * currentLog = cache->m_begin;
			if (currentLog == nullptr)
				dsFailedAndLogIt(1, "local log file cache is empty", ERROR);
			while (currentLog != nullptr && currentLog->getFileId() < seq.fileId)
				currentLog = currentLog->getNext();
			if (currentLog == nullptr || currentLog->getFileId() != seq.fileId)
				dsFailedAndLogIt(1, "can not find seqNo:" << seqNo, ERROR);
			fileIter.setLogFile(currentLog);
			dsReturn(fileIter.seek(seqNo, e));
		}

		DS seekToBegin(logEntry*& e)
		{
			logFile* currentLog = cache->m_begin;
			if (currentLog == nullptr)
				dsFailedAndLogIt(1, "local log file cache is empty", ERROR);
			fileIter.setLogFile(currentLog);
			dsReturn(fileIter.seekToBegin(e));
		}

		inline DS next(logEntry*& e,uint32_t outTime)
		{
			DS s = fileIter.next(e, outTime);
			if (!dsCheck(s))
				dsReturnCode(s);
			if (e != nullptr)
				dsOk();
			if (s == 0)//out time
				dsOk();
			//end of file
			dsReturn(fileIter.attachToNextFile(e));
		}
	};
	};
}