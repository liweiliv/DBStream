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
	class LocalLogFileCache
	{
	private:
		logFile* m_current;
		logFile* m_begin;
		logFile* m_firstUnFlushed;
		logFile* m_firstNotNeedPurged;

		std::string m_baseDir;

		std::map<uint64_t, logFile*> m_unfinishedFiles;

		uint32_t m_maxFileSize;
		bool m_rotateBySize;
		uint32_t m_maxFileTime;
		bool m_rotateByTime;
		uint32_t m_currentFileCreateTime;
		RPC::LogSeqNo m_purgeToSeq;

		char* m_compressBuf;
		uint32_t m_compressBufSize;

		memoryInfo m_memoryInfo;

		std::thread m_flushAndPurgeThread;
		dsStatus m_errorInfo;

		shared_mutex m_lock;

	private:
		void purge();
		DS flush(logFile* begin, bool flushAll, bool flushIndex);

		void flushAndPurgeThread();
		static int64_t getLogFileId(const char* name);
		DLL_EXPORT DS loadFiles(std::set<int64_t>& fileIds);
	public:
		DLL_EXPORT LocalLogFileCache(Config* conf);
		DLL_EXPORT ~LocalLogFileCache();


		DLL_EXPORT DS init(uint64_t purgedSeqNo = 0);

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

		DLL_EXPORT DS createNextLogFile(uint64_t srcLogFileId, logFile*& nextFile);

		DLL_EXPORT DS finishLogFile(logFile*& file);


		inline LogEntry* allocNextRecord(uint32_t allocSize)
		{
			LogEntry* entry;
			if ((m_rotateBySize && m_current->getSize() >= m_maxFileSize)
				|| (m_rotateByTime && Timer::getNowTimestamp() - m_current->getCreateTime() >= m_maxFileTime)
				|| unlikely((entry = m_current->allocNextRecord(allocSize)) == nullptr))
			{
				logFile* next = new logFile(m_current == nullptr ? 1 : m_current->getFileId() + 1, &m_memoryInfo);
				entry = next->allocNextRecord(allocSize);
				m_current->setNext(next);
				m_current = next;
			}
			return entry;
		}

		DLL_EXPORT void start();

		DLL_EXPORT void stop();

		DLL_EXPORT bool containSeqNo(uint64_t seqNo);

		DLL_EXPORT DS lastEntry(LogEntry*&e)
		{
			e = nullptr;
			if (m_current == nullptr)
				dsOk();
			dsReturn(m_current->getLastEntryAndCheck(e));
		}

		DLL_EXPORT std::list<logFile*> getUnfinishedLogFiles()
		{
			std::list<logFile*> list;
			m_lock.lock_shared();
			for (auto iter : m_unfinishedFiles)
				list.push_back(iter.second);
			m_lock.unlock_shared();
			return list;
		}

		DLL_EXPORT logFile* getCurrentLogFile()
		{
			return m_current;
		}

		class iterator
		{
		private:
			LocalLogFileCache* cache;
			logFile::iterator fileIter;
		public:
			DLL_EXPORT iterator(LocalLogFileCache* cache);

			DLL_EXPORT DS seek(uint64_t seqNo, LogEntry*& e);

			DLL_EXPORT DS seekToBegin(LogEntry*& e);

			inline DS next(LogEntry*& e, uint32_t outTime)
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
