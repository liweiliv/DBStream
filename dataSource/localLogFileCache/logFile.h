#pragma once
#include <stdint.h>
#include <string>
#include <atomic>
#include <vector>
#include <functional>
#include "thread/shared_mutex.h"
#include "util/status.h"
#include "util/file.h"
#include "util/likely.h"
#include "util/timer.h"
#include "util/winDll.h"
#include "util/crcBySSE.h"
#include "lz4/lib/lz4.h"
#include "thread/barrier.h"
#include "memoryInfo.h"
#include "logEntry.h"
#include "../dataSourceConf.h"
namespace DATA_SOURCE
{
	DLL_EXPORT struct logFileShardMetaInfo
	{
		uint32_t shardId;
		RPC::LogSeqNo beginSeqno;
		uint32_t count;
		uint32_t writedSize;
		uint32_t lastRecordoffset;
		uint32_t compressedSize;
		uint64_t offsetInFile;
		logFileShardMetaInfo() :shardId(0), count(0), writedSize(0), lastRecordoffset(0), compressedSize(0), offsetInFile(0)
		{
			beginSeqno.seqNo = 0;
		}
		logFileShardMetaInfo(uint32_t shardId, uint64_t beginSeqno) :shardId(shardId), count(0), writedSize(0), lastRecordoffset(0), compressedSize(0), offsetInFile(0)
		{
			this->beginSeqno.seqNo = beginSeqno;
		}
	};
	DLL_EXPORT struct logFileShard
	{
		RPC::Checkpoint* begin;
		RPC::Checkpoint* end;
		bool compress;
		bool flushed;
		shared_mutex readStatus;
		logFileShard* next;
		uint32_t volumn;
		uint16_t beginCheckpointSize;
		bool finished;
		memoryInfo* memInfo;

		logFileShardMetaInfo metaInfo;

		char* mem;
		DLL_EXPORT logFileShard(memoryInfo* memInfo, uint32_t shardId, uint64_t seqNo, uint32_t volumn = DEFAULT_SHARED_SIZE);

		DLL_EXPORT logFileShard(memoryInfo* memInfo, logFileShardMetaInfo* meta);

		DLL_EXPORT ~logFileShard();

		DLL_EXPORT DS flush(fileHandle fd, char* compressBuf, uint32_t compressBufSize);


		DLL_EXPORT static DS loadCheckpoint(fileHandle file, RPC::Checkpoint*& c);

		DLL_EXPORT DS loadData(fileHandle file);



		DLL_EXPORT static DS loadMetaInfo(memoryInfo* memInfo, logFileShard*& shard, fileHandle file);

		DLL_EXPORT static DS load(memoryInfo* memInfo, logFileShard*& shard, fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize);

		DLL_EXPORT static DS loadMetaInfoAndJumpToNext(memoryInfo* memInfo, logFileShard*& shard, fileHandle file);

		DLL_EXPORT DS load(fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize);

		inline void recordSetted()
		{
			LogEntry* entry = (LogEntry*)(mem + metaInfo.writedSize);
			wmb();
			metaInfo.count++;
			metaInfo.lastRecordoffset = metaInfo.writedSize;
			metaInfo.writedSize += entry->size;
			end = entry->getCheckpoint();
			if (begin == nullptr)
				begin = end;
		}

		inline LogEntry* allocNextRecord(uint32_t allocSize)
		{
			if (unlikely(volumn - metaInfo.writedSize < allocSize))
			{
				if (metaInfo.writedSize == 0)
				{
					mem = memInfo->realloc(mem, volumn, allocSize);
					if (mem == nullptr)
					{
						volumn = 0;
						return nullptr;
					}
					volumn = allocSize;
					return (LogEntry*)mem;
				}
				else
				{
					return nullptr;
				}
			}
			else
			{
				return (LogEntry*)(mem + metaInfo.writedSize);
			}
		}

		DLL_EXPORT void startRead();

		DLL_EXPORT void finshedRead();
	};

	enum class META_FLAG
	{
		FINISHED,
		FLUSHED,
		INDEX_FLUSHED
	};
	DLL_EXPORT class logFile
	{
	private:
		struct metaInfo
		{
			uint64_t flag;
			uint64_t dataSizeInFile;
			uint64_t rawFileId;
			uint64_t createTime;
			RPC::LogSeqNo seqNo;
			metaInfo() :flag(0), dataSizeInFile(0), rawFileId(0), createTime(0)
			{
				seqNo.seqNo = 0;
			}
			metaInfo(uint32_t fileId, uint64_t rawFileId, uint64_t createTime) :flag(0), dataSizeInFile(0), rawFileId(rawFileId), createTime(createTime)
			{
				seqNo.fileId = fileId;
				seqNo.logId = 0;
			}
			inline bool getFlag(META_FLAG flag) const
			{
				return this->flag & (0x01ULL << static_cast<uint16_t>(flag));
			}
			inline void setFlag(META_FLAG flag)
			{
				this->flag |= (0x01ULL << static_cast<uint16_t>(flag));
			}
			inline void unsetFlag(META_FLAG flag)
			{
				this->flag &= ~(0x01ULL << static_cast<uint16_t>(flag));
			}
		};
	private:
		std::mutex m_lock;
		logFileShard* m_first;
		logFileShard* m_firstNotFlushed;
		logFileShard* m_current;
		String m_filePath;
		fileHandle m_readFd;
		fileHandle m_writeFd;

		metaInfo m_metaInfo;

		uint32_t m_size;

		memoryInfo* m_memInfo;
		std::mutex m_condLock;
		std::condition_variable m_cond;
		logFile* m_next;
		shared_mutex m_isReading;

		friend class LocalLogFileCache;
	public:
		DLL_EXPORT logFile(uint32_t fileId, memoryInfo* memInfo);
		DLL_EXPORT logFile(uint32_t fileId, uint64_t rawFileid, memoryInfo* memInfo);
		DLL_EXPORT ~logFile();

		DLL_EXPORT DS init();



		bool isFlushed()
		{
			return m_metaInfo.getFlag(META_FLAG::FLUSHED);
		}
		bool isFinished()
		{
			return m_metaInfo.getFlag(META_FLAG::FLUSHED);
		}

		inline void recordSetted()
		{
			if (unlikely(m_current->next != nullptr))
			{
				LogEntry* entry = (LogEntry*)m_current->next->mem;
				entry->getCheckpoint()->seqNo.seqNo = ++m_metaInfo.seqNo.seqNo;
				m_current->next->recordSetted();
				wmb();
				m_size += entry->size;
				m_current->finished = true;
				m_current = m_current->next;
			}
			else
			{
				LogEntry* entry = (LogEntry*)(m_current->mem + m_current->metaInfo.writedSize);
				entry->getCheckpoint()->seqNo.seqNo = ++m_metaInfo.seqNo.seqNo;
				m_size += entry->size;
				m_current->recordSetted();
			}
			m_cond.notify_one();
		}

		inline LogEntry* allocNextRecord(uint32_t allocSize)
		{
			LogEntry* next;
			if (likely(!m_current->finished && (next = m_current->allocNextRecord(allocSize)) != nullptr))
				return next;
			else if (!m_memInfo->isRunning())
				return nullptr;
			logFileShard* nextShard = new logFileShard(m_memInfo, m_current->metaInfo.shardId + 1, m_metaInfo.seqNo.seqNo + 1, std::max<uint32_t>(m_memInfo->getDefaultShardSize(), allocSize));
			m_current->next = nextShard;
			return m_current->next->allocNextRecord(allocSize);
		}

		inline uint32_t getSize()
		{
			return m_size;
		}

		inline uint64_t getSeqNo()
		{
			return m_metaInfo.seqNo.seqNo;
		}

		inline uint32_t getCreateTime()
		{
			return m_metaInfo.createTime;
		}

		inline uint32_t getFileId()
		{
			return m_metaInfo.seqNo.fileId;
		}

		inline uint32_t getRawFileId()
		{
			return m_metaInfo.rawFileId;
		}

		//only called by reader thread
		 DS getLastEntryAndCheck(LogEntry*& e)
		{
			e = nullptr;
			startRead();
			if (m_current == nullptr)
			{
				dsReturnIfFailedWithOp(loadIndex(), finishRead());
				if (m_current == nullptr)
					dsOk();
			}
			m_current->startRead();
			if (m_current->next != nullptr)
			{
				delete m_current->next;
				m_current->next = nullptr;
			}
			if (m_current->metaInfo.writedSize == 0)//empty
			{
				uint32_t shardId = m_current->metaInfo.shardId;
				m_current->finshedRead();
				finishRead();
				if (m_current->metaInfo.shardId > 0)
					dsFailedAndLogIt(1, "current shard is empty but shard id " << shardId << "is not 0", ERROR);
				dsOk();
			}
			if (m_current->mem == nullptr)
			{
				if (m_current->metaInfo.offsetInFile >= 0)
				{
					dsReturnIfFailedWithOp(loadShardData(m_current), do { m_current->finshedRead(); finishRead(); } while (0));
				}
				else
				{
					m_current->finshedRead();
					finishRead();
					dsFailedAndLogIt(1, "current shard  " << m_current->metaInfo.shardId << " is not empty but shard is not in mem and file", ERROR);
				}
			}
			LogEntry* entry =  (LogEntry*)(m_current->mem + m_current->metaInfo.lastRecordoffset);
			e = (LogEntry*)malloc(sizeof(entry->size) + entry->size);
			memcpy(e, entry, sizeof(entry->size) + entry->size);
			m_current->finshedRead();
			finishRead();
			dsOk();
		}

	private:
		inline metaInfo& getMetaInfo()
		{
			return m_metaInfo;
		}
		inline void wait()
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(10);
			std::unique_lock<std::mutex> lock(m_condLock);
			m_cond.wait_until(lock, t);
		}

		inline logFile* getNext()
		{
			return m_next;
		}

		inline void setNext(logFile* next)
		{
			m_next = next;
		}

		void finish()
		{
			if (m_current != nullptr)
				m_current->finished = true;
			wmb();
			m_metaInfo.setFlag(META_FLAG::FINISHED);
		}

		void purgeTo(uint64_t seqNo);

		void clear(bool removeFile);

		DS close(const char* fileDir, char* compressBuf, uint32_t compressBufSize);

		inline DS flush(const char* fileDir, char* compressBuf, uint32_t compressBufSize);

		DS appendEndInfoToFile();

		DS flushIndex(const char* fileDir, char* compressBuf, uint32_t compressBufSize);

		static DS getBeginCheckpoint(const char* file, RPC::Checkpoint*& begin);

		DS getEndCheckpoint(RPC::Checkpoint*& end);

		DS loadShardData(logFileShard* s);

		DS generateIndexFromData();

		DS loadIndex();

		DS loadMetaInfo();

		static DS load(uint32_t fileId, const char* filePath, memoryInfo* memInfo, logFile*& file);

		void closeReadFd();

		DLL_EXPORT void startRead();
		DLL_EXPORT void finishRead();

	public:
		class iterator
		{
		private:
			logFile* file;
			logFileShard* shard;
			uint32_t offset;
			DLL_EXPORT DS attachToNextShard();
			DLL_EXPORT DS waitNextRecord(LogEntry*& e, int32_t outTim);
			inline void _next(LogEntry*& e)
			{
				e = (LogEntry*)(shard->mem + offset);
				offset += e->size;
				assert(offset <= shard->metaInfo.writedSize);
			}
		public:
			DLL_EXPORT iterator(logFile* file);
			DLL_EXPORT ~iterator();

			DLL_EXPORT void setLogFile(logFile* file);
			DLL_EXPORT DS seek(uint64_t seqNo, LogEntry*& e);

			DLL_EXPORT DS seekToBegin(LogEntry*& e);

			DLL_EXPORT DS attachToNextFile(LogEntry*& e);

			inline DS next(LogEntry*& e, int32_t outTime)
			{
				if (shard->metaInfo.writedSize == offset)
					dsReturn(waitNextRecord(e, outTime));
				_next(e);
				dsOk();
			}
		};
	};
}
