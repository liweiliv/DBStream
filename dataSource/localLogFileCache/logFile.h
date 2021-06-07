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
#include "util/crcBySSE.h"
#include "lz4/lib/lz4.h"
#include "thread/barrier.h"
#include "memoryInfo.h"
#include "../dataSourceConf.h"
namespace DATA_SOURCE
{
	typedef union {
		struct
		{
			uint32_t logId;
			uint32_t fileId;
		};
		uint64_t seqNo;
	} logSeqNo;
	struct checkpoint
	{
		uint64_t timestamp;
		uint64_t srcPosition;
		logSeqNo seqNo;
		uint16_t externSize;
		char externInfo[1];//like mysql gtid , oracle logminer safe checkpoint, oracle xstream position
		uint16_t checkpointSize()
		{
			return sizeof(checkpoint) - 1 + externSize;
		}
		checkpoint* clone()
		{
			checkpoint* c = (checkpoint*)malloc(checkpointSize());
			memcpy(c, this, checkpointSize());
			return c;
		}
	};
	struct logEntry
	{
		uint32_t size;
		char data[1];
		inline checkpoint* getCheckpoint()
		{
			return (checkpoint*)&data[0];
		}
		inline const char* getRealData()
		{
			return &data[0] + getCheckpoint()->checkpointSize();
		}
	};

	struct logFileShardMetaInfo
	{
		uint32_t shardId;
		logSeqNo beginSeqno;
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
	struct logFileShard
	{
		checkpoint* begin;
		checkpoint* end;
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
		logFileShard(memoryInfo* memInfo, uint32_t shardId, uint64_t seqNo, uint32_t volumn = DEFAULT_SHARED_SIZE) :begin(nullptr), end(nullptr), compress(true), flushed(false), next(nullptr),
			volumn(volumn), beginCheckpointSize(0), finished(false), memInfo(memInfo),metaInfo(shardId, seqNo), mem(nullptr)
		{
			memInfo->addMemSize(sizeof(logFileShard));
			if (volumn > 0)
				mem = memInfo->alloc(volumn);
		}

		logFileShard(memoryInfo* memInfo, logFileShardMetaInfo* meta) :begin(nullptr), end(nullptr), compress(true), flushed(true),next(nullptr),
			 volumn(0), beginCheckpointSize(0), finished(true), memInfo(memInfo), mem(nullptr)
		{
			memInfo->addMemSize(sizeof(logFileShard));
			memcpy(&metaInfo, meta, sizeof(metaInfo));
		}

		~logFileShard()
		{
			memInfo->subMemSize(sizeof(logFileShard));
			if (mem != nullptr)
				memInfo->freeMem(mem, volumn);
		}

		DS flush(fileHandle fd, char* compressBuf, uint32_t compressBufSize)
		{
			if (!finished)
				dsFailedAndLogIt(1, "can not flush sharded before it finished", ERROR);
			if (metaInfo.writedSize == 0 || metaInfo.offsetInFile > 0)
				dsOk();
			int32_t filePos = seekFile(fd, 0, SEEK_CUR);
			if (filePos < 0)
				dsFailedAndLogIt(1, "get current file position failed for: " << errno << "," << strerror(errno), ERROR);
			char* writeDataBuf;
			uint32_t writeDataSize;
			if (compress)
			{
				uint32_t compressBufNeededSize = LZ4_COMPRESSBOUND(metaInfo.writedSize);
				if (compressBufNeededSize > compressBufSize)
					writeDataBuf = (char*)malloc(compressBufNeededSize);
				else
					writeDataBuf = compressBuf;
				writeDataSize = metaInfo.compressedSize = LZ4_compress_default(mem, writeDataBuf, metaInfo.writedSize, compressBufNeededSize);
			}
			else
			{
				writeDataBuf = mem;
				writeDataSize = metaInfo.writedSize;
			}
			beginCheckpointSize = begin->checkpointSize();
			if (sizeof(metaInfo) != writeFile(fd, (char*)&metaInfo, sizeof(metaInfo))
				|| beginCheckpointSize != writeFile(fd, (char*)begin, beginCheckpointSize)
				|| writeDataSize != writeFile(fd, writeDataBuf, writeDataSize))
			{
				if (compress && writeDataBuf != compressBuf)
					free(writeDataBuf);
				dsFailedAndLogIt(1, "write log entry to local log file failed for " << errno << "," << strerror(errno), ERROR);
			}
			metaInfo.offsetInFile = (uint32_t)filePos;
			flushed = true;
			if (!readStatus.try_lock())
				dsOk();
			char* _mem = mem;
			mem = nullptr;
			begin = end = nullptr;
			readStatus.unlock();
			memInfo->freeMem(_mem, volumn);
			dsOk();
		}


		static DS loadCheckpoint(fileHandle file, checkpoint*& c)
		{
			c = nullptr;
			logFileShardMetaInfo meta;
			if (sizeof(meta) != readFile(file, (char*)&meta, sizeof(meta)))
				dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
			if (meta.writedSize == 0)
				dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for shard is empty", ERROR);
			checkpoint _c;
			if (offsetof(checkpoint, externInfo) != readFile(file, (char*)&c, offsetof(checkpoint, externInfo)))
				dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
			if (_c.externSize == 0)
			{
				c = _c.clone();
				dsOk();
			}
			else
			{
				c = (checkpoint*)malloc(sizeof(checkpoint) + c->externSize - 1);
				if (_c.externSize != readFile(file, c->externInfo, _c.externSize))
				{
					free(c);
					c = nullptr;
					dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
				}
				memcpy(c, &_c, offsetof(checkpoint, externInfo));
				dsOk();
			}
		}

		DS loadData(fileHandle file)
		{
			char* _mem = memInfo->alloc(metaInfo.writedSize);
			if (metaInfo.compressedSize > 0)
			{
				char* _deCompressBuf = (char*)malloc(metaInfo.compressedSize);
				if (metaInfo.compressedSize != readFile(file, _deCompressBuf, metaInfo.compressedSize))
				{
					free(_deCompressBuf);
					memInfo->freeMem(_mem, metaInfo.writedSize);
					dsFailedAndLogIt(1, "read log entry from local log file failed for " << errno << "," << strerror(errno), ERROR);
				}
				if (LZ4_decompress_safe(_deCompressBuf, _mem, metaInfo.compressedSize, metaInfo.writedSize) < 0)
				{
					free(_deCompressBuf);
					memInfo->freeMem(_mem, metaInfo.writedSize);
					dsFailedAndLogIt(1, "decompress read log entry from local log file failed", ERROR);
				}
				free(_deCompressBuf);
				compress = true;
			}
			else
			{
				if (metaInfo.writedSize != readFile(file, _mem, metaInfo.writedSize))
				{
					memInfo->freeMem(_mem, metaInfo.writedSize);
					dsFailedAndLogIt(1, "read log entry from local log file failed for " << errno << "," << strerror(errno), ERROR);
				}
			}
			begin = (checkpoint*)_mem;
			end = (checkpoint*)(_mem + metaInfo.lastRecordoffset);
			volumn = metaInfo.writedSize;
			flushed = true;
			mem = _mem;
			dsOk();
		}



		static DS loadMetaInfo(memoryInfo* memInfo, logFileShard*& shard, fileHandle file)
		{
			shard = nullptr;
			logFileShardMetaInfo meta;
			if (sizeof(meta) != readFile(file, (char*)&meta, sizeof(meta)))
				dsFailedAndLogIt(1, "read shard meta info from local log file failed for " << errno << "," << strerror(errno), ERROR);
			if (meta.writedSize == 0)
				dsFailedAndLogIt(1, "read local log file failed for shard is empty", ERROR);
			if (meta.shardId == UINT32_MAX)//end of shards
				dsOk();
			checkpoint c;
			if (sizeof(c) - 1 != readFile(file, (char*)&c, sizeof(c) - 1))
				dsFailedAndLogIt(1, "read shard checkpoint info from local log file failed for " << errno << "," << strerror(errno), ERROR);
			if (c.externSize > 0)
			{
				if (seekFile(file, c.externSize, SEEK_CUR) < 0)
					dsFailedAndLogIt(1, "read shard checkpoint info from local log file failed for " << errno << "," << strerror(errno), ERROR);
			}
			shard = new logFileShard(memInfo, &meta);
			dsOk();
		}

		static DS load(memoryInfo* memInfo, logFileShard*& shard, fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize)
		{
			dsReturnIfFailed(loadMetaInfo(memInfo, shard, file));
			if (shard == nullptr)// end of shards
				dsOk();
			if (!dsCheck(shard->loadData(file)))
			{
				delete shard;
				shard = nullptr;
				dsReturn(getLocalStatus().code);
			}
			dsOk();
		}

		static DS loadMetaInfoAndJumpToNext(memoryInfo* memInfo, logFileShard*& shard, fileHandle file)
		{
			dsReturnIfFailed(loadMetaInfo(memInfo, shard, file));
			if (shard == nullptr)// end of shards
				dsOk();
			int64_t offset = shard->metaInfo.compressedSize > 0 ? shard->metaInfo.compressedSize : shard->metaInfo.writedSize;
			if (seekFile(file, offset, SEEK_CUR) < 0)
			{
				delete shard;
				shard = nullptr;
				dsFailedAndLogIt(1, "seek to logFileShard offset" << offset << " in file failed for " << errno << "," << strerror(errno), ERROR);
			}
			dsOk();
		}

		DS load(fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize)
		{
			mem = (char*)malloc(metaInfo.writedSize);
			uint32_t dataOffset = metaInfo.offsetInFile + beginCheckpointSize + sizeof(logFileShardMetaInfo);
			if (dataOffset != seekFile(file, dataOffset, SEEK_SET))
				dsFailedAndLogIt(1, "seek to logFileShard offset" << dataOffset << " in file failed for " << errno << "," << strerror(errno), ERROR);
			dsReturn(loadData(file));
		}

		inline void recordSetted()
		{
			logEntry* entry = (logEntry*)(mem + metaInfo.writedSize);
			wmb();
			metaInfo.count++;
			metaInfo.lastRecordoffset = metaInfo.writedSize;
			metaInfo.writedSize += entry->size;
			end = entry->getCheckpoint();
			if (begin == nullptr)
				begin = end;
		}

		inline logEntry* allocNextRecord(uint32_t allocSize)
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
					return (logEntry*)mem;
				}
				else
				{
					return nullptr;
				}
			}
			else
			{
				return (logEntry*)(mem + metaInfo.writedSize);
			}
		}

		void startRead()
		{
			readStatus.lock_shared();
		}

		void finshedRead()
		{
			readStatus.unlock_shared();
			if (metaInfo.offsetInFile > 0)//flushed
			{
				if (readStatus.try_lock())
				{
					if (mem != nullptr)
					{
						char* _mem = mem;
						mem = nullptr;
						begin = end = nullptr;
						readStatus.unlock();
						memInfo->freeMem(_mem, volumn);
					}
					else
						readStatus.unlock();
				}
			}
		}
	};

	enum class META_FLAG
	{
		FINISHED,
		FLUSHED,
		INDEX_FLUSHED
	};
	class logFile
	{
	private:
		struct metaInfo
		{
			uint64_t flag;
			uint64_t dataSizeInFile;
			uint64_t rawFileId;
			uint64_t createTime;
			logSeqNo seqNo;
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
		//logFileShard* m_firstNotNeedPurged;
		logFileShard* m_firstNotFlushed;
		logFileShard* m_current;
		String m_filePath;
		fileHandle m_readFd;
		fileHandle m_writeFd;

		metaInfo m_metaInfo;

		//bool m_finished;
		//bool m_flushed;
		//bool m_indexFlushed;
		uint32_t m_size;

		memoryInfo* m_memInfo;
		std::mutex m_condLock;
		std::condition_variable m_cond;
		logFile* m_next;
		shared_mutex m_isReading;
	public:
		logFile(uint32_t fileId, memoryInfo* memInfo) :m_first(nullptr), m_firstNotFlushed(nullptr),
			m_current(nullptr), m_readFd(INVALID_HANDLE_VALUE), m_writeFd(INVALID_HANDLE_VALUE), m_metaInfo(fileId, 0, 0),
			m_size(0), m_memInfo(memInfo), m_next(nullptr)
		{
			m_memInfo->addMemSize(sizeof(logFile));
		}
		logFile(uint32_t fileId, uint64_t rawFileid, memoryInfo* memInfo) :m_first(new logFileShard(memInfo, 0, 0)), m_firstNotFlushed(m_first),
			m_current(m_first), m_readFd(INVALID_HANDLE_VALUE), m_writeFd(INVALID_HANDLE_VALUE), m_metaInfo(fileId, rawFileid, timer::getNowTimestamp()),
			m_size(0), m_memInfo(memInfo), m_next(nullptr)
		{
			m_memInfo->addMemSize(sizeof(logFile));
			m_first->metaInfo.beginSeqno.seqNo = m_metaInfo.seqNo.seqNo;
		}

		~logFile()
		{
			clear(false);
			m_memInfo->subMemSize(sizeof(logFile));
		}

		DS init()
		{
			m_current = new logFileShard(m_memInfo, m_metaInfo.seqNo.seqNo, m_memInfo->getDefaultShardSize());
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
			logEntry* entry = (logEntry*)(m_current->mem + m_current->metaInfo.writedSize);
			entry->getCheckpoint()->seqNo.seqNo = ++m_metaInfo.seqNo.seqNo;
			m_size += entry->size;
			m_current->recordSetted();
			m_cond.notify_one();
		}

		inline logEntry* allocNextRecord(uint32_t allocSize)
		{
			logEntry* next;
			if (likely(!m_current->finished && (next = m_current->allocNextRecord(allocSize)  )!= nullptr))
				return next;
			else if (!m_memInfo->isRunning())
				return nullptr;
			logFileShard* nextShard = new logFileShard(m_memInfo, m_current->metaInfo.shardId + 1, m_metaInfo.seqNo.seqNo + 1, std::max<uint32_t>(m_memInfo->getDefaultShardSize(), allocSize));
			m_current->next = nextShard;
			wmb();
			m_current->finished = true;
			m_current = nextShard;
			return m_current->allocNextRecord(allocSize);
		}

		inline metaInfo& getMetaInfo()
		{
			return m_metaInfo;
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

		inline void purgeTo(uint64_t seqNo)
		{
			if (m_first == nullptr)
				return;
			uint32_t unflushedShardId = m_firstNotFlushed == nullptr ? 0 : m_firstNotFlushed->metaInfo.shardId;
			while (m_first != nullptr)
			{
				if (m_first->next == nullptr)
				{
					if (!m_metaInfo.getFlag(META_FLAG::FINISHED))
						break;
					if (m_first->readStatus.try_lock())
					{
						if (m_first->next == nullptr)
						{
							delete m_first;
							m_first = nullptr;
							m_firstNotFlushed = nullptr;
							m_current = nullptr;
							return;
						}
					}

				}
				if (m_first->next->metaInfo.beginSeqno.seqNo > seqNo)
					break;
				logFileShard* next = m_first->next;
				delete m_first;
				m_first = next;
			}
			if (m_first->metaInfo.shardId >= unflushedShardId)
				m_firstNotFlushed = m_first;
		}

		inline void clear(bool removeFile)
		{
			while (m_first != nullptr)
			{
				logFileShard* s = m_first->next;
				delete m_first;
				m_first = s;
			}
			if (m_filePath.empty())
				return;
			if (fileHandleValid(m_readFd))
			{
				closeFile(m_readFd);
				m_readFd = INVALID_HANDLE_VALUE;
			}
			if (fileHandleValid(m_writeFd))
			{
				closeFile(m_writeFd);
				m_writeFd = INVALID_HANDLE_VALUE;
			}
			if (removeFile)
			{
				if (0 != remove(m_filePath.c_str()))
					LOG(WARNING) << "remove  local log file " << m_filePath << " failed, error info: " << errno << "," << strerror(errno);
			}
		}

		DS close(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
		{
			if (m_current != nullptr)
			{
				if (!m_current->finished)
					m_current->finished = true;
			}
			dsReturnIfFailed(flush(fileDir, compressBuf, compressBufSize));
			if (m_metaInfo.getFlag(META_FLAG::FINISHED))
				dsReturnIfFailed(flushIndex(fileDir, compressBuf, compressBufSize));
		}

		inline DS flush(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
		{
			if (m_metaInfo.getFlag(META_FLAG::FLUSHED))
				dsOk();
			logFileShard* s = m_firstNotFlushed;
			if (s == nullptr || !s->finished)
				dsOk();
			if (m_filePath.empty())
			{
				m_filePath.assign(fileDir);
				m_filePath.append("/").append(getFileId()).append(".log");
			}
			if (!fileHandleValid(m_writeFd))
			{
				m_writeFd = openFile(m_filePath.c_str(), true, true, true);
				if (!fileHandleValid(m_writeFd))
					dsFailedAndLogIt(1, "open local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
				if (getFileSize(m_writeFd) != 0)
				{
					if (seekFile(m_writeFd, 0, SEEK_END) < 0)
						dsFailedAndLogIt(1, "seek to end of  local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
				}
				else
				{
					if (sizeof(m_metaInfo) != writeFile(m_writeFd, (char*)&m_metaInfo, sizeof(m_metaInfo)))
						dsFailedAndLogIt(1, "write local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
				}
			}
			while (s != nullptr && s->finished)
			{
				dsReturnIfFailed(s->flush(m_writeFd, compressBuf, compressBufSize));
				s = s->next;
			}
			m_metaInfo.dataSizeInFile = getFileSize(m_writeFd);
			m_firstNotFlushed = s;
			if (m_firstNotFlushed == nullptr)//all shard flushed
			{
				if (m_metaInfo.getFlag(META_FLAG::FINISHED))//if finished ,flushed is true 
					m_metaInfo.setFlag(META_FLAG::FLUSHED);
				closeFile(m_writeFd);
				m_writeFd = INVALID_HANDLE_VALUE;
			}
			dsOk();
		}

		DS appendEndInfoToFile()
		{
			logFileShardMetaInfo meta;
			meta.shardId = UINT32_MAX;//means logfile finished
			if (sizeof(meta) != writeFile(m_writeFd, (char*)&meta, sizeof(meta)))
				dsFailedAndLogIt(1, "write tail info to local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			dsOk();
		}

		DS flushIndex(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
		{
			if (!m_metaInfo.getFlag(META_FLAG::FINISHED))
				dsFailedAndLogIt(1, "can not flush index of log file before log " << getFileId() << " finished", ERROR);
			if (!m_metaInfo.getFlag(META_FLAG::FLUSHED))
				dsReturnIfFailed(flush(fileDir, compressBuf, compressBufSize));
			if (m_metaInfo.getFlag(META_FLAG::INDEX_FLUSHED))
				dsOk();
			if (m_first == nullptr)//todo
			{
				m_metaInfo.setFlag(META_FLAG::FLUSHED);
				dsOk();
			}
			if (m_filePath.empty())
			{
				m_filePath.assign(fileDir);
				m_filePath.append("/").append(getFileId()).append(".log");
			}
			if (!fileHandleValid(m_writeFd))
			{
				m_writeFd = openFile(m_filePath.c_str(), true, true, true);
				if (!fileHandleValid(m_writeFd))
					dsFailedAndLogIt(1, "open local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			else
			{
				if (0 != seekFile(m_writeFd, 0, SEEK_SET))
					dsFailedAndLogIt(1, "seek to begin of local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}

			int64_t fileSize = 0;
			if ((fileSize = getFileSize(m_writeFd)) < 0)
				dsFailedAndLogIt(1, "get file size of local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);

			m_metaInfo.dataSizeInFile = fileSize == 0 ? sizeof(m_metaInfo) : fileSize;
			if (sizeof(m_metaInfo) != writeFile(m_writeFd, (char*)&m_metaInfo, sizeof(m_metaInfo)))
				dsFailedAndLogIt(1, "write local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);

			if (seekFile(m_writeFd, 0, SEEK_END) < 0)
				dsFailedAndLogIt(1, "seek to end of local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			dsReturnIfFailed(appendEndInfoToFile());
			if (m_first == nullptr)
				dsOk();

			logFileShard* s = m_first;
			uint32_t shardCount = m_current->metaInfo.shardId - m_first->metaInfo.shardId + 1;
			char defaultBuf[10240];
			uint32_t indexSize = shardCount * sizeof(logFileShardMetaInfo) + sizeof(uint32_t);
			char* buf = indexSize > sizeof(defaultBuf) ? new char[indexSize] : defaultBuf;
			uint32_t idx = 0;
			while (s != nullptr)
			{
				memcpy(buf + sizeof(uint32_t) + idx * sizeof(logFileShardMetaInfo), &s->metaInfo, sizeof(s->metaInfo));
				idx++;
				s = s->next;
			}
			*(uint32_t*)buf = hwCrc32c(0, buf + sizeof(uint32_t), indexSize - sizeof(uint32_t));
			if (indexSize != writeFile(m_writeFd, buf, indexSize))
			{
				if (buf != defaultBuf)
					delete[]buf;
				dsFailedAndLogIt(1, "write index info to local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			closeFile(m_writeFd);
			m_writeFd = INVALID_HANDLE_VALUE;
			if (buf != defaultBuf)
				delete[] buf;
			m_metaInfo.setFlag(META_FLAG::INDEX_FLUSHED);
			if (m_isReading.try_lock())
			{
				logFileShard* shard = m_first;
				m_first = nullptr;
				m_current = nullptr;
				m_firstNotFlushed = nullptr;
				m_isReading.unlock();
				while (shard != nullptr)
				{
					logFileShard* tmp = shard->next;
					delete shard;
					shard = tmp;
				}
			}
			dsOk();
		}

		static DS getBeginCheckpoint(const char* file, checkpoint*& begin)
		{
			begin = nullptr;
			fileHandle fd = openFile(file, true, false, false);
			if (!fileHandleValid(fd))
				dsFailedAndLogIt(1, "open local log file " << file << " failed for " << errno << "," << strerror(errno), ERROR);
			int64_t fileSize = getFileSize(fd);
			if (fileSize < sizeof(metaInfo))
			{
				closeFile(fd);
				dsFailedAndLogIt(1, "local log file " << file << "corrupt", ERROR);
			}
			if (fileSize == sizeof(metaInfo))
			{
				closeFile(fd);
				dsOk();
			}
			if (fileSize < sizeof(metaInfo) + sizeof(checkpoint) - 1)
			{
				closeFile(fd);
				dsFailedAndLogIt(1, "local log file " << file << "corrupt", ERROR);
			}
			metaInfo  m;
			if (sizeof(m) != readFile(fd, (char*)&m, sizeof(m)))
			{
				closeFile(fd);
				dsFailedAndLogIt(1, "read local log file " << file << " failed for " << errno << "," << strerror(errno), ERROR);
			}
			dsReturnWithOp(logFileShard::loadCheckpoint(fd, begin), closeFile(fd));
		}

		DS getEndCheckpoint(checkpoint*& end)
		{
			if (m_metaInfo.getFlag(META_FLAG::FINISHED))
			{
				if (m_current == nullptr)
				{
					if (m_metaInfo.seqNo.logId > 0)
					{
						dsReturnIfFailed(loadMetaInfo());
					}
					else
					{
						end = nullptr;
						dsOk();
					}
				}
				if (m_current->mem == nullptr)
				{

				}
			}
		}

		DS loadShardData(logFileShard* s)
		{
			std::lock_guard<std::mutex> guard(m_lock);
			if (!fileHandleValid(m_readFd))
			{
				if (!fileHandleValid(m_readFd))
				{
					m_readFd = openFile(m_filePath.c_str(), true, true, false);
					if (!fileHandleValid(m_readFd))
						dsFailedAndLogIt(1, "open local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
				}
			}
			dsReturn(s->loadData(m_readFd));
		}

		DS generateIndexFromData()
		{
			int64_t fileSize = getFileSize(m_readFd);
			if (fileSize < 0)
				dsFailedAndLogIt(1, "get file size of " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			if (fileSize < sizeof(m_metaInfo))
				dsFailedAndLogIt(1, "file size of " << m_filePath << "is less than size " << m_metaInfo.dataSizeInFile << " in meta info ", ERROR);

			if (sizeof(m_metaInfo) != seekFile(m_readFd, sizeof(m_metaInfo), SEEK_SET))
				dsFailedAndLogIt(1, "seek local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			logFileShard* first = nullptr;
			logFileShard* last = nullptr;
			logFileShard* s;
			int64_t offsetOfFile = sizeof(m_metaInfo);
			while (true)
			{
				if (fileSize - offsetOfFile < sizeof(logFileShardMetaInfo))//end of file
				{
					if(fileSize != offsetOfFile) // corrupt file
						truncateFile(m_readFd, offsetOfFile);
					break;
				}
				if (!dsCheck(logFileShard::loadMetaInfoAndJumpToNext(m_memInfo, s, m_readFd)))
				{
					while (first != nullptr)
					{
						logFileShard* tmp = first->next;
						delete first;
						first = tmp;
					}
					dsReturn(getLocalStatus().code);
				}
				if (s == nullptr)//end of file
					break;

				if (last != nullptr)
				{
					if (last->metaInfo.shardId + 1 == s->metaInfo.shardId)
					{
						last->next = s;
						last = s;
					}
					else
					{
						uint32_t lastId = last->metaInfo.shardId;
						while (first != nullptr)
						{
							logFileShard* tmp = first->next;
							delete first;
							first = tmp;
						}
						if (lastId + 1 < s->metaInfo.shardId)
						{
							first = last = s;
						}
						else
						{
							delete s;
							dsFailedAndLogIt(1, "illegal shard id in " << m_filePath, ERROR);
						}
					}
				}
				if (first == nullptr)
					first = last = s;
				if (0 > (offsetOfFile = seekFile(m_readFd, 0, SEEK_CUR)))
				{
					while (first != nullptr)
					{
						logFileShard* tmp = first->next;
						delete first;
						first = tmp;
					}
					dsFailedAndLogIt(1, "seek local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
				}
			}
			wmb();
			m_first = first;
			m_current = last;
			m_firstNotFlushed = nullptr;
			dsOk();
		}

		DS loadIndex()
		{
			if (m_first != nullptr)//index exist;
				dsOk();
			std::lock_guard<std::mutex> gurad(m_lock);
			if (!fileHandleValid(m_readFd))
			{
				m_readFd = openFile(m_filePath.c_str(), true, true, false);
				if (!fileHandleValid(m_readFd))
					dsFailedAndLogIt(1, "open local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			int64_t fileSize = getFileSize(m_readFd);
			if (fileSize < m_metaInfo.dataSizeInFile)
				dsFailedAndLogIt(1, "file size of " << m_filePath << "is less than size " << m_metaInfo.dataSizeInFile << " in meta info ", ERROR);
			if (m_metaInfo.dataSizeInFile == 0)
				dsReturn(generateIndexFromData());

			if (fileSize < m_metaInfo.dataSizeInFile + sizeof(logFileShardMetaInfo))// corrupt file
			{
				LOG(WARNING) << "file " << m_filePath << " is corrupt,truncate size to " << m_metaInfo.dataSizeInFile;
				truncateFile(m_readFd, m_metaInfo.dataSizeInFile);
				dsReturn(generateIndexFromData());
			}
			if (m_metaInfo.dataSizeInFile != seekFile(m_readFd, m_metaInfo.dataSizeInFile, SEEK_SET))
				dsFailedAndLogIt(1, "seek local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);

			logFileShardMetaInfo shardMeta;
			if (sizeof(shardMeta) != readFile(m_readFd, (char*)&shardMeta, sizeof(shardMeta)))
				dsFailedAndLogIt(1, "read local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);

			if (shardMeta.shardId != UINT32_MAX)
				dsReturn(generateIndexFromData());

			char defaultBuf[10240];
			uint32_t indexSize = fileSize - m_metaInfo.dataSizeInFile + sizeof(logFileShardMetaInfo);
			if (indexSize <= sizeof(uint32_t) || (indexSize - sizeof(uint32_t)) % sizeof(logFileShardMetaInfo) != 0)
				dsReturn(generateIndexFromData());
			char* buf = indexSize > sizeof(defaultBuf) ? new char[indexSize] : defaultBuf;
			if (indexSize != readFile(m_readFd, buf, indexSize))
			{
				if (buf != defaultBuf)
					delete[] buf;
				dsFailedAndLogIt(1, "read local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			}
			if (*(uint32_t*)buf != hwCrc32c(0, buf + sizeof(uint32_t), indexSize - sizeof(uint32_t)))
			{
				if (buf != defaultBuf)
					delete[] buf;
				dsReturn(generateIndexFromData());
			}
			uint32_t offset = sizeof(uint32_t);
			logFileShard* first = nullptr;
			logFileShard* last = nullptr;
			while (offset < indexSize)
			{
				logFileShard* s = new logFileShard(m_memInfo, (logFileShardMetaInfo*)(buf + offset));
				offset += sizeof(logFileShardMetaInfo);
				if (last != nullptr)
				{
					if (last->metaInfo.shardId + 1 == s->metaInfo.shardId)
					{
						last->next = s;
						last = s;
					}
					else
					{
						uint32_t lastId = last->metaInfo.shardId;
						while (first != nullptr)
						{
							logFileShard* tmp = first->next;
							delete first;
							first = tmp;
						}
						if (lastId + 1 < s->metaInfo.shardId)
						{
							first = last = s;
						}
						else
						{
							if (buf != defaultBuf)
								delete[] buf;
							delete s;
							dsFailedAndLogIt(1, "illegal shard id in " << m_filePath, ERROR);
						}
					}
				}
				if (first == nullptr)
					first = last = s;
			}
			wmb();
			m_first = first;
			m_current = last;
			dsOk();
		}



		DS loadMetaInfo()
		{
			if (!fileHandleValid(m_readFd))
			{
				m_readFd = openFile(m_filePath.c_str(), true, true, false);
				if (!fileHandleValid(m_readFd))
					dsFailedAndLogIt(1, "open local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			else
			{
				if (0 != seekFile(m_readFd, 0, SEEK_SET))
					dsFailedAndLogIt(1, "seek local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			}
			if (sizeof(m_metaInfo) != readFile(m_readFd, (char*)&m_metaInfo, sizeof(m_metaInfo)))
				dsFailedAndLogIt(1, "read local log file " << m_filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			dsOk();
		}

		static DS load(uint32_t fileId, const char* filePath, memoryInfo* memInfo, logFile*& file)
		{
			file = nullptr;
			logFile* _file = new logFile(fileId, memInfo);
			_file->m_filePath = filePath;
			dsReturnIfFailedWithOp(_file->loadMetaInfo(), delete _file);
			file = _file;
			dsOk();
		}

		void closeReadFd()
		{
			if (!fileHandleValid(m_readFd))
			{
				closeFile(m_readFd);
				m_readFd = INVALID_HANDLE_VALUE;
			}
		}

		void startRead()
		{
			m_isReading.lock_shared();
		}

		void finishRead()
		{
			m_isReading.unlock_shared();
			if (m_isReading.try_lock())
			{
				closeReadFd();
				if (m_metaInfo.getFlag(META_FLAG::FLUSHED))
				{
					logFileShard* shard = m_first;
					if (shard != nullptr)
					{
						m_first = nullptr;
						m_current = nullptr;
						m_firstNotFlushed = nullptr;
						m_isReading.unlock();
						while (shard != nullptr)
						{
							logFileShard* next = shard->next;
							delete shard;
							shard = next;
						}
					}
					else
						m_isReading.unlock();
				}
			}
		}

	public:
		class iterator
		{
		private:
			logFile* file;
			logFileShard* shard;
			uint32_t offset;
			DS attachToNextShard()
			{
				logFileShard* next = shard->next;
				next->startRead();
				shard->finshedRead();
				shard = next;
				offset = 0;
				if (shard->mem == nullptr)
					dsReturnIfFailed(file->loadShardData(shard));
				dsOk();
			}
		public:
			iterator(logFile* file) :file(file), shard(nullptr), offset(0)
			{
				if (file != nullptr)
					file->startRead();
			}
			~iterator()
			{
				if (shard != nullptr)
				{
					shard->finshedRead();
				}
				if (file != nullptr)
				{
					file->finishRead();
				}
			}

			void setLogFile(logFile* file)
			{
				this->file = file;
				file->startRead();
			}

			DS seek(uint64_t seqNo, logEntry*& e)
			{
				if (shard != nullptr)
				{
					shard->finshedRead();
					shard = nullptr;
				}

				e = nullptr;
				logSeqNo seq;
				seq.seqNo = seqNo;
				if (seq.fileId < file->getFileId())
					dsReturnCode(1);
				else if (seq.fileId > file->getFileId())
					dsReturnCode(2);
				uint64_t maxSeqNoInFile = file->m_metaInfo.seqNo.seqNo;
				if (maxSeqNoInFile < seqNo)
					dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " is large than max seqNo" << maxSeqNoInFile << " in file " << file->getFileId(), ERROR);

				logFileShard* s = file->m_first;
				if (s == nullptr)
					dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " in file " << file->getFileId() << " failed for file is empty ", ERROR);
				if (s->metaInfo.beginSeqno.seqNo > seqNo)
					dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " in file " << file->getFileId() << " failed for shard has been purged", ERROR);
				while (s != nullptr)
				{
					if (s->metaInfo.beginSeqno.seqNo + s->metaInfo.count > seqNo)
						break;
					s = s->next;
				}
				if (s == nullptr)
					dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " is large than max seqNo" << maxSeqNoInFile << " in file " << file->getFileId(), ERROR);
				s->startRead();
				if (s->mem == nullptr)
					dsReturnIfFailedWithOp(file->loadShardData(s), s->finshedRead());
				uint32_t soffset = 0;
				while (shard->metaInfo.writedSize > soffset)
				{
					if (((logEntry*)(s->mem + soffset))->getCheckpoint()->seqNo.seqNo == seqNo)
					{
						e = (logEntry*)(s->mem + soffset);
						offset = soffset;
						shard = s;
						dsOk();
					}
				}
				s->finshedRead();
				dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " in file " << file->getFileId() << " failed for shard has been purged", ERROR);
			}

			DS seekToBegin(logEntry*& e)
			{
				if (shard != nullptr)
				{
					shard->finshedRead();
					shard = nullptr;
				}

				logFileShard* s = file->m_first;
				if (s == nullptr)
					dsFailedAndLogIt(1, "seek to begin of file " << file->getFileId() << " failed for file is empty ", ERROR);
				s->startRead();
				if (s->mem == nullptr)
					dsReturnIfFailedWithOp(file->loadShardData(s), s->finshedRead());
				shard = s;
				if (s->metaInfo.writedSize == 0)
				{
					e = nullptr;
					offset = 0;
				}
				else
				{
					e = (logEntry*)(s->mem);
					offset = e->size;
				}
				dsOk();
			}

			DS attachToNextFile(logEntry*& e)
			{
				logFile* next = file->getNext();
				if (next != nullptr)
				{
					next->startRead();
					if (next->m_first == nullptr)
					{
						if (next->getMetaInfo().getFlag(META_FLAG::FINISHED))
						{
							dsReturnIfFailedWithOp(next->loadIndex(), next->finishRead());
						}
						else if (next->m_first == nullptr)
						{
							next->finishRead();
							dsFailedAndLogIt(1, "attatch to next file of " << file->getFileId() << " failed for next is empty", ERROR);
						}
					}
					file->finishRead();
					file = next;
					dsReturn(seekToBegin(e));
				}
				else
				{
					dsFailedAndLogIt(1, "attatch to next file of " << file->getFileId() << " failed for next is null", ERROR);
				}
			}

			inline DS next(logEntry*& e, int32_t outTime)
			{
				while (shard->metaInfo.writedSize == offset)
				{
					if (shard->finished)
					{
						if (shard->metaInfo.writedSize == offset)
						{
							logFileShard* nextShard = shard->next;
							if (nextShard == nullptr)
							{
								if (file->getMetaInfo().getFlag(META_FLAG::FINISHED))
								{
									rmb();
									if ((nextShard = shard->next) == nullptr)
									{
										shard->finshedRead();
										shard = nullptr;
										e = nullptr;
										dsReturnCode(1);
									}
								}
								else
								{
									if (outTime <= 0)
									{
										e = nullptr;
										dsOk();
									}
									file->wait();
									outTime -= 10;
									continue;
								}
							}
							dsReturnIfFailed(attachToNextShard());
							continue;
						}
					}
					else
					{
						if (outTime <= 0)
						{
							e = nullptr;
							dsOk();
						}
						file->wait();
						outTime -= 10;
					}
				}
				e = (logEntry*)(shard->mem + offset);
				offset += e->size;
				assert(offset <= shard->metaInfo.writedSize);
				dsOk();
			}
		};
	};
}
