#include <glog/logging.h>
#include "logFile.h"
namespace DATA_SOURCE
{
	DLL_EXPORT logFileShard::logFileShard(memoryInfo* memInfo, uint32_t shardId, uint64_t seqNo, uint32_t volumn) :begin(nullptr), end(nullptr), compress(true), flushed(false), next(nullptr),
		volumn(volumn), beginCheckpointSize(0), finished(false), memInfo(memInfo), metaInfo(shardId, seqNo), mem(nullptr)
	{
		memInfo->addMemSize(sizeof(logFileShard));
		if (volumn > 0)
			mem = memInfo->alloc(volumn);
	}

	DLL_EXPORT logFileShard::logFileShard(memoryInfo* memInfo, logFileShardMetaInfo* meta) :begin(nullptr), end(nullptr), compress(true), flushed(true), next(nullptr),
		volumn(0), beginCheckpointSize(0), finished(true), memInfo(memInfo), mem(nullptr)
	{
		memInfo->addMemSize(sizeof(logFileShard));
		memcpy(&metaInfo, meta, sizeof(metaInfo));
	}

	DLL_EXPORT logFileShard::~logFileShard()
	{
		memInfo->subMemSize(sizeof(logFileShard));
		if (mem != nullptr)
			memInfo->freeMem(mem, volumn);
	}

	DLL_EXPORT DS logFileShard::flush(fileHandle fd, char* compressBuf, uint32_t compressBufSize)
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


	DLL_EXPORT DS logFileShard::loadCheckpoint(fileHandle file, RPC::Checkpoint*& c)
	{
		c = nullptr;
		logFileShardMetaInfo meta;
		if (sizeof(meta) != readFile(file, (char*)&meta, sizeof(meta)))
			dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
		if (meta.writedSize == 0)
			dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for shard is empty", ERROR);
		RPC::Checkpoint _c;
		if (offsetof(RPC::Checkpoint, externInfo) != readFile(file, (char*)&c, offsetof(RPC::Checkpoint, externInfo)))
			dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
		if (_c.externSize == 0)
		{
			c = _c.clone();
			dsOk();
		}
		else
		{
			c = (RPC::Checkpoint*)malloc(sizeof(RPC::Checkpoint) + c->externSize - 1);
			if (_c.externSize != readFile(file, c->externInfo, _c.externSize))
			{
				free(c);
				c = nullptr;
				dsFailedAndLogIt(1, "read log entry checkpoint from local log file failed for " << errno << "," << strerror(errno), ERROR);
			}
			memcpy(c, &_c, offsetof(RPC::Checkpoint, externInfo));
			dsOk();
		}
	}

	DLL_EXPORT DS logFileShard::loadData(fileHandle file)
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
		begin = (RPC::Checkpoint*)_mem;
		end = (RPC::Checkpoint*)(_mem + metaInfo.lastRecordoffset);
		volumn = metaInfo.writedSize;
		flushed = true;
		mem = _mem;
		dsOk();
	}



	DLL_EXPORT DS logFileShard::loadMetaInfo(memoryInfo* memInfo, logFileShard*& shard, fileHandle file)
	{
		shard = nullptr;
		logFileShardMetaInfo meta;
		if (sizeof(meta) != readFile(file, (char*)&meta, sizeof(meta)))
			dsFailedAndLogIt(1, "read shard meta info from local log file failed for " << errno << "," << strerror(errno), ERROR);
		if (meta.writedSize == 0)
			dsFailedAndLogIt(1, "read local log file failed for shard is empty", ERROR);
		if (meta.shardId == UINT32_MAX)//end of shards
			dsOk();
		RPC::Checkpoint c;
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

	DLL_EXPORT DS logFileShard::load(memoryInfo* memInfo, logFileShard*& shard, fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize)
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

	DLL_EXPORT DS logFileShard::loadMetaInfoAndJumpToNext(memoryInfo* memInfo, logFileShard*& shard, fileHandle file)
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

	DLL_EXPORT DS logFileShard::load(fileHandle file, char* deCompressBuf, uint32_t deCompressBufSize)
	{
		mem = (char*)malloc(metaInfo.writedSize);
		uint32_t dataOffset = metaInfo.offsetInFile + beginCheckpointSize + sizeof(logFileShardMetaInfo);
		if (dataOffset != seekFile(file, dataOffset, SEEK_SET))
			dsFailedAndLogIt(1, "seek to logFileShard offset" << dataOffset << " in file failed for " << errno << "," << strerror(errno), ERROR);
		dsReturn(loadData(file));
	}




	DLL_EXPORT void logFileShard::startRead()
	{
		readStatus.lock_shared();
	}

	void logFileShard::finshedRead()
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




	DLL_EXPORT logFile::logFile(uint32_t fileId, memoryInfo* memInfo) :m_first(nullptr), m_firstNotFlushed(nullptr),
		m_current(nullptr), m_readFd(INVALID_HANDLE_VALUE), m_writeFd(INVALID_HANDLE_VALUE), m_metaInfo(fileId, 0, 0),
		m_size(0), m_memInfo(memInfo), m_next(nullptr)
	{
		m_memInfo->addMemSize(sizeof(logFile));
	}
	DLL_EXPORT logFile::logFile(uint32_t fileId, uint64_t rawFileid, memoryInfo* memInfo) :m_first(new logFileShard(memInfo, 0, 0)), m_firstNotFlushed(m_first),
		m_current(m_first), m_readFd(INVALID_HANDLE_VALUE), m_writeFd(INVALID_HANDLE_VALUE), m_metaInfo(fileId, rawFileid, Timer::getNowTimestamp()),
		m_size(0), m_memInfo(memInfo), m_next(nullptr)
	{
		m_memInfo->addMemSize(sizeof(logFile));
		m_first->metaInfo.beginSeqno.seqNo = m_metaInfo.seqNo.seqNo;
	}

	DLL_EXPORT logFile::~logFile()
	{
		clear(false);
		m_memInfo->subMemSize(sizeof(logFile));
	}

	DLL_EXPORT DS logFile::init()
	{
		m_current = new logFileShard(m_memInfo, m_metaInfo.seqNo.seqNo, m_memInfo->getDefaultShardSize());
		dsOk();
	}

	void logFile::purgeTo(uint64_t seqNo)
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
					m_first->readStatus.unlock();
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

	void logFile::clear(bool removeFile)
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

	DS logFile::close(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
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

	inline DS logFile::flush(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
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

	DS logFile::appendEndInfoToFile()
	{
		logFileShardMetaInfo meta;
		meta.shardId = UINT32_MAX;//means logfile finished
		if (sizeof(meta) != writeFile(m_writeFd, (char*)&meta, sizeof(meta)))
			dsFailedAndLogIt(1, "write tail info to local log file " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		dsOk();
	}

	DS logFile::flushIndex(const char* fileDir, char* compressBuf, uint32_t compressBufSize)
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

	DS logFile::getBeginCheckpoint(const char* file, RPC::Checkpoint*& begin)
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
		if (fileSize < sizeof(metaInfo) + sizeof(RPC::Checkpoint) - 1)
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

	DS logFile::getEndCheckpoint(RPC::Checkpoint*& end)
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

	DS logFile::loadShardData(logFileShard* s)
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

	DS logFile::generateIndexFromData()
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
				if (fileSize != offsetOfFile) // corrupt file
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

	DS logFile::loadIndex()
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



	DS logFile::loadMetaInfo()
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

	DS logFile::load(uint32_t fileId, const char* filePath, memoryInfo* memInfo, logFile*& file)
	{
		file = nullptr;
		logFile* _file = new logFile(fileId, memInfo);
		_file->m_filePath = filePath;
		dsReturnIfFailedWithOp(_file->loadMetaInfo(), delete _file);
		file = _file;
		dsOk();
	}

	void logFile::closeReadFd()
	{
		if (!fileHandleValid(m_readFd))
		{
			closeFile(m_readFd);
			m_readFd = INVALID_HANDLE_VALUE;
		}
	}

	DLL_EXPORT void logFile::startRead()
	{
		m_isReading.lock_shared();
	}

	DLL_EXPORT void logFile::finishRead()
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

	DLL_EXPORT logFile::iterator::iterator(logFile* file) :file(file), shard(nullptr), offset(0)
	{
		if (file != nullptr)
			file->startRead();
	}
	DLL_EXPORT logFile::iterator::~iterator()
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

	DLL_EXPORT DS logFile::iterator::attachToNextShard()
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

	DLL_EXPORT DS logFile::iterator::waitNextRecord(LogEntry*& e, int32_t outTime)
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
		_next(e);
		dsOk();
	}


	DLL_EXPORT void logFile::iterator::setLogFile(logFile* file)
	{
		this->file = file;
		file->startRead();
	}

	DLL_EXPORT DS logFile::iterator::seek(uint64_t seqNo, LogEntry*& e)
	{
		if (shard != nullptr)
		{
			shard->finshedRead();
			shard = nullptr;
		}

		e = nullptr;
		RPC::LogSeqNo seq;
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
			if (((LogEntry*)(s->mem + soffset))->getCheckpoint()->seqNo.seqNo == seqNo)
			{
				e = (LogEntry*)(s->mem + soffset);
				offset = soffset;
				shard = s;
				dsOk();
			}
		}
		s->finshedRead();
		dsFailedAndLogIt(1, "seek seqNo:" << seqNo << " in file " << file->getFileId() << " failed for shard has been purged", ERROR);
	}

	DLL_EXPORT DS logFile::iterator::seekToBegin(LogEntry*& e)
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
			e = (LogEntry*)(s->mem);
			offset = e->size;
		}
		dsOk();
	}

	DLL_EXPORT DS logFile::iterator::attachToNextFile(LogEntry*& e)
	{
		logFile* next = file->getNext();
		if (next == nullptr)
			dsFailedAndLogIt(1, "attatch to next file of " << file->getFileId() << " failed for next is null", ERROR);

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
}