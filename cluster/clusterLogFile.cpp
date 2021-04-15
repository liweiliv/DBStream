#include "clusterLogFile.h"
namespace CLUSTER
{
	DLL_EXPORT clusterLogFile::block::block(std::function<DS (block*)>& loadFunc, uint32_t defaultBlockSize, uint32_t offset, const logEntryRpcBase* logEntry) :
		idx(offset, logEntry->logIndex), data(nullptr), id(0), volumn(0), size(0), writedSize(0), next(nullptr), m_ref(this, loadFunc)
	{
		if (logEntry->size > defaultBlockSize)
			volumn = logEntry->size;
		else
			volumn = defaultBlockSize;
		data = new char[volumn];
		memcpy(data, logEntry, logEntry->size);
		size = logEntry->size;
	}
	DLL_EXPORT clusterLogFile::block::block(std::function<DS (block*)>& loadFunc, const index& i) :
		idx(i.offset, i.logIndex), data(nullptr), volumn(0),
		size(0), writedSize(0), next(nullptr), m_ref(this, loadFunc)
	{
	}
	DLL_EXPORT clusterLogFile::block::block(std::function<DS (block*)>& loadFunc, const index& i, char* data, uint32_t volumn, uint32_t size) :
		idx(i.offset, i.logIndex), data(data), volumn(volumn),
		size(size), writedSize(size), next(nullptr), m_ref(this, loadFunc)
	{
	}
	DLL_EXPORT clusterLogFile::block::~block()
	{
		if (data != nullptr)
			delete[] data;
	}
	DLL_EXPORT void clusterLogFile::block::close()
	{
		delete[] data;
		data = nullptr;
	}
	uint32_t clusterLogFile::block::find(const logIndexInfo& logIndex)
	{
		if (data == nullptr)
			return INVALID_RESULT;
		uint32_t currentSize = size;
		const logEntryRpcBase* logEntry = (const logEntryRpcBase*)data;
		while (((const char*)logEntry) - data < currentSize && logEntry->logIndex < logIndex)
			logEntry = (const logEntryRpcBase*)(((const char*)logEntry) + logEntry->size);
		if (((const char*)logEntry) - data >= currentSize)
			return INVALID_RESULT;
		return ((const char*)logEntry) - data;
	}
	void clusterLogFile::block::append(const logEntryRpcBase* logEntry)
	{
		memcpy(data + size, logEntry, logEntry->size);
		wmb();
		size += logEntry->size;
	}
	DLL_EXPORT clusterLogFile::clusterLogFile(const char* filePath, uint64_t fileId, logConfig& config) :m_filePath(filePath), m_fileId(fileId), m_offset(0),
		m_blockCount(0), m_blocks(nullptr), m_currentBlock(nullptr),
		m_config(config), m_maxBlockCount(m_config.getDefaultLogFileSize() / m_config.getDefaultBlockSize()),
		m_next(nullptr),
		m_blockLoadFunc(std::bind(&clusterLogFile::loadBlock, this, std::placeholders::_1)),
		m_fileLoadFunc(std::bind(&clusterLogFile::loadFile, this, std::placeholders::_1)),
		m_ref(this, m_fileLoadFunc),
		m_fd(INVALID_HANDLE_VALUE), m_indexFd(INVALID_HANDLE_VALUE), m_readFd(INVALID_HANDLE_VALUE)
	{
	}
	DLL_EXPORT clusterLogFile::~clusterLogFile()
	{
		close();
	}
	DLL_EXPORT uint64_t clusterLogFile::getFileId()
	{
		return m_fileId;
	}
	const logIndexInfo& clusterLogFile::getPrevLogIndex()
	{
		return m_prevLogIndex;
	}
	DLL_EXPORT const logIndexInfo& clusterLogFile::getBeginLogIndex()
	{
		return m_beginLogIndex;
	}
	DLL_EXPORT const logIndexInfo& clusterLogFile::getLogIndex()
	{
		return m_logIndex;
	}
	DLL_EXPORT clusterLogFile* clusterLogFile::getNext()
	{
		return m_next;
	}
	DLL_EXPORT void clusterLogFile::setNext(clusterLogFile* next)
	{
		m_next = next;
	}
	 void clusterLogFile::notify()
	{
		std::unique_lock <std::mutex> lck(m_lock);
		m_condition.notify_all();
	}
	 void clusterLogFile::wait()
	{
		std::unique_lock <std::mutex> lck(m_lock);
		m_condition.wait_for(lck, std::chrono::microseconds(10));
	}
	DS clusterLogFile::openLogFile(fileHandle& handle, bool readOnly)
	{
		handle = ::openFile(m_filePath.c_str(), true, !readOnly, false);
		if (handle == INVALID_HANDLE_VALUE)
		{
			int err = errno;
			if (checkFileExist(m_filePath.c_str(), F_OK) != 0)
				dsFailedAndLogIt(errorCode::ioError, m_filePath << " not exist", ERROR);
			else
				dsFailedAndLogIt(errorCode::ioError, "open file :" << m_filePath << " failed for " << err << "," << strerror(err), ERROR);
		}
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::readRecordHead(fileHandle fd, uint32_t dataFileSize, uint32_t pos, logEntryRpcBase& head, bool readOnly)
	{
		if (dataFileSize - pos < sizeof(head))
		{
			LOG(WARNING) << "cluster log file " << m_filePath << " ended with an unfinished record";
			if (readOnly)
				dsFailedAndLogIt(errorCode::ioError, "cluster log file " << m_filePath << " ended with an unfinished record", ERROR);
			if (0 != truncateFile(fd, pos))
				dsFailedAndLogIt(errorCode::ioError, "truncate file:" << m_filePath << "to new size:" << pos << " failed,error:" << errno << "," << strerror(errno), ERROR);
			dsFailed(errorCode::endOfFile, "");
		}
		if (sizeof(head) != readFile(fd, (char*)&head, sizeof(head)))
			dsFailedAndLogIt(errorCode::ioError, "read record head from  file:" << m_filePath << " failed,error:" << errno << "," << strerror(errno), ERROR);
		if (dataFileSize - pos < head.size)
		{
			LOG(WARNING) << "cluster log file " << m_filePath << " ended with an unfinished record,type:" << head.recordType << ",logIndex:" << head.logIndex.term << "." << head.logIndex.logIndex;
			if (readOnly)
				dsFailedAndLogIt(errorCode::ioError, "cluster log file " << m_filePath << " ended with an unfinished record,type:" << head.recordType << ",logIndex:" << head.logIndex.term << "." << head.logIndex.logIndex, ERROR);
			if (0 != truncateFile(fd, pos))
				dsFailedAndLogIt(errorCode::ioError, "truncate file:" << m_filePath << "to new size:" << pos << " failed,error:" << errno << "," << strerror(errno), ERROR);
			dsFailed(errorCode::endOfFile, "");
		}
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::recoveryIndexFromLastBlock(fileHandle fd, uint32_t offset, uint32_t dataFileSize, bool readOnly)
	{
		if (seekFile(fd, offset, SEEK_SET) != offset)
			dsFailedAndLogIt(errorCode::ioError, "seek to " << offset << " of log file:" << m_filePath << " failed,error:" << errno << "," << strerror(errno), ERROR);
		if (seekFile(m_indexFd, sizeof(index) * m_blockCount, SEEK_SET) != (int64_t)(sizeof(index) * m_blockCount))
			dsFailedAndLogIt(errorCode::ioError, "seek to " << offset << " of log file:" << (m_filePath + INDEX_NAME) << " failed,error:" << errno << "," << strerror(errno), ERROR);
		char* buffer;
		uint32_t bufferSize = 0;
		int64_t pos = offset;
		while (pos < dataFileSize)
		{
			logEntryRpcBase head;
			DS rtv = readRecordHead(fd, dataFileSize, pos, head, readOnly);
			if (!dsCheck(rtv))
			{
				if (rtv == -errorCode::endOfFile)
					break;
				else
					dsReturn(rtv);
			}
			if (head.size > m_config.getDefaultBlockSize())
				buffer = new char[bufferSize = head.size];
			else
				buffer = new char[bufferSize = m_config.getDefaultBlockSize()];
			index idx(pos, head.logIndex);
			bool readHead = false;
			int off = 0;
			while (true)
			{
				if (readHead)
				{
					DS rtv = readRecordHead(fd, dataFileSize, pos, head, readOnly);
					if (!dsCheck(rtv))
					{
						if (rtv == -errorCode::endOfFile)
							break;
						else
							dsReturn(rtv);
					}
					readHead = false;
					pos += sizeof(head);
					if (head.size + off >= bufferSize)
						break;
				}
				else
				{
					memcpy(buffer + off, &head, sizeof(head));
					uint32_t readSize = ((logEntryRpcBase*)buffer + off)->size - sizeof(logEntryRpcBase);
					if (readSize != readFile(fd, buffer + off + sizeof(raftRpcHead), readSize))
						dsFailedAndLogIt(errorCode::ioError, "read record head from file:" << m_filePath << " failed,error:" << errno << "," << strerror(errno), ERROR);
					readHead = true;
					off += head.size;
					if ((pos += readSize) == dataFileSize)//end of file
						break;
				}
			}
			m_blocks[m_blockCount] = new block(m_blockLoadFunc, idx, buffer, bufferSize, off);
			m_blocks[m_blockCount]->id = m_blockCount;
			if (m_blockCount > 0)
			{
				m_blocks[m_blockCount - 1]->size = m_blocks[m_blockCount]->idx.offset - m_blocks[m_blockCount - 1]->idx.offset;
				m_blocks[m_blockCount - 1]->next = m_blocks[m_blockCount];
			}
			m_blockCount++;
			m_offset = pos;
			if (sizeof(idx) != writeFile(m_indexFd, (char*)&idx, sizeof(idx)))
				dsFailedAndLogIt(errorCode::ioError, "write index info to:" << (m_filePath + INDEX_NAME) << " failed,error:" << errno << "," << strerror(errno), ERROR);
		}
		if (m_blockCount > 0)
			m_blocks[m_blockCount]->size = pos - m_blocks[m_blockCount]->idx.offset;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::loadIndex(fileHandle fd, bool readOnly)
	{
		int64_t dataFileSize = getFileSize(fd);
		if (dataFileSize < 0)
			dsFailedAndLogIt(errorCode::ioError, "get size of file:" << m_filePath << "failed,error:" << errno << "," << strerror(errno), ERROR);
		int64_t indexFileSize;
		int64_t readSize;
		int indexCount;
		char buf[sizeof(index) * INDEX_READ_BATCH];
		if (m_indexFd == INVALID_HANDLE_VALUE)
		{
			if ((m_indexFd = ::openFile((m_filePath + INDEX_NAME).c_str(), true, true, false)) == INVALID_HANDLE_VALUE)
			{
				int err = errno;
				if (checkFileExist((m_filePath + INDEX_NAME).c_str(), F_OK) == 0)
					dsFailedAndLogIt(errorCode::ioError, "open file :" << (m_filePath + INDEX_NAME) << " failed for " << err << "," << strerror(err), ERROR);
				if ((m_indexFd = ::openFile((m_filePath + INDEX_NAME).c_str(), true, true, true)) == INVALID_HANDLE_VALUE)
					dsFailedAndLogIt(errorCode::ioError, "create file :" << (m_filePath + INDEX_NAME) << " failed for " << errno << "," << strerror(errno), ERROR);
				goto CHECK_LAST_BLOCK;
			}
		}
		else
		{
			if (0 != seekFile(m_indexFd, 0, SEEK_SET))
				dsFailedAndLogIt(errorCode::ioError, "read cluster log index " << m_filePath << INDEX_NAME << " failed for:" << errno << "," << strerror(errno), ERROR);
		}

		indexFileSize = getFileSize(m_indexFd);
		if (indexFileSize < 0)
			dsFailedAndLogIt(errorCode::ioError, "get size of file:" << m_filePath << INDEX_NAME << " failed,error:" << errno << "," << strerror(errno), ERROR);

		indexCount = indexFileSize / sizeof(index);
		for (int readed = 0, count = 0; readed < indexCount; readed += count)
		{
			count = INDEX_READ_BATCH;
			if (count > indexCount - readed)
				count = indexCount - readed;
			if ((readSize = readFile(m_indexFd, buf, (count * sizeof(index)))) != (int64_t)(count * sizeof(index)))
				dsFailedAndLogIt(errorCode::ioError, "read index file:" << m_filePath << INDEX_NAME << " failed,error:" << errno << "," << strerror(errno), ERROR);
			for (int i = 0; i < count; i++)
			{
				m_blocks[readed + i] = new block(m_blockLoadFunc, *(index*)(&buf[sizeof(index) * i]));
				m_blocks[readed + i]->id = readed + i;
				if (readed + i > 0)
				{
					m_blocks[readed + i - 1]->next = m_blocks[readed + i];
					m_blocks[readed + i - 1]->size = m_blocks[readed + i]->idx.offset - m_blocks[readed + i - 1]->idx.offset;
				}
			}
		}
		//check if last index only write part to file
		if (indexFileSize % sizeof(index) != 0)
		{
			if (0 != truncateFile(m_indexFd, indexFileSize - indexFileSize % sizeof(index)))
			{
				dsFailedAndLogIt(errorCode::ioError, "truncate index file:" << m_filePath << INDEX_NAME << "to new size:" << indexFileSize - indexFileSize % sizeof(index) << " failed,error:" << errno << "," << strerror(errno), ERROR);
			}
		}
		m_blockCount = indexCount;
	CHECK_LAST_BLOCK:
		if (m_blockCount > 0)
		{
			if (dataFileSize - m_blocks[m_blockCount - 1]->idx.offset < m_config.getDefaultBlockSize())
			{
				m_blocks[m_blockCount - 1]->size = dataFileSize - m_blocks[m_blockCount - 1]->idx.offset;
				m_offset = dataFileSize;
				dsOk();
			}
			else
				dsReturn(recoveryIndexFromLastBlock(fd, m_blocks[m_blockCount - 1]->idx.offset, dataFileSize, readOnly));
		}
		else
		{
			if (dataFileSize >= (int64_t)HEAD_SIZE)
				dsReturn(recoveryIndexFromLastBlock(fd, HEAD_SIZE, dataFileSize, readOnly));
			else
				dsFailedAndLogIt(errorCode::emptyLogFile, "size of " << m_filePath << " is " << dataFileSize, WARNING);
		}
	}

	DLL_EXPORT DS clusterLogFile::readMetaInfo()
	{
		if (m_readFd == INVALID_HANDLE_VALUE)
			dsReturnIfFailed(openLogFile(m_readFd, true));
		else if (0 != seekFile(m_readFd, 0, SEEK_SET))
			dsFailedAndLogIt(errorCode::ioError, "seek to begin of cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		if (sizeof(m_prevLogIndex) != readFile(m_readFd, (char*)&m_prevLogIndex, sizeof(m_prevLogIndex))
			|| sizeof(m_beginLogIndex) != readFile(m_readFd, (char*)&m_beginLogIndex, sizeof(m_beginLogIndex)))
			dsFailedAndLogIt(errorCode::ioError, "read meta info from cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		dsOk();
	}

	DLL_EXPORT DS clusterLogFile::recovery()
	{
		if (m_fd == INVALID_HANDLE_VALUE)
			dsReturnIfFailed(openLogFile(m_fd, false));
		if (m_beginLogIndex.invalid() || m_prevLogIndex.invalid())
			dsReturnIfFailed(readMetaInfo());
		if (m_blocks == nullptr)
		{
			m_blocks = new block * [m_maxBlockCount];
			memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
		}
		dsReturnIfFailed(loadIndex(m_fd, false));
		if (m_blockCount > 0)
		{
			m_currentBlock = m_blocks[m_blockCount - 1];
			dsReturnIfFailed(m_currentBlock->m_ref.use());
			if (seekFile(m_fd, m_currentBlock->idx.offset + m_currentBlock->size, SEEK_SET) != m_currentBlock->idx.offset + m_currentBlock->size)
				dsFailedAndLogIt(errorCode::ioError, "seek to end of cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		else
		{
			if (seekFile(m_fd, HEAD_SIZE, SEEK_SET) != HEAD_SIZE)
				dsFailedAndLogIt(errorCode::ioError, "seek to end of cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		m_ref.useForWrite();
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::load()
	{
		if (m_readFd == INVALID_HANDLE_VALUE)
			dsReturnIfFailed(openLogFile(m_readFd, true));
		if (m_blocks == nullptr)
		{
			m_blocks = new block * [m_maxBlockCount];
			memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
		}
		dsReturn(loadIndex(m_readFd, true));
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::loadFile(clusterLogFile* logFile)
	{
		dsReturn(logFile->load());
	}

	inline DS clusterLogFile::_append(const logEntryRpcBase* logEntry)
	{
		if (unlikely(m_currentBlock == nullptr))
		{
			dsReturnIfFailed(appendToNewBlock(logEntry));
		}
		else if (unlikely(m_currentBlock->size + logEntry->size + sizeof(raftRpcHead) > m_currentBlock->volumn))
		{
			if (m_blockCount + 1 >= m_maxBlockCount)
				dsFailed(errorCode::full, nullptr);
			dsReturnIfFailed(appendToNewBlock(logEntry));
		}
		else
		{
			m_currentBlock->append(logEntry);
		}
		m_logIndex = logEntry->logIndex;
		m_offset += logEntry->size;
		notify();
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::appendToNewBlock(const logEntryRpcBase* logEntry)
	{
		//flush current block
		if (m_currentBlock != nullptr)
			dsReturnIfFailed(writeCurrentBlock());
		block* newBlock = new block(m_blockLoadFunc, m_config.getDefaultBlockSize(), m_offset, logEntry);

		if (m_indexFd == INVALID_HANDLE_VALUE)
		{
			if ((m_indexFd = openFile((m_filePath + INDEX_NAME).c_str(), true, true, true)) == INVALID_HANDLE_VALUE)
				dsFailedAndLogIt(errorCode::ioError, "open index file:" << m_filePath << ".idx failed for:" << errno << "," << strerror(errno), ERROR);
			if (seekFile(m_indexFd, sizeof(index) * m_blockCount, SEEK_SET) != (int64_t)(sizeof(index) * m_blockCount))
				dsFailedAndLogIt(errorCode::ioError, "seek index file:" << m_filePath << ".idx to " << sizeof(index) * m_blockCount << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		if (writeFile(m_indexFd, (char*)&newBlock->idx, sizeof(newBlock->idx)) != sizeof(newBlock->idx) || ::fsync(m_indexFd) != 0)
		{
			int err = errno;
			truncateFile(m_indexFd, sizeof(newBlock->idx) * m_blockCount);
			dsFailedAndLogIt(errorCode::ioError, "write index file:" << m_filePath << ".idx failed for:" << err << "," << strerror(err), ERROR);
		}
		wmb();
		m_blocks[m_blockCount] = newBlock;
		newBlock->id = m_blockCount;
		newBlock->m_ref.useForWrite();
		m_blockCount++;
		if (m_currentBlock != nullptr)
		{
			m_currentBlock->next = newBlock;
			m_currentBlock->m_ref.unuse();
		}
		m_currentBlock = newBlock;
		dsOk();
	}
	DLL_EXPORT clusterLogFile::block* clusterLogFile::findBlock(const logIndexInfo& logIndex)
	{
		logIndexInfo  blockEndLogIndex;
		if (m_blockCount == 0)
			return nullptr;
		int s = 0, e = m_blockCount, m;
		while (s < e)
		{
			m = (s + e) >> 1;
			block* b = m_blocks[m];
			if (b->idx.logIndex < logIndex)
			{
				s = m + 1;
				continue;
			}
			if (b->idx.logIndex > logIndex)
			{
				e = m - 1;
				continue;
			}
			else
				return b;
		}
		if (e < 0)
			return nullptr;

		if (s >= (int)m_blockCount)
			s = m_blockCount - 1;
		if (s == (int)m_blockCount - 1)
		{
			blockEndLogIndex = m_logIndex;
			blockEndLogIndex.logIndex += 1;
		}
		else
		{
			blockEndLogIndex = m_blocks[s + 1]->idx.logIndex;
		}

		if ((m_blocks[s]->idx.logIndex <= logIndex)
			&& blockEndLogIndex > logIndex)
			return m_blocks[s];
		else
			return nullptr;
	}
	DLL_EXPORT DS clusterLogFile::loadBlock(block* b)
	{
		if (b->data != nullptr)
			dsOk();
		char* data = new char[b->size];
		m_lock.lock();
		if (m_readFd == INVALID_HANDLE_VALUE)
		{
			if (INVALID_HANDLE_VALUE == (m_readFd = openFile(m_filePath.c_str(), true, false, false)))
			{
				m_lock.unlock();
				delete[]data;
				dsFailedAndLogIt(errorCode::ioError, "open cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
		}
		if (b->idx.offset != seekFile(m_readFd, b->idx.offset, SEEK_SET))
		{
			m_lock.unlock();
			delete[]data;
			dsFailedAndLogIt(errorCode::ioError, "read cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		if (b->size != readFile(m_readFd, data, b->size))
		{
			m_lock.unlock();
			delete[]data;
			dsFailedAndLogIt(errorCode::ioError, "read cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		m_lock.unlock();
		if (b->data == nullptr)
			b->data = data;
		else
			delete[]data;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::open()
	{
		if ((m_fd = openFile(m_filePath.c_str(), true, true, false)) == INVALID_HANDLE_VALUE)
		{
			dsFailedAndLogIt(errorCode::ioError, "open cluster log " << m_filePath << "failed for:" << errno << "," << strerror(errno), ERROR);
		}
		if ((m_offset = seekFile(m_fd, 0, SEEK_END)) < 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "seek to end of cluster log " << m_filePath << "failed for:" << errno << "," << strerror(errno), ERROR);
		}
		dsOk();
	}

	DLL_EXPORT DS clusterLogFile::create(clusterLogFile* prev, const logIndexInfo& beginLogIndex)
	{
		dsReturnIfFailed(create(prev->m_logIndex, beginLogIndex));
		prev->m_next = this;
		dsOk();
	}

	DLL_EXPORT DS clusterLogFile::create(const logIndexInfo& prevLogIndex, const logIndexInfo& beginLogIndex)
	{
		if (checkFileExist(m_filePath.c_str(), F_OK) == 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "create cluster log " << m_filePath << "failed for file exist", ERROR);
		}
		if ((m_fd = openFile(m_filePath.c_str(), true, true, true)) == INVALID_HANDLE_VALUE)
		{
			dsFailedAndLogIt(errorCode::ioError, "create cluster log " << m_filePath << "failed for:" << errno << "," << strerror(errno), ERROR);
		}
		if (writeFile(m_fd, (char*)&prevLogIndex, sizeof(prevLogIndex)) != sizeof(prevLogIndex) ||
			writeFile(m_fd, (char*)&beginLogIndex, sizeof(beginLogIndex)) != sizeof(beginLogIndex))
		{
			int errCode = errno;
			closeFile(m_fd);
			m_fd = INVALID_HANDLE_VALUE;
			remove(m_filePath.c_str());
			dsFailedAndLogIt(errorCode::ioError, "create cluster log " << m_filePath << "failed for:" << errCode << "," << strerror(errCode), ERROR);
		}
		m_logIndex = m_prevLogIndex = prevLogIndex;
		m_beginLogIndex = beginLogIndex;
		m_blocks = new block * [m_maxBlockCount];
		m_offset = HEAD_SIZE;
		memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
		m_ref.useForWrite();
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::deleteFile()
	{
		close();
		if (remove((m_filePath + ".idx").c_str()) != 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "delete index file:" << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		if (remove(m_filePath.c_str()) != 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "delete file:" << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		dsOk();
	}
	DLL_EXPORT void clusterLogFile::close()
	{
		if (m_fd != INVALID_HANDLE_VALUE)
		{
			if (m_currentBlock != nullptr)
			{
				if (m_currentBlock->size > m_currentBlock->writedSize)
					dsCheckButIgnore(writeCurrentBlock());
				m_currentBlock->m_ref.unuse();
			}
			closeFile(m_fd);
			m_fd = INVALID_HANDLE_VALUE;
		}
		if (m_readFd != INVALID_HANDLE_VALUE)
		{
			closeFile(m_readFd);
			m_readFd = INVALID_HANDLE_VALUE;
		}
		if (m_indexFd != INVALID_HANDLE_VALUE)
		{
			closeFile(m_indexFd);
			m_indexFd = INVALID_HANDLE_VALUE;
		}
		if (m_blocks != nullptr)
		{
			for (uint32_t i = 0; i < m_blockCount; i++)
			{
				m_blocks[i]->m_ref.needFree();
				m_blocks[i] = nullptr;
			}
			delete[]m_blocks;
			m_blocks = nullptr;
		}
	}
	DLL_EXPORT DS clusterLogFile::finish()
	{
		raftRpcHead head = { sizeof(raftRpcHead),static_cast<uint8_t>(rpcType::endOfFile),VERSION };
		memcpy(m_currentBlock->data + m_currentBlock->size, &head, sizeof(head));
		wmb();
		m_currentBlock->size += sizeof(head);
		m_offset += sizeof(head);
		dsReturnIfFailed(flush());
		m_currentBlock->m_ref.unuse();
		closeFile(m_fd);
		m_fd = INVALID_HANDLE_VALUE;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::writeCurrentBlock()
	{
		std::lock_guard<std::mutex> guard(m_flushLock);
		if (m_currentBlock->size == m_currentBlock->writedSize)
			dsOk();
		if (writeFile(m_fd, m_currentBlock->data + m_currentBlock->writedSize, m_currentBlock->size - m_currentBlock->writedSize) !=
			(m_currentBlock->size - m_currentBlock->writedSize) || ::fsync(m_fd) != 0)
		{
			dsFailedAndLogIt(errorCode::ioError, "write cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
		}
		m_currentBlock->writedSize = m_currentBlock->size;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::append(const logEntryRpcBase* logEntry)
	{
		if (unlikely(m_logIndex != logEntry->prevRecordLogIndex))
		{
			if (m_logIndex < logEntry->prevRecordLogIndex)
			{
				dsFailedAndLogIt(errorCode::prevNotMatch, "prev log index of new logEntry :" << logEntry->prevRecordLogIndex.term << "." << logEntry->prevRecordLogIndex.logIndex << " is large than current logIndex:" <<
					m_logIndex.term << "." << m_logIndex.logIndex, WARNING);
			}
			else
			{
				dsFailedAndLogIt(errorCode::rollback, "prev log index of new logEntry :" << logEntry->prevRecordLogIndex.term << "." << logEntry->prevRecordLogIndex.logIndex << " is less than current logIndex:" <<
					m_logIndex.term << "." << m_logIndex.logIndex, WARNING);
			}
		}
		//check if is full
		if (unlikely(m_offset + logEntry->size >= m_config.getDefaultLogFileSize()))
			dsFailed(errorCode::full, nullptr);
		dsReturn(_append(logEntry));
	}
	DLL_EXPORT DS clusterLogFile::clear()
	{
		for (uint32_t i = 0; i < m_blockCount; i++)
		{
			delete m_blocks[i];
			m_blocks[i] = nullptr;
		}
		m_currentBlock = nullptr;
		m_logIndex = m_beginLogIndex;
		dsOk();
	}

	DLL_EXPORT DS clusterLogFile::rollback(const logIndexInfo& logIndex)
	{
		block* block = findBlock(logIndex);
		if (block == nullptr)
			dsOk();
		if (block->data == nullptr)
		{
			dsReturnIfFailed(loadBlock(block));
		}
		uint32_t offsetInBlock = block->find(logIndex);
		if (offsetInBlock == block::INVALID_RESULT)
			dsOk();
		block->size = offsetInBlock;
		int newBlockCount = offsetInBlock == 0 ? block->id : block->id + 1;

		m_currentBlock = newBlockCount > 0 ? m_blocks[newBlockCount - 1] : nullptr;
		m_offset = m_currentBlock == nullptr ? 0 : m_currentBlock->idx.offset + m_currentBlock->size;
		if (seekFile(m_indexFd, 0, SEEK_END) > int64_t(sizeof(index) * newBlockCount))
		{
			if (0 != truncateFile(m_indexFd, sizeof(index) * newBlockCount))
			{
				dsFailedAndLogIt(errorCode::ioError, "rollback to " << logIndex.term << "." << logIndex.logIndex << " failed for truncate index file " << m_filePath << " failed," << errno << "," << strerror(errno), ERROR);
			}
		}
		if (seekFile(m_fd, 0, SEEK_END) > m_offset)
		{
			if (0 != truncateFile(m_fd, m_offset))
			{
				dsFailedAndLogIt(errorCode::ioError, "rollback to " << logIndex.term << "." << logIndex.logIndex << " failed for truncate log file " << m_filePath << "failed," << errno << "," << strerror(errno), ERROR);
			}
		}

		for (uint32_t blockId = (offsetInBlock == 0 ? block->id : block->id + 1); blockId < m_blockCount; blockId++)
		{
			delete m_blocks[blockId];
			m_blocks[blockId] = nullptr;
		}
		m_blockCount = newBlockCount;
		if (m_currentBlock != nullptr)
			m_currentBlock->next = nullptr;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::flush()
	{
		if (m_currentBlock != nullptr)
		{
			dsReturnIfFailed(writeCurrentBlock());
		}
		if (::fsync(m_fd) != 0)
		{
			dsFailedAndLogIt(errorCode::rollback, "flush cluster log failed for:" << errno << "," << strerror(errno), ERROR);
		}
		dsOk();
	}


	DLL_EXPORT DS clusterLogFile::iterator::rotate(block* b)
	{
		dsReturnIfFailed(b->m_ref.use());
		if (m_block != nullptr)
			m_block->m_ref.unuse();
		m_block = b;
		m_offset = 0;
		dsOk();
	}
	DLL_EXPORT DS clusterLogFile::iterator::setLogFile(clusterLogFile* file)
	{
		m_file = file;
		dsReturn(file->m_ref.use());
	}
	DLL_EXPORT DS clusterLogFile::iterator::attachToNextLogFile(clusterLogFile* file)
	{
		dsReturnIfFailed(file->use());
		if (m_block != nullptr)
		{
			m_block->m_ref.unuse();
			m_block = nullptr;
		}
		m_file->unUse();
		m_file = file;
		dsReturn(rotate(m_file->m_blocks[0]));
	}
	DLL_EXPORT DS clusterLogFile::iterator::search(const logIndexInfo& logIndex)
	{
		if (nullptr == (m_block = m_file->findBlock(logIndex)))
		{
			dsFailedAndLogIt(errorCode::logIndexNotFound, "do not find logIndex:" << logIndex.term << "." << logIndex.logIndex, WARNING);
		}
		dsReturnIfFailed(m_block->m_ref.use());
		if ((m_offset = m_block->find(logIndex)) == block::INVALID_RESULT)
		{
			m_block->m_ref.unuse();
			dsFailedAndLogIt(errorCode::logIndexNotFound, "do not find logIndex:" << logIndex.term << "." << logIndex.logIndex, WARNING);
		}
		m_logIndex = logIndex;
		dsOk();
	}

	DLL_EXPORT DS clusterLogFile::iterator::next(const logEntryRpcBase*& logEntry)
	{
		do {
			if (m_offset == m_block->size)
			{
				if (m_block->next != nullptr)
				{
					barrier;
					if (m_offset == m_block->size)//rotate
						dsReturnIfFailed(rotate(m_block->next));
					continue;
				}
				else
				{
					logEntry = nullptr;
					dsOk();
				}
			}
			else
			{
				logEntry = (const logEntryRpcBase*)(m_block->data + m_offset);
				if (unlikely(logEntry->recordType == static_cast<uint8_t>(rpcType::endOfFile)))
				{
					if (m_block->next != nullptr || m_offset + logEntry->size != m_block->size)
						dsFailedAndLogIt(errorCode::illegalLogEntry, "find illegal endOfFile type logEntry in middle of log file:" << m_file->m_filePath, WARNING);
					dsFailed(errorCode::endOfFile, "");
				}
				m_offset += logEntry->size;
				dsOk();
			}
		} while (true);
	}
	DLL_EXPORT DS clusterLogFile::iterator::next(const logEntryRpcBase*& logEntry, long outTime)
	{
		do {
			dsReturnIfFailed(next(logEntry));
			if (likely(logEntry != nullptr))
				dsOk();
			if (outTime > 0)
			{
				m_file->wait();
				outTime -= 10;
			}
			else
				dsOk();
		} while (true);
	}
}
