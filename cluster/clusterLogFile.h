#pragma once
#include <glog/logging.h>
#include <stdlib.h>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <functional>

#include "thread/barrier.h"
#include "thread/yield.h"
#include "util/file.h"
#include "util/likely.h"
#include "rpc.h"
#include "errorCode.h"
#include "util/status.h"
#include "util/String.h"
#include "util/winDll.h"
#include "config.h"
namespace CLUSTER
{
#pragma pack(1)
	struct index
	{
		uint32_t offset;
		logIndexInfo logIndex;
		index(uint32_t offset, uint64_t term, uint64_t logIndex) :offset(offset), logIndex(term, logIndex) {}
		index(uint32_t offset, const logIndexInfo& logIndex) :offset(offset), logIndex(logIndex) {}
	};
#pragma pack()
	template<class T>
	class ref
	{
	public:
		enum class unuseResult {
			NORMAL,
			CLOSE,
			FREE
		};
	private:
		constexpr static int IN_CLEAR = 0x80000000;
		constexpr static int IN_LOAD = 0xc0000000;

		std::atomic<int> m_ref;
		T* m_data;
		std::function<dsStatus& (T*)>& loadFunc;
	public:
		ref(T* data, std::function<dsStatus& (T*)>& loadFunc) :m_ref(0), m_data(data), loadFunc(loadFunc) {}
		unuseResult unuse()
		{
			for (;;)
			{
				int r = m_ref.load(std::memory_order_relaxed);
				if (r == 1)
				{
					if (m_ref.compare_exchange_weak(r, IN_CLEAR))
					{
						m_data->close();
						m_ref.store(0, std::memory_order_release);
						return unuseResult::CLOSE;
					}
				}
				else
				{
					if (m_ref.compare_exchange_weak(r, r - 1))
					{
						if (r == 0)
						{
							delete m_data;
							return unuseResult::FREE;
						}
						return unuseResult::NORMAL;
					}
				}
			}
		}
		dsStatus& use()
		{
			for (;;)
			{
				int r = m_ref.load(std::memory_order_relaxed);
				if (unlikely(r == IN_CLEAR || r == IN_LOAD))
				{
					yield();
					continue;
				}
				if (r == 0)
				{
					if (m_ref.compare_exchange_weak(r, IN_LOAD))
					{
						dsStatus& s = loadFunc(m_data);
						if (!dsCheck(s))
						{
							m_ref.fetch_sub(std::memory_order_relaxed);
							dsReturn(s);
						}
						m_ref.store(1, std::memory_order_release);
						dsOk();
					}
				}
				else
				{
					assert(r > 0);
					if (m_ref.compare_exchange_weak(r, r + 1))
					{
						dsOk();
					}
				}
			}
		}
	};
	class clusterLogFile
	{
	private:
		constexpr static auto INDEX_NAME = ".index";
		constexpr static auto INDEX_READ_BATCH = 256;
		constexpr static auto HEAD_SIZE = sizeof(logIndexInfo) * 2;
		struct block
		{
			constexpr static uint32_t INVALID_RESULT = 0xffffffffu;
			index  idx;
			char* data;
			uint32_t id;
			uint32_t volumn;
			uint32_t size;
			uint32_t writedSize;
			block* next;
			ref<block> m_ref;
			block(std::function<dsStatus& (block*)>& loadFunc, uint32_t defaultBlockSize, uint32_t offset, const logEntryRpcBase* logEntry) :
				idx(offset, logEntry->logIndex),writedSize(0), next(nullptr), m_ref(this, loadFunc)
			{
				if (logEntry->size > defaultBlockSize)
					volumn = logEntry->size;
				else
					volumn = defaultBlockSize;
				data = new char[volumn];
				memcpy(data, logEntry, logEntry->size);
				size = logEntry->size;
			}
			block(std::function<dsStatus& (block*)>& loadFunc, const index& i) :
				idx(i.offset, i.logIndex), data(nullptr), volumn(0),
				size(0), writedSize(0), next(nullptr), m_ref(this, loadFunc)
			{
			}
			block(std::function<dsStatus& (block*)>& loadFunc,const index& i,char * data, uint32_t volumn,uint32_t size) :
				idx(i.offset, i.logIndex), data(data), volumn(volumn),
				size(size), writedSize(size), next(nullptr), m_ref(this, loadFunc)
			{
			}
			~block()
			{
				if (data != nullptr)
					delete[] data;
			}
			void close()
			{
				delete[] data;
				data = nullptr;
			}
			uint32_t find(const logIndexInfo& logIndex)
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
			inline void append(const logEntryRpcBase* logEntry)
			{
				memcpy(data + size, logEntry, logEntry->size);
				wmb();
				size += logEntry->size;
			}
		};
	private:
		std::string m_filePath;
		uint64_t m_fileId;
		int32_t m_offset;
		uint32_t m_blockCount;
		logIndexInfo m_beginLogIndex;
		logIndexInfo m_logIndex;
		block** m_blocks;
		block* m_currentBlock;
		logConfig m_config;
		uint32_t m_maxBlockCount;
		clusterLogFile* m_next;
		std::mutex m_lock;
		std::function<dsStatus& (block*)> m_blockLoadFunc;
		std::function<dsStatus& (clusterLogFile*)> m_fileLoadFunc;
		ref<clusterLogFile> m_ref;
		fileHandle m_fd;
		fileHandle m_indexFd;
		fileHandle m_readFd;
	public:
		DLL_EXPORT clusterLogFile(const char* filePath, uint64_t fileId, logConfig& config) :m_filePath(filePath), m_fileId(fileId),
			m_blockCount(0), m_blocks(nullptr), m_currentBlock(nullptr),
			m_config(config), m_maxBlockCount(m_config.m_defaultLogFileSize / m_config.m_defaultBlockSize),
			m_next(nullptr),
			m_blockLoadFunc(std::bind(&clusterLogFile::loadBlock, this, std::placeholders::_1)),
			m_fileLoadFunc(std::bind(&clusterLogFile::loadFile, this, std::placeholders::_1)),
			m_ref(this, m_fileLoadFunc),
			m_fd(INVALID_HANDLE_VALUE), m_indexFd(INVALID_HANDLE_VALUE), m_readFd(INVALID_HANDLE_VALUE)
		{
		}
		DLL_EXPORT ~clusterLogFile()
		{
			close();
		}
		DLL_EXPORT uint64_t getFileId()
		{
			return m_fileId;
		}
		DLL_EXPORT const logIndexInfo& getBeginLogIndex()
		{
			return m_beginLogIndex;
		}
		DLL_EXPORT const logIndexInfo& getLogIndex()
		{
			return m_logIndex;
		}
		DLL_EXPORT clusterLogFile* getNext()
		{
			return m_next;
		}
		DLL_EXPORT void setNext(clusterLogFile* next)
		{
			m_next = next;
		}
		DLL_EXPORT dsStatus& readRecordHead(fileHandle fd,uint32_t dataFileSize,uint32_t pos, logEntryRpcBase &head, bool readOnly)
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
		DLL_EXPORT dsStatus& recoveryIndexFromLastBlock(fileHandle fd,uint32_t offset, uint32_t dataFileSize,bool readOnly)
		{
			if (seekFile(fd, offset, SEEK_SET) != offset)
				dsFailedAndLogIt(errorCode::ioError, "seek to " << offset << " of log file:" << m_filePath << " failed,error:" << errno << "," << strerror(errno), ERROR);
			if (seekFile(m_indexFd, sizeof(index)*m_blockCount, SEEK_SET) != (int64_t)(sizeof(index)*m_blockCount))
				dsFailedAndLogIt(errorCode::ioError, "seek to " << offset << " of log file:" << (m_filePath + INDEX_NAME) << " failed,error:" << errno << "," << strerror(errno), ERROR);
			char* buffer;
			uint32_t bufferSize = 0;
			for (int64_t pos = offset; pos < dataFileSize;)
			{
				logEntryRpcBase head;
				dsStatus &rtv = readRecordHead(fd,dataFileSize, pos, head, readOnly);
				if (!dsCheck(rtv))
				{
					if (rtv.code == errorCode::endOfFile)
						break;
					else
						dsReturn(rtv);
				}
				if (head.size > m_config.m_defaultBlockSize)
					buffer = new char[bufferSize = head.size];
				else
					buffer = new char[bufferSize = m_config.m_defaultBlockSize];
				index idx(pos, head.logIndex);
				bool readHead = false;
				int off = 0;
				while (true)
				{
					if (readHead)
					{
						dsStatus& rtv = readRecordHead(fd,dataFileSize, pos, head, readOnly);
						if (!dsCheck(rtv))
						{
							if (rtv.code == errorCode::endOfFile)
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
						memcpy(buffer + off , &head, sizeof(head));
						uint32_t readSize = ((logEntryRpcBase*)buffer + off)->size - sizeof(logEntryRpcBase);
						if(readSize!= readFile(fd, buffer + off +sizeof(raftRpcHead), readSize))
							dsFailedAndLogIt(errorCode::ioError, "read record head from file:" << m_filePath << " failed,error:" << errno << "," << strerror(errno), ERROR);
						readHead = true;
						off += head.size;
						if ((pos += readSize) == dataFileSize)//end of file
							break;
					}
				}
				m_blocks[m_blockCount++] = new block(m_blockLoadFunc, idx, buffer, bufferSize, off);
				if (sizeof(idx) != writeFile(m_indexFd, (char*)&idx, sizeof(idx)))
					dsFailedAndLogIt(errorCode::ioError, "write index info to:" << (m_filePath + INDEX_NAME) << " failed,error:" << errno << "," << strerror(errno), ERROR);
			}
			dsOk();
		}
		DLL_EXPORT dsStatus& loadIndex(fileHandle fd,bool readOnly)
		{
			int64_t dataFileSize = getFileSize(fd);
			if (dataFileSize < 0)
				dsFailedAndLogIt(errorCode::ioError, "get size of file:" << m_filePath << "failed,error:" << errno << "," << strerror(errno), ERROR);
			int64_t indexFileSize;
			int64_t readSize;
			int indexCount;
			char buf[sizeof(index) * INDEX_READ_BATCH];
			if(m_indexFd == INVALID_HANDLE_VALUE)
			{
				if ((m_indexFd = openFile((m_filePath + INDEX_NAME).c_str(), true, true, false)) == INVALID_HANDLE_VALUE)
				{
					int err = errno;
					if (checkFileExist((m_filePath + INDEX_NAME).c_str(), F_OK) == 0)
						dsFailedAndLogIt(errorCode::ioError, "open file :" << (m_filePath + INDEX_NAME) << " failed for " << err << "," << strerror(err), ERROR);
					if ((m_indexFd = openFile((m_filePath + INDEX_NAME).c_str(), true, true, true)) == INVALID_HANDLE_VALUE)
						dsFailedAndLogIt(errorCode::ioError, "create file :" << (m_filePath + INDEX_NAME) << " failed for " << errno << "," << strerror(errno), ERROR);
					goto CHECK_LAST_BLOCK;
				}
			}
			else
			{
				if(0 != seekFile(m_indexFd, 0, SEEK_SET))
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
					m_blocks[readed + i] = new block(m_blockLoadFunc,*(index*)(&buf[sizeof(index) * i]));
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
				if (dataFileSize - m_blocks[m_blockCount - 1]->idx.offset < m_config.m_defaultBlockSize)
					dsOk();
				else
					dsReturn(recoveryIndexFromLastBlock(fd,m_blocks[m_blockCount - 1]->idx.offset, dataFileSize, readOnly));
			}
			else
			{
				if (dataFileSize >= (int64_t)HEAD_SIZE)
					dsReturn(recoveryIndexFromLastBlock(fd,HEAD_SIZE, dataFileSize, readOnly));
				else
					dsFailedAndLogIt(errorCode::emptyLogFile, "size of " << m_filePath << " is " << dataFileSize, WARNING);
			}
		}
		DLL_EXPORT dsStatus& recovery()
		{
			m_fd = openFile(m_filePath.c_str(), true, true, false);
			if (m_fd == INVALID_HANDLE_VALUE)
			{
				int err = errno;
				if (checkFileExist(m_filePath.c_str(), F_OK) != 0)
					dsFailedAndLogIt(errorCode::ioError, m_filePath << " not exist", ERROR);
				else
					dsFailedAndLogIt(errorCode::ioError, "open file :" << m_filePath << " failed for " << err << "," << strerror(err), ERROR);
			}
			if (m_blocks == nullptr)
			{
				m_blocks = new block * [m_maxBlockCount];
				memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
			}
			dsReturn(loadIndex(m_fd, false));
		}
		DLL_EXPORT dsStatus& load()
		{
			m_readFd = openFile(m_filePath.c_str(), true, false, false);
			if (m_readFd == INVALID_HANDLE_VALUE)
			{
				int err = errno;
				if (checkFileExist(m_filePath.c_str(), F_OK) != 0)
					dsFailedAndLogIt(errorCode::ioError, m_filePath << " not exist", ERROR);
				else
					dsFailedAndLogIt(errorCode::ioError, "open file :" << m_filePath << " failed for " << err << "," << strerror(err), ERROR);
			}
			if (m_blocks == nullptr)
			{
				m_blocks = new block * [m_maxBlockCount];
				memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
			}
			dsReturn(loadIndex(m_readFd, true));
		}
		DLL_EXPORT dsStatus& loadFile(clusterLogFile* logFile)
		{
			dsReturn(logFile->load());
		}
		DLL_EXPORT dsStatus& writeCurrentBlock()
		{
			if (writeFile(m_fd, m_currentBlock->data + m_currentBlock->writedSize, m_currentBlock->size - m_currentBlock->writedSize) !=
				(m_currentBlock->size - m_currentBlock->writedSize) || ::fsync(m_fd) != 0)
			{
				dsFailedAndLogIt(errorCode::ioError, "write cluster log " << m_filePath << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			m_offset += m_currentBlock->size - m_currentBlock->writedSize;
			m_currentBlock->writedSize = m_currentBlock->size;
			dsOk();
		}
		inline dsStatus& _append(const logEntryRpcBase* logEntry)
		{
			if (unlikely(m_currentBlock == nullptr))
			{
				dsReturnIfFailed(appendToNewBlock(logEntry));
			}

			if (unlikely(m_currentBlock->size + logEntry->size +sizeof(raftRpcHead) > m_currentBlock->volumn))
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
			dsOk();
		}
		DLL_EXPORT dsStatus& appendToNewBlock(const logEntryRpcBase* logEntry)
		{
			//flush current block
			if (m_currentBlock != nullptr)
				dsReturnIfFailed(writeCurrentBlock());
			block* newBlock = new block(m_blockLoadFunc, m_config.m_defaultBlockSize, m_offset, logEntry);

			if(m_indexFd == INVALID_HANDLE_VALUE)
			{
				if ((m_indexFd = openFile((m_filePath + INDEX_NAME).c_str(), true, true, true)) == INVALID_HANDLE_VALUE)
					dsFailedAndLogIt(errorCode::ioError, "open index file:" << m_filePath << ".idx failed for:" << errno << "," << strerror(errno), ERROR);
				if (seekFile(m_indexFd, sizeof(index)*m_blockCount, SEEK_SET) != (int64_t)(sizeof(index)*m_blockCount))
					dsFailedAndLogIt(errorCode::ioError, "seek index file:" << m_filePath << ".idx to " << sizeof(index)*m_blockCount << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			if (writeFile(m_indexFd, (char*)&newBlock->idx, sizeof(newBlock->idx)) != sizeof(newBlock->idx) || ::fsync(m_indexFd) != 0)
			{
				int err = errno;
				truncateFile(m_indexFd, sizeof(newBlock->idx) * m_blockCount);
				dsFailedAndLogIt(errorCode::ioError, "write index file:" << m_filePath << ".idx failed for:" << err << "," << strerror(err), ERROR);
			}
			wmb();
			m_blocks[m_blockCount] = newBlock;
			m_blockCount++;
			if (m_currentBlock != nullptr)
				m_currentBlock->next = newBlock;
			m_currentBlock = newBlock;
			dsOk();
		}
		DLL_EXPORT block* findBlock(const logIndexInfo& logIndex)
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

			if ((m_blocks[s]->idx.logIndex < logIndex)
				&& blockEndLogIndex > logIndex)
				return m_blocks[s];
			else
				return nullptr;
		}
		DLL_EXPORT dsStatus& loadBlock(block* b)
		{
			if (b->data != nullptr)
				dsOk();
			int size = (b->next == nullptr ? m_offset : b->next->idx.offset) - b->idx.offset;
			char* data = new char[size];
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
			if (size != readFile(m_readFd, data, size))
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
	public:
		DLL_EXPORT dsStatus& open()
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

		DLL_EXPORT dsStatus& create(clusterLogFile* prev, const logIndexInfo& beginLogIndex)
		{
			dsReturnIfFailed(create(prev->m_logIndex, beginLogIndex));
			prev->m_next = this;
			dsOk();
		}

		DLL_EXPORT dsStatus& create(const logIndexInfo& prevLogIndex, const logIndexInfo& beginLogIndex)
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
			m_logIndex = prevLogIndex;
			m_beginLogIndex = beginLogIndex;
			m_blocks = new block * [m_maxBlockCount];
			memset(m_blocks, 0, sizeof(block*) * m_maxBlockCount);
			dsOk();
		}
		DLL_EXPORT dsStatus& deleteFile()
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
		DLL_EXPORT void close()
		{
			LOG(ERROR)<<"close log file";
			if (m_fd != INVALID_HANDLE_VALUE)
			{
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
					delete m_blocks[i];
					m_blocks[i] = nullptr;
				}
				delete[]m_blocks;
				m_blocks = nullptr;
			}
			m_next = nullptr;
			m_offset = 0;
			m_blockCount = 0;
		}
		DLL_EXPORT dsStatus& finish()
		{
			raftRpcHead head = { sizeof(raftRpcHead),static_cast<uint8_t>(rpcType::endOfFile),VERSION };
			memcpy(m_currentBlock->data + m_currentBlock->size, &head,sizeof(head));
			wmb();
			m_currentBlock->size += sizeof(head);
			dsReturn(flush());
		}

		DLL_EXPORT dsStatus& append(const logEntryRpcBase* logEntry)
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
			if (unlikely(m_offset + logEntry->size >= m_config.m_defaultLogFileSize))
				dsFailed(errorCode::full, nullptr);
			dsReturn(_append(logEntry));
		}
		DLL_EXPORT dsStatus& clear()
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

		DLL_EXPORT dsStatus& rollback(const logIndexInfo& logIndex)
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
		DLL_EXPORT dsStatus& flush()
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
		DLL_EXPORT inline dsStatus& use()
		{
			dsReturn(m_ref.use());
		}
		DLL_EXPORT inline ref<clusterLogFile>::unuseResult unUse()
		{
			return m_ref.unuse();
		}
		class iterator {
		private:
			logIndexInfo m_logIndex;
			uint32_t m_offset;
			clusterLogFile* m_file;
			block* m_block;
			bool m_onlyReadCommitted;
			DLL_EXPORT dsStatus& rotate(block* b)
			{
				dsReturnIfFailed(b->m_ref.use());
				if (m_block != nullptr)
					m_block->m_ref.unuse();
				m_block = b;
				m_offset = 0;
				dsOk();
			}
		public:
			DLL_EXPORT iterator() : m_offset(0), m_file(nullptr), m_block(nullptr) {}
			DLL_EXPORT ~iterator()
			{
				if (m_block != nullptr)
					m_block->m_ref.unuse();
			}
			DLL_EXPORT dsStatus& setLogFile(clusterLogFile* file)
			{
				dsReturn(file->m_ref.use());
			}
			DLL_EXPORT dsStatus& attachToNextLogFile(clusterLogFile* file)
			{
				assert(file->m_blockCount > 0 && file->m_blocks[0] != nullptr);
				m_file = file;
				dsReturn(rotate(m_file->m_blocks[0]));
			}
			DLL_EXPORT dsStatus& search(const logIndexInfo& logIndex)
			{
				if (nullptr == (m_block = m_file->findBlock(logIndex)))
				{
					dsFailedAndLogIt(errorCode::logIndexNotFound, "do not find logIndex:" << logIndex.term << "." << logIndex.logIndex, WARNING);
				}
				dsReturnIfFailed(m_file->m_ref.use());
				if ((m_offset = m_block->find(logIndex)) == block::INVALID_RESULT)
				{
					m_block->m_ref.unuse();
					dsFailedAndLogIt(errorCode::logIndexNotFound, "do not find logIndex:" << logIndex.term << "." << logIndex.logIndex, WARNING);
				}
				m_logIndex = logIndex;
				dsOk();
			}

			DLL_EXPORT dsStatus& next(const logEntryRpcBase*& logEntry)
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
				} while (1);
			}
		};
	};
}
