#pragma once
#include <string>
#include <stdint.h>
#include <errno.h>
#include <glog/logging.h>
#include "memory/simpleMemCache.h"
#include "util/sparsepp/spp.h"
#include "util/file.h"
#include "util/fileList.h"
#include "util/String.h"
#include "lz4/lib/lz4.h"
#include "localLogFileCache/logEntry.h"
namespace DATA_SOURCE
{
	constexpr static int MAX_RECORD_LIST_SIZE = 512;
	constexpr static int MAX_RECORD_LIST_MEM_USE = 512 * 1024;
	constexpr static int DEFAUL_DATA_BUF_SIZE = 512 * 1024;
	constexpr static auto TXN_FILE_NAME = ".txn";
	constexpr static auto TXN_TRANS_INDEX_NAME = ".idx";

	struct TransId
	{
		TransId() {}
		TransId(const TransId &tid) {}
		virtual TransId& operator=(const TransId& t) = 0;
		virtual size_t hash() const = 0;
		virtual bool compare(const TransId& t) const = 0;
		virtual std::string toString() const = 0;
	};

	struct ULongTid :public TransId
	{
		uint64_t tid;
		ULongTid(uint64_t tid) :tid(tid) {}
		ULongTid(const TransId& t) :tid(static_cast<const ULongTid&>(t).tid)
		{}
		ULongTid& operator=(const TransId& t)
		{
			tid = static_cast<const ULongTid&>(t).tid;
			return *this;
		}
		inline size_t hash() const
		{
			return tid;
		}
		inline bool compare(const TransId& t) const
		{
			return tid == static_cast<const ULongTid&>(t).tid;
		}
		inline std::string toString() const
		{
			String s;
			s.append(tid);
			return s;
		}
	};

	struct StrTid :public TransId
	{
		char* tid;
		StrTid() :tid(nullptr) {}
		StrTid(const TransId& t)
		{
			const char* ttid = static_cast<const StrTid&>(t).tid;
			if (ttid == nullptr)
				return;
			uint16_t size = strlen(ttid);
			tid = new char[size + 1];
			memcpy(tid, ttid, size);
			tid[size] = '\0';
		}
		~StrTid()
		{
			if (tid != nullptr)
				delete[]tid;
		}
		StrTid& operator=(const TransId& t)
		{
			if (tid != nullptr)
				delete[]tid;
			const char* ttid = static_cast<const StrTid&>(t).tid;
			if (ttid != nullptr)
			{
				uint16_t size = strlen(ttid);
				tid = new char[size + 1];
				memcpy(tid, ttid, size);
				tid[size] = '\0';
			}
			else
			{
				tid = nullptr;
			}
			return *this;
		}
		inline size_t hash() const
		{
			return _hash(tid);
		}
		inline bool compare(const TransId& t) const
		{
			return strcmp(tid, static_cast<const StrTid&>(t).tid);
		}
		inline std::string toString() const
		{
			return tid;
		}
	};

	class TidMemBufImp
	{
	public:
		virtual TransId* alloc(const TransId& tid) = 0;
		virtual void free(TransId* t) = 0;
	};

	template<typename T>
	class TidMemBuf :public TidMemBufImp
	{
	private:
		SimpleMemCache<T> cache;
	public:
		inline TransId* alloc(const TransId& tid)
		{
			return cache.alloc(tid);
		}
		inline void free(TransId* tid)
		{
			return cache.free(tid);
		}
	};

	struct DataBuf
	{
		char* m_dataBuf;
		uint32_t dataBufSize;
		char* compressBuf;
		uint32_t compressBufSize;
		DataBuf(uint32_t dataBufSize = MAX_RECORD_LIST_SIZE) :dataBufSize(dataBufSize), compressBufSize(LZ4_COMPRESSBOUND(dataBufSize))
		{
			m_dataBuf = new char[dataBufSize];
			compressBuf = new char[compressBufSize];
		}
		~DataBuf()
		{
			delete[]m_dataBuf;
			delete[]compressBuf;
		}
	};

	struct RecordList
	{
		char* buf;
		LogEntry* records[MAX_RECORD_LIST_SIZE];
		uint16_t recordCount;
		RecordList* next;
		uint32_t memUsed;
		RecordList() :buf(nullptr), recordCount(0), next(nullptr), memUsed(0)
		{
		}

		~RecordList()
		{
			if (buf != nullptr)
				free(buf);
		}

		inline bool canAdd(LogEntry* e)
		{
			return recordCount + 1 <= MAX_RECORD_LIST_SIZE && (memUsed == 0 || memUsed + e->size <= MAX_RECORD_LIST_MEM_USE);
		}

		inline void add(LogEntry* e)
		{
			records[recordCount++] = e;
			memUsed += e->size;
		}

		void clearBuf(DataBuf& m_dataBuf)
		{
			if (buf != nullptr)
			{
				if (buf != m_dataBuf.m_dataBuf)
					free(buf);
				buf = nullptr;
			}
		}

		DS flush(fileHandle fd, DataBuf& m_dataBuf)
		{
			char* realDataBuf;
			if (m_dataBuf.dataBufSize >= memUsed)
				realDataBuf = m_dataBuf.m_dataBuf;
			else
				realDataBuf = (char*)malloc(memUsed);
			char* pos = realDataBuf;
			for (uint16_t i = 0; i < recordCount; i++)
			{
				memcpy(pos, records[i], records[i]->size);
				pos += records[i]->size;
			}

			char* realCompressBuf;
			uint32_t compressBufNeededSize = LZ4_COMPRESSBOUND(memUsed);
			if (compressBufNeededSize > m_dataBuf.compressBufSize)
				realCompressBuf = (char*)malloc(compressBufNeededSize);
			else
				realCompressBuf = m_dataBuf.compressBuf;
			int writeDataSize = LZ4_compress_default(realDataBuf, realCompressBuf, memUsed, compressBufNeededSize);
			if (sizeof(memUsed) != writeFile(fd, (char*)&memUsed, sizeof(memUsed))
				|| sizeof(writeDataSize) != writeFile(fd, (char*)&writeDataSize, sizeof(writeDataSize))
				|| writeDataSize != writeFile(fd, realDataBuf, writeDataSize))
			{
				if (realDataBuf != m_dataBuf.m_dataBuf)
					free(realDataBuf);
				if (realCompressBuf != m_dataBuf.compressBuf)
					free(realCompressBuf);
				dsFailedAndLogIt(1, "write records to file failed for:" << errno << "," << strerror(errno), ERROR);
			}
			if (realDataBuf != m_dataBuf.m_dataBuf)
				free(realDataBuf);
			if (realCompressBuf != m_dataBuf.compressBuf)
				free(realCompressBuf);
			dsOk();
		}

		DS load(fileHandle fd, DataBuf& m_dataBuf)
		{
			clearBuf(m_dataBuf);
			int writeDataSize = 0;
			if (sizeof(memUsed) != readFile(fd, (char*)&memUsed, sizeof(memUsed))
				|| sizeof(writeDataSize) != readFile(fd, (char*)&writeDataSize, sizeof(writeDataSize)))
			{
				dsFailedAndLogIt(1, "read records to file failed for:" << errno << "," << strerror(errno), ERROR);
			}
			char* realCompressBuf;
			if (writeDataSize > m_dataBuf.compressBufSize)
				realCompressBuf = (char*)malloc(writeDataSize);
			else
				realCompressBuf = m_dataBuf.compressBuf;

			if (writeDataSize != readFile(fd, m_dataBuf.compressBuf, writeDataSize))
			{
				if (realCompressBuf != m_dataBuf.compressBuf)
					free(realCompressBuf);
				dsFailedAndLogIt(1, "read records to file failed for:" << errno << "," << strerror(errno), ERROR);
			}
			if (memUsed <= m_dataBuf.dataBufSize)
				buf = m_dataBuf.m_dataBuf;
			else
				buf = (char*)malloc(memUsed);
			if (LZ4_decompress_safe(buf, realCompressBuf, writeDataSize, memUsed) < 0)
			{
				if (realCompressBuf != m_dataBuf.compressBuf)
					free(realCompressBuf);
				free(buf);
				buf = nullptr;
				dsFailedAndLogIt(1, "call LZ4_decompress_safe failed, data is illegal", ERROR);
			}
			if (realCompressBuf != m_dataBuf.compressBuf)
				free(realCompressBuf);
			recordCount = 0;
			char* pos = buf;
			while (pos - buf < memUsed)
			{
				records[recordCount++] = (LogEntry*)pos;
				pos += ((LogEntry*)pos)->size;
			}
			dsOk();
		}
	};

	class Transaction
	{
	private:
		TransId* tid;
		RPC::Checkpoint startCheckpoint;
		RecordList* recordListHead;
		RecordList* recordListEnd;

		Transaction* prev;
		Transaction* next;

		uint64_t flushedSize;
		uint64_t lastSeqNo;
		uint64_t memUsed;
	public:
		Transaction(TransId* tid, LogEntry* entry) : tid(tid), recordListHead(nullptr), recordListEnd(nullptr), next(nullptr)
		{
			memcpy(&startCheckpoint, entry->getCheckpoint(), sizeof(startCheckpoint) - 1);
		}

		~Transaction()
		{
		}



		inline void clearMem(SimpleMemCache<RecordList>& recordListCache, TidMemBufImp* transIdCache)
		{
			if (tid != nullptr)
			{
				transIdCache->free(tid);
				tid = nullptr;
			}
			while (recordListHead != nullptr)
			{
				RecordList* n = recordListHead->next;
				recordListCache.free(recordListHead);
				recordListHead = n;
			}
			recordListEnd = nullptr;
		}

		void clear(SimpleMemCache<RecordList>& recordListCache, TidMemBufImp* transIdCache, std::string& cacheFileDir)
		{
			if (flushedSize > 0)
			{
				String file(cacheFileDir);
				file.append("/").append(tid->toString()).append(TXN_FILE_NAME);
				if (flushedSize == 0)
				{
					if (checkFileExist(file.c_str(), F_OK) == 0)
					{
						if (0 != remove(file.c_str()) || checkFileExist(file.c_str(), F_OK) == 0)
							LOG(WARNING) << "remove exist transaction file " << file << " failed for " << errno << "," << strerror(errno);
					}
				}
			}
			clearMem(recordListCache, transIdCache);
		}

		inline Transaction* getNext()
		{
			return next;
		}

		inline Transaction* getPrev()
		{
			return prev;
		}

		inline RPC::Checkpoint& getStartCheckpoint()
		{
			return startCheckpoint;
		}

		inline void addAfter(Transaction* t)
		{
			t->next = this;
			this->prev = t;
		}

		inline uint64_t getRealMemUse()
		{
			return memUsed - flushedSize;
		}

		inline uint64_t getMemUse()
		{
			return memUsed;
		}

		inline uint64_t getLastSeqNo()
		{
			return lastSeqNo;
		}

		inline void dropFromChain()
		{
			if (next != nullptr)
				next->prev = prev;
			if (prev != nullptr)
				prev->next = next;
		}

		void add(SimpleMemCache<RecordList>& recordListCache, LogEntry* e)
		{
			if (unlikely(recordListEnd == nullptr))
			{
				recordListHead = recordListEnd = recordListCache.alloc();
			}
			else
			{
				if (unlikely(e->getCheckpoint()->seqNo.seqNo >= lastSeqNo))
					return;
				if (!recordListEnd->canAdd(e))
				{
					RecordList* l = recordListCache.alloc();
					recordListEnd->next = l;
					recordListEnd = l;
				}
			}
			recordListEnd->add(e);
			memUsed += e->size;
			lastSeqNo = e->getCheckpoint()->seqNo.seqNo;
		}

		DS flush(SimpleMemCache<RecordList>& recordListCache, std::string& cacheFileDir, DataBuf& m_dataBuf)
		{
			fileHandle fd;
			String file(cacheFileDir);
			file.append("/").append(tid->toString()).append(TXN_FILE_NAME);
			if (flushedSize == 0)
			{
				if (checkFileExist(file.c_str(), F_OK) == 0)
				{
					if (0 != remove(file.c_str()) || checkFileExist(file.c_str(), F_OK) == 0)
						dsFailedAndLogIt(1, "remove exist transaction file " << file << " failed for " << errno << "," << strerror(errno), ERROR);
				}
			}
			if (INVALID_HANDLE_VALUE == (fd = openFile(file.c_str(), true, true, true)))
				dsFailedAndLogIt(1, "open file " << file << " failed for:" << errno << "," << strerror(errno), ERROR);
			int64_t startPos = seekFile(fd, 0, SEEK_END);
			if (startPos < 0)
			{
				closeFile(fd);
				fd = INVALID_HANDLE_VALUE;
				dsFailedAndLogIt(1, "seek to end of  file " << file << " failed for:" << errno << "," << strerror(errno), ERROR);
			}
			RecordList* l = recordListHead;
			while (l != nullptr)
			{
				dsReturnIfFailedWithOp(l->flush(fd, m_dataBuf), do {
					truncateFile(fd, startPos); closeFile(fd); fd = INVALID_HANDLE_VALUE;
				} while (0));
				l = l->next;
			}
			flushedSize = memUsed;
			closeFile(fd);
			fd = INVALID_HANDLE_VALUE;
			while (recordListHead != nullptr)
			{
				RecordList* n = recordListHead->next;
				recordListCache.free(recordListHead);
				recordListHead = n;
			}
			recordListEnd = nullptr;
			dsOk();
		}
#define CLEAR_TRANS_LOAD_INFO  do { closeFile(fd); if(recordList!=nullptr){recordListMemCache.free(recordList); recordList = nullptr;} if (t != nullptr) { delete t; t = nullptr; } } while (0)
		static DS loadTransAndCheck(TransId* tid, DataBuf& m_dataBuf, SimpleMemCache<RecordList>& recordListMemCache, const std::string& fileName,
			Transaction*& t, uint64_t beginSeqNo, uint64_t endSeqNo)
		{
			t = nullptr;
			fileHandle fd;
			if (INVALID_HANDLE_VALUE == (fd = openFile(fileName.c_str(), true, true, true)))
				dsFailedAndLogIt(1, "open file " << fileName << " failed for:" << errno << "," << strerror(errno), ERROR);
			int64_t fileSize = getFileSize(fd);
			if (fileSize < 0)
			{
				int errNo = errno;
				closeFile(fd);
				dsFailedAndLogIt(1, "get size of file " << fileName << " failed for:" << errNo << "," << strerror(errNo), ERROR);
			}
			if (fileSize <= 8)
			{
				closeFile(fd);
				dsFailedAndLogIt(1, "size " << fileSize << " of file " << fileName << " is less than size of RecordList head", ERROR);
			}
			RecordList* recordList = nullptr;
			while (true)
			{
				int64_t offset = seekFile(fd, 0, SEEK_CUR);
				if (offset < 0)
				{
					int errNo = errno;
					CLEAR_TRANS_LOAD_INFO;
					dsFailedAndLogIt(1, "get offset of file " << fileName << " failed for:" << errNo << "," << strerror(errNo), ERROR);
				}
				if (offset == fileSize)
					break;
				recordList = recordListMemCache.alloc();
				if (!dsCheck(recordList->load(fd, m_dataBuf)))
				{
					if (t->lastSeqNo >= endSeqNo)
					{
						LOG(WARNING) << "read record list from "<< fileName<<" failed, but we have read to end seqNo "<<endSeqNo<<", truncate this file";
						getLocalStatus().clear();
						if (0 != truncateFile(fd, offset)) 
						{
							int errNo = errno;
							CLEAR_TRANS_LOAD_INFO;
							dsFailedAndLogIt(1, "truncate file " << fileName << " failed for:" << errNo << "," << strerror(errNo), ERROR);
						}
						recordListMemCache.free(recordList);
						break;
					}
					else
					{
						CLEAR_TRANS_LOAD_INFO;
						dsReturn(getLocalStatus().code);
					}
				}
				dsReturnIfFailedWithOp(recordList->load(fd, m_dataBuf), CLEAR_TRANS_LOAD_INFO);
				if (recordList->memUsed < sizeof(LogEntry))
				{
					CLEAR_TRANS_LOAD_INFO;
					dsFailedAndLogIt(1, "invald record list in trans " << tid->toString(), ERROR);
				}
				if (t == nullptr)
					t = new Transaction(tid, (LogEntry*)recordList->buf);
				t->memUsed += recordList->memUsed;
				t->flushedSize += recordList->memUsed;
				const char* pos = recordList->buf;
				while (pos < recordList->buf + recordList->memUsed)
				{
					t->lastSeqNo = ((const LogEntry*)pos)->getCheckpoint()->seqNo.seqNo;
					pos += ((const LogEntry*)pos)->size;
				}
				recordListMemCache.free(recordList);
				recordList = nullptr;
			}
			closeFile(fd);
			if (t == nullptr || t->lastSeqNo < endSeqNo)
			{
				uint64_t lastSeqNo = (t == nullptr ? 0 : t->lastSeqNo);
				if (t != nullptr)
					delete t;
				dsFailedAndLogIt(1, "last seqNo of " << tid->toString() << " is " << lastSeqNo << ", less than trans end seqNo " << endSeqNo << " in index file", ERROR);
			}
			dsOk();
		}

	public:
		class Iterator
		{
		private:
			Transaction* t;
			uint64_t offset;
			uint64_t recordOffset;
			RecordList* recordList;
			bool listLoadFromFile;
			SimpleMemCache<RecordList>& recordListMemCache;
			fileHandle fd;
			DataBuf& m_dataBuf;
		public:
			Iterator(Transaction* t, SimpleMemCache<RecordList>& recordListMemCache, DataBuf& m_dataBuf) :t(t), offset(0), recordOffset(0), recordList(nullptr),
				listLoadFromFile(false), recordListMemCache(recordListMemCache), fd(INVALID_HANDLE_VALUE), m_dataBuf(m_dataBuf) {}
			~Iterator()
			{
				if (fd != INVALID_HANDLE_VALUE)
					closeFile(fd);
				if (recordList != nullptr && listLoadFromFile)
				{
					recordList->clearBuf(m_dataBuf);
					recordListMemCache.free(recordList);
				}
			}

			DS init(std::string& cacheFileDir)
			{
				if (t->flushedSize > 0)
				{
					String file(cacheFileDir);
					file.append("/").append(t->tid->toString()).append(TXN_FILE_NAME);
					if (INVALID_HANDLE_VALUE == (fd = openFile(file.c_str(), true, false, false)))
						dsFailedAndLogIt(1, "failed to open transaction file:" << file << ", error:" << errno << "," << strerror(errno), ERROR);
					recordList = recordListMemCache.alloc();
					dsReturnIfFailedWithOp(recordList->load(fd, m_dataBuf), do { recordListMemCache.free(recordList); recordList = nullptr; } while (0));
					listLoadFromFile = true;
					offset = recordList->memUsed;
				}
				else
				{
					recordList = t->recordListHead;
				}
				dsOk();
			}

			DS attachToNext()
			{
				if (listLoadFromFile)
				{
					if (offset == t->flushedSize)
					{
						recordList->clearBuf(m_dataBuf);
						recordListMemCache.free(recordList);
						if (nullptr == (recordList = t->recordListHead))
							dsReturnCode(1);
						listLoadFromFile = false;
					}
					else
					{
						dsReturnIfFailedWithOp(recordList->load(fd, m_dataBuf), do { recordListMemCache.free(recordList); recordList = nullptr; } while (0));
					}
				}
				else
				{
					if (recordList->next == nullptr)
						dsReturnCode(1);
					recordList->clearBuf(m_dataBuf);
					recordList = recordList->next;
				}
				offset += recordList->memUsed;
				recordOffset = 0;
			}

			inline DS next(LogEntry*& e)
			{
				if (recordList->memUsed == recordOffset)
				{
					DS s = attachToNext();
					if (s == 1)
						dsReturnCode(s);
					else if (!dsCheck(s))
						dsReturn(s);
				}
				e = (LogEntry*)(recordList->buf + recordOffset);
				recordOffset += e->size;
				dsOk();
			}
		};
	};

	struct TransIndexInfo {
		TransId* tid;
		uint64_t beginSeqNo;
		uint64_t lastSeqNo;
		TransIndexInfo() :tid(nullptr), beginSeqNo(0), lastSeqNo(0) {}
		TransIndexInfo(const TransIndexInfo& tii) :tid(tii.tid), beginSeqNo(tii.beginSeqNo), lastSeqNo(tii.lastSeqNo) {}
		TransIndexInfo& operator=(const TransIndexInfo& tii) {
			tid = tii.tid; 
			beginSeqNo = tii.beginSeqNo; 
			lastSeqNo = tii.lastSeqNo;
			return *this;
		}
		bool appendToBuf(char*& str, uint32_t size)
		{
			std::string s = tid->toString();
			if (s.size() + 64 > size)
				return false;
			memcpy(str, s.c_str(), s.size());
			str += s.size();
			*str = ':';
			str++;
			str += u64toa_sse2(beginSeqNo, str);
			*str++ = '-';
			str += u64toa_sse2(lastSeqNo, str);
			*str = '\n';
			str++;
			return true;
		}
		DS load(const char* buf, std::function<DS(const char*, TransId*&)> loadTransIdFunc)
		{
			String _s(buf);
			String s = _s.trim();
			size_t size = strlen(buf);
			if (s.size() == 0)
				dsOk();
			int32_t tidOffset = size - 1;
			while (tidOffset > 0 && buf[tidOffset] == ':')
				tidOffset--;
			if (tidOffset <= 0)
				dsFailedAndLogIt(1, "invalid trans info " << buf, ERROR);
			String line(buf + tidOffset);
			std::vector<String> fields = line.split("-");
			if (fields.size() != 2)
				dsFailedAndLogIt(1, "invalid trans info " << buf, ERROR);
			String tidRaw(buf, tidOffset);
			if(!fields.at(0).trim().getInt(beginSeqNo) || !fields.at(1).trim().getInt(lastSeqNo))
				dsFailedAndLogIt(1, "invalid trans info " << buf, ERROR);
			dsReturn(loadTransIdFunc(tidRaw.trim().c_str(), tid));
		}
	};

	class TransactionCache
	{
	private:
		typedef spp::sparse_hash_map<TransId*, Transaction*> gtidSet;
		std::string m_cacheFileDir;
		Transaction* m_head;
		Transaction* m_last;
		gtidSet m_trans;
		uint64_t m_minSeqNo;
		uint64_t m_flushedTo;
		uint64_t m_firstUnflushedSeqNo;
		uint64_t m_dataSize;
		uint64_t m_flushedSize;
		uint64_t m_maxMemSize;
		SimpleMemCache<Transaction> m_transMemCache;
		SimpleMemCache<RecordList> m_recordListMemCache;
		TidMemBufImp* m_transIdCache;
		DataBuf m_dataBuf;
	private:
		DS setHeadFileInfo(uint64_t seqNo)
		{
			String tmpFile = m_cacheFileDir;
			tmpFile.append("/").append(seqNo).append(".").append(m_head->getStartCheckpoint().seqNo.seqNo).append(TXN_TRANS_INDEX_NAME).append(".tmp");
			if (checkFileExist(tmpFile.c_str(), F_OK) == 0)
			{
				if (0 != remove(tmpFile.c_str()) || checkFileExist(tmpFile.c_str(), F_OK) == 0)
					dsFailedAndLogIt(1, "remove exist transaction tmp index file " << tmpFile << " failed for " << errno << "," << strerror(errno), ERROR);
			}
			fileHandle fd = openFile(tmpFile.c_str(), true, true, true);
			if (fd == INVALID_HANDLE_VALUE)
				dsFailedAndLogIt(1, "create transaction tmp index file " << tmpFile << " failed for " << errno << "," << strerror(errno), ERROR);
			char* headBuf = m_dataBuf.m_dataBuf;
			char* pos = headBuf;
			for (auto& iter : m_trans)
			{
				std::string s = iter.first->toString();
				if (pos + s.size() + 64 >= headBuf + m_dataBuf.dataBufSize)
				{
					if (pos - headBuf != writeFile(fd, headBuf, pos - headBuf))
					{
						if (headBuf != m_dataBuf.m_dataBuf)
							free(headBuf);
						closeFile(fd);
						dsFailedAndLogIt(1, "write transaction tmp index file " << tmpFile << " failed for " << errno << "," << strerror(errno), ERROR);
					}
					pos = headBuf;
				}
				memcpy(pos, s.c_str(), s.size());
				pos += s.size();
				*pos = ':';
				pos++;
				pos += u64toa_sse2(iter.second->getStartCheckpoint().seqNo.seqNo, pos);
				*pos++ = '-';
				pos += u64toa_sse2(iter.second->getLastSeqNo(), pos);
				*pos = '\n';
				pos++;
			}
			if (pos - headBuf != writeFile(fd, headBuf, pos - headBuf))
			{
				if (headBuf != m_dataBuf.m_dataBuf)
					free(headBuf);
				closeFile(fd);
				dsFailedAndLogIt(1, "write transaction tmp index file " << tmpFile << " failed for " << errno << "," << strerror(errno), ERROR);
			}
			closeFile(fd);
			if (headBuf != m_dataBuf.m_dataBuf)
				free(headBuf);
			dsOk();
		}

		DS renameTmpHeadFile(uint64_t seqNo)
		{
			String file = m_cacheFileDir;
			file.append("/").append(seqNo).append(".").append(m_head->getStartCheckpoint().seqNo.seqNo).append(TXN_TRANS_INDEX_NAME);
			String tmpFile = file;
			tmpFile.append(".tmp");
			if (!rename(tmpFile.c_str(), file.c_str()))
				dsFailedAndLogIt(1, "rename transaction tmp index file " << tmpFile << " failed for " << errno << "," << strerror(errno), ERROR);
			dsOk();
		}

		DS flush(uint64_t seqNo)
		{
			if (seqNo == m_flushedTo)
				dsOk();
			dsReturnIfFailed(setHeadFileInfo(seqNo));
			for (auto& iter : m_trans)
			{
				Transaction* t = iter.second;
				if (t->getRealMemUse() == 0)
					continue;
				dsReturnIfFailed(iter.second->flush(m_recordListMemCache, m_cacheFileDir, m_dataBuf));
			}
			dsReturnIfFailed(renameTmpHeadFile(seqNo));
			m_firstUnflushedSeqNo = 0;
			m_flushedTo = seqNo;
		}
	public:
		TransactionCache(const char* cacheFileDir, TidMemBufImp* transIdCache) :m_cacheFileDir(cacheFileDir), m_transIdCache(transIdCache)
		{
		}

		~TransactionCache()
		{
			clearMem(); 
			if (m_transIdCache != nullptr)
				delete m_transIdCache;
		}

		void clearMem()
		{
			for (auto& iter : m_trans)
			{
				iter.second->clearMem(m_recordListMemCache, m_transIdCache);
				delete iter.second;
			}
			m_trans.clear();
		}

		void renameUnusedTransFile(std::function<DS(const char*, TransId*&)> loadTransIdFunc)
		{
			std::vector<String> files;
			if (!dsCheck(fileList::getFileList(m_cacheFileDir, files)))
			{
				getLocalStatus().clear();
				return;
			}
			for (auto& fileName : files)
			{
				if (!fileName.endWith(TXN_FILE_NAME))
					continue;
				std::string tidStr = fileName.substr(0, fileName.size() - strlen(TXN_FILE_NAME));
				TransId* txId = nullptr;
				if (!dsCheck(loadTransIdFunc(tidStr.c_str(), txId)))
					getLocalStatus().clear();
				if (txId == nullptr || m_trans.contains(txId))
					continue;
				std::string src = m_cacheFileDir + "/" + fileName;
				std::string dest = src + ".expire";
				LOG(WARNING) << "rename " << src << " to " << dest;
				if (0 != rename(src.c_str(), dest.c_str()))
					LOG(WARNING) << "rename " << src << " to " << dest << " failed for " << errno << ", " << strerror(errno);
			}
		}

		DS loadTransInfoFromIndexFile(const std::string& selectFile, uint64_t safeSeqNo, std::list<TransIndexInfo>& transIds, std::function<DS(const char*, TransId*&)> loadTransIdFunc)
		{
			std::string idxFile = m_cacheFileDir + "/" + selectFile;
			FILE* fp = fopen(idxFile.c_str(), "r");
			if (fp == nullptr)
				dsFailedAndLogIt(1, "open trans cache index file:" << idxFile << " failed for " << errno << ", " << strerror(errno), ERROR);
			char buf[1024] = { 0 };
			while (nullptr != fgets(buf, sizeof(buf) - 1, fp))
			{
				TransIndexInfo tii;
				dsReturnIfFailed(tii.load(buf, loadTransIdFunc));
				if (tii.beginSeqNo < safeSeqNo)
				{
					LOG(INFO) << "ignore trans:" << tii.tid->toString() << ", seqNo:" << tii.beginSeqNo;
					delete tii.tid;
					continue;
				}
				LOG(INFO) << "get trans:" << tii.tid->toString() << ", seqNo:" << tii.beginSeqNo << "-" << tii.lastSeqNo;
				transIds.push_back(tii);
			}
			int errNo = errno;
			fclose(fp);
			if (errNo != 0)
				dsFailedAndLogIt(1, "read trans cache index file:" << idxFile << " failed for " << errNo << ", " << strerror(errNo), ERROR);
			dsOk();
		}

		DS load(uint64_t seqNo, uint64_t safeSeqNo, std::function<DS(const char*, TransId*&)> loadTransIdFunc)
		{
			std::vector<String> files;
			dsReturnIfFailed(fileList::getFileList(m_cacheFileDir, files));
			if (files.empty())
				dsOk();
			String selectFile;
			uint64_t selectFileSeqNo = 0;
			for (auto& fileName : files)
			{
				std::vector<String> fileds = fileName.split(".");
				uint64_t fileSeqNo, fileSafeSeqNo;
				if (fileds.size() != 3 || fileds.at(1).compare(TXN_TRANS_INDEX_NAME + 1) != 0 ||
					!fileds.at(0).getInt(fileSeqNo) || !!fileds.at(1).getInt(fileSafeSeqNo))
					continue;
				if (fileSeqNo < safeSeqNo)
					continue;
				if (fileSafeSeqNo > safeSeqNo)
					continue;
				if (fileSeqNo > selectFileSeqNo)
				{
					selectFileSeqNo = fileSeqNo;
					selectFile = fileName;
				}
			}
			if (selectFileSeqNo == 0)
			{
				renameUnusedTransFile(loadTransIdFunc);
				dsOk();
			}
			std::list<TransIndexInfo> transIds;
			std::map<uint64_t, Transaction*> transMap;
			dsReturnIfFailedWithOp(loadTransInfoFromIndexFile(selectFile, safeSeqNo, transIds, loadTransIdFunc), do {
				for (auto& iter : transIds)
					delete iter.tid;
			} while (0));
			bool failed = false;
			for (auto& iter : transIds)
			{
				if (failed)
					delete iter.tid;
				std::string fileName = m_cacheFileDir + "/" + iter.tid->toString() + TXN_FILE_NAME;
				Transaction* t = nullptr;
				if (!dsCheck(Transaction::loadTransAndCheck(iter.tid, m_dataBuf, m_recordListMemCache, fileName, t,iter.beginSeqNo,iter.lastSeqNo)))
				{
					delete iter.tid;
					failed = true;
				}
				else
				{
					m_trans.insert(std::pair<TransId*, Transaction*>(iter.tid, t));
					transMap.insert(std::pair<uint64_t, Transaction*>(t->getStartCheckpoint().seqNo.seqNo, t));
				}
			}
			if (failed)
			{
				clearMem();
				dsReturn(getLocalStatus().code);
			}
			renameUnusedTransFile(loadTransIdFunc);
			for (auto& iter : transMap)
			{
				if (m_head == nullptr)
					m_head = iter.second;
				else
					iter.second->addAfter(m_last);
				m_last = iter.second;
				m_dataSize += iter.second->getMemUse();
				if (m_flushedTo < iter.second->getLastSeqNo())
					m_flushedTo = iter.second->getLastSeqNo();
			}
			if (m_head != nullptr)
			{
				m_minSeqNo = m_head->getStartCheckpoint().seqNo.seqNo;
				m_flushedSize = m_dataSize;
			}
			dsOk();
		}

		inline Transaction* commit(TransId& tid)
		{
			m_trans.erase(&tid);
			auto iter = m_trans.find(&tid);
			if (iter == m_trans.end())
				return nullptr;
			Transaction* t = iter->second;
			if (m_head == t)
			{
				m_head = t->getNext();
				t->dropFromChain();
				if (m_head == nullptr)
				{
					m_last = nullptr;
					m_minSeqNo = 0;
				}
				else
				{
					m_minSeqNo = m_head->getStartCheckpoint().seqNo.seqNo;
				}
			}
			else
			{
				if (m_last == t)
					m_last = t->getPrev();
				t->dropFromChain();
			}
			m_trans.erase(iter);
			return t;
		}

		inline void clearTransaction(Transaction* t)
		{
			t->clear(m_recordListMemCache, m_transIdCache, m_cacheFileDir);
			m_transMemCache.free(t);
		}

		inline DS add(TransId& tid, LogEntry* entry)
		{
			Transaction* t;
			auto iter = m_trans.find(&tid);
			if (iter == m_trans.end())
			{
				TransId* _tid = m_transIdCache->alloc(tid);
				t = m_transMemCache.alloc(_tid, entry);
				if (m_last == nullptr)
				{
					m_last = m_head = t;
					m_minSeqNo = entry->getCheckpoint()->seqNo.seqNo;
				}
				else
				{
					t->addAfter(m_last);
				}
				m_trans.insert(std::pair<TransId*, Transaction*>(&tid, t));
			}
			t->add(m_recordListMemCache, entry);
			if ((m_dataSize += entry->size) - m_flushedSize >= m_maxMemSize)
			{
				dsReturnIfFailed(flush(entry->getCheckpoint()->seqNo.seqNo));
			}
			else
			{
				if (m_firstUnflushedSeqNo == 0)
					m_firstUnflushedSeqNo = entry->getCheckpoint()->seqNo.seqNo;
			}
			dsOk();
		}

		inline uint64_t getSafePurgeSeqNo(uint64_t currentSeqNo)
		{
			if (m_firstUnflushedSeqNo == 0)
				return currentSeqNo;
			else
				return m_firstUnflushedSeqNo;
		}

		inline uint64_t minSeqNo()
		{
			return m_minSeqNo;
		}
	};
}