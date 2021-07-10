#pragma once
#include <glog/logging.h>
#include <stdlib.h>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <functional>
#include <mutex>
#include <condition_variable>
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
	private:
		constexpr static uint32_t IN_CLEAR = 0x80000000U;
		constexpr static uint32_t IN_LOAD = 0xc0000000U;
		constexpr static uint32_t NEED_FREE = 0x20000000U;

		std::atomic<uint32_t> m_ref;
		T* m_data;
		std::function<DS (T*)>& loadFunc;
	public:
		ref(T* data, std::function<DS (T*)>& loadFunc) :m_ref(0), m_data(data), loadFunc(loadFunc) {}
		void needFree()
		{
			if(0 == m_ref.fetch_or(NEED_FREE))
				delete m_data;
		}
		void unuse()
		{
			for (;;)
			{
				uint32_t r = m_ref.load(std::memory_order_relaxed);
				if ((r & ~NEED_FREE) == 1)
				{
					if (m_ref.compare_exchange_weak(r, IN_CLEAR))
					{
						m_data->close();
						if (r & NEED_FREE)
							delete m_data;
						else
							m_ref.store(0, std::memory_order_relaxed);
						return;
					}
				}
				else
				{
					if (m_ref.compare_exchange_weak(r, r - 1))
						return;
				}
			}
		}
		void useForWrite()
		{
			m_ref.store(1, std::memory_order_relaxed);
		}
		DS use()
		{
			for (;;)
			{
				uint32_t r = m_ref.load(std::memory_order_relaxed);
				//do not allow iterator call use after needFree()
				assert(!(r & NEED_FREE));
				if (unlikely(r == IN_CLEAR || r == IN_LOAD))
				{
					yield();
					continue;
				}
				if (r == 0)
				{
					if (m_ref.compare_exchange_weak(r, IN_LOAD))
					{
						DS s = loadFunc(m_data);
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
					if (m_ref.compare_exchange_weak(r, r + 1))
						dsOk();
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
			DLL_EXPORT block(std::function<DS (block*)>& loadFunc, uint32_t defaultBlockSize, uint32_t offset, const logEntryRpcBase* LogEntry);
			DLL_EXPORT block(std::function<DS (block*)>& loadFunc, const index& i);
			DLL_EXPORT block(std::function<DS (block*)>& loadFunc, const index& i, char* data, uint32_t volumn, uint32_t size);
			DLL_EXPORT ~block();
			DLL_EXPORT void close();
			uint32_t find(const logIndexInfo& logIndex);
			void append(const logEntryRpcBase* LogEntry);
		};
	private:
		std::string m_filePath;
		uint64_t m_fileId;
		int32_t m_offset;
		uint32_t m_blockCount;
		logIndexInfo m_beginLogIndex;
		logIndexInfo m_prevLogIndex;

		logIndexInfo m_logIndex;
		block** m_blocks;
		block* m_currentBlock;
		logConfig m_config;
		uint32_t m_maxBlockCount;
		clusterLogFile* m_next;
		std::function<DS (block*)> m_blockLoadFunc;
		std::function<DS (clusterLogFile*)> m_fileLoadFunc;
		ref<clusterLogFile> m_ref;
		std::mutex m_lock;
		std::mutex m_flushLock;
		std::condition_variable m_condition;

		fileHandle m_fd;
		fileHandle m_indexFd;
		fileHandle m_readFd;
	public:
		DLL_EXPORT clusterLogFile(const char* filePath, uint64_t fileId, logConfig& config);
		DLL_EXPORT ~clusterLogFile();
		DLL_EXPORT uint64_t getFileId();
		const logIndexInfo& getPrevLogIndex();
		DLL_EXPORT const logIndexInfo& getBeginLogIndex();
		DLL_EXPORT const logIndexInfo& getLogIndex();
		DLL_EXPORT clusterLogFile* getNext();
		DLL_EXPORT void setNext(clusterLogFile* next);
	private:
		void notify();
		void wait();
		DS openLogFile(fileHandle& handle, bool readOnly);
		DLL_EXPORT DS readRecordHead(fileHandle fd, uint32_t dataFileSize, uint32_t pos, logEntryRpcBase& head, bool readOnly);
		DLL_EXPORT DS recoveryIndexFromLastBlock(fileHandle fd, uint32_t offset, uint32_t dataFileSize, bool readOnly);
		DLL_EXPORT DS loadIndex(fileHandle fd, bool readOnly);
	public:

		DLL_EXPORT DS readMetaInfo();
		DLL_EXPORT DS recovery();
		DLL_EXPORT DS load();
	private:
		DLL_EXPORT DS loadFile(clusterLogFile* logFile);
		DS _append(const logEntryRpcBase* LogEntry);
		DLL_EXPORT DS appendToNewBlock(const logEntryRpcBase* LogEntry);
		DLL_EXPORT block* findBlock(const logIndexInfo& logIndex);
		DLL_EXPORT DS loadBlock(block* b);
	public:
		DLL_EXPORT DS open();
		DLL_EXPORT DS create(clusterLogFile* prev, const logIndexInfo& beginLogIndex);

		DLL_EXPORT DS create(const logIndexInfo& prevLogIndex, const logIndexInfo& beginLogIndex);
		DLL_EXPORT DS deleteFile();
		DLL_EXPORT void close();
		DLL_EXPORT DS finish();
		DLL_EXPORT DS writeCurrentBlock();
		DLL_EXPORT DS append(const logEntryRpcBase* LogEntry);
		DLL_EXPORT DS clear();
		DLL_EXPORT DS rollback(const logIndexInfo& logIndex);
		DLL_EXPORT DS flush();
		DLL_EXPORT inline DS use()
		{
			dsReturn(m_ref.use());
		}
		DLL_EXPORT inline void unUse()
		{
			m_ref.unuse();
		}
		class iterator {
		private:
			logIndexInfo m_logIndex;
			uint32_t m_offset;
			clusterLogFile* m_file;
			block* m_block;
			bool m_onlyReadCommitted;
			DLL_EXPORT DS rotate(block* b);
		public:
			DLL_EXPORT iterator() : m_offset(0), m_file(nullptr), m_block(nullptr), m_onlyReadCommitted(true) {}
			DLL_EXPORT ~iterator()
			{
				if (m_block != nullptr)
					m_block->m_ref.unuse();
			}
			DLL_EXPORT DS setLogFile(clusterLogFile* file);
			DLL_EXPORT DS attachToNextLogFile(clusterLogFile* file);
			DLL_EXPORT DS search(const logIndexInfo& logIndex);
			DLL_EXPORT DS next(const logEntryRpcBase*& LogEntry);
			DLL_EXPORT DS next(const logEntryRpcBase*& LogEntry, long outTime);
		};
	};
}
