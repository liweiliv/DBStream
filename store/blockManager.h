/*
 * blockManager.h
 *
 *  Created on: 2019年1月18日
 *      Author: liwei
 */

#ifndef BLOCKMANAGER_H_
#define BLOCKMANAGER_H_
#include <thread>
#include <string>
#include <string.h>
#include <set>
#include <condition_variable>
#include "page.h"
#include "block.h"
#include "iterator.h"
#include "util/pageTable.h"
#include "util/config.h"
#include "glog/logging.h"
#include "message/record.h"
#include "filter.h"
#include "util/shared_mutex.h"
#include "util/ringFixedQueue.h"
#include "util/threadPool.h"
namespace META {
	class metaDataCollection;
}
namespace STORE
{
	constexpr auto C_STORE_SCTION = "store";
	constexpr auto C_LOG_DIR = ".logDir";
	constexpr auto C_LOG_PREFIX = ".logPrefix";
	constexpr auto C_REDO = ".redo";
	constexpr auto C_COMPRESS = ".compress";
	constexpr auto C_BLOCK_DEFAULT_SIZE = ".blockDefaultSize";
	constexpr auto C_REDO_FLUSH_DATA_SIZE = ".redoFlushDataSize";
	constexpr auto C_REDO_FLUSH_PERIOD = ".redoFlushPeriod";
	constexpr auto C_OUTDATED = ".outdated";
	constexpr auto C_MAX_UNFLUSHED_BLOCK = ".maxUnflushedBlock";
	constexpr auto C_FILESYS_PAGE_SIZE = ".fileSysPageSize";
	constexpr auto C_MAX_FLUSH_THREAD = ".maxFlushThread";

#ifdef OS_WIN
	constexpr auto separatorChar = "\\";
#endif
#ifdef OS_LINUX
	constexpr auto separatorChar = "/";
#endif
#define REAL_CONF_STRING(c) std::string(m_confPrefix).append(c).c_str()
#define MAX_FLUSH_THREAD 32
	class blockManagerIterator;
	class appendingBlock;
	class block;
	class blockManager
	{
		friend class blockManagerIterator;
		friend class threadPool<blockManager, void>;
	public:
		enum BLOCK_MANAGER_STATUS {
			BM_RUNNING,
			BM_STOPPED,
			BM_FAILED,
			BM_UNINIT
		};
	private:
		BLOCK_MANAGER_STATUS m_status;
		const char* m_confPrefix;
		pageTable<block*> m_blocks;
		std::atomic<int> m_lastFlushedFileID;
		uint64_t m_maxBlockID;
		config* m_config;
		appendingBlock* m_current;

		bufferPool* m_pool;
		META::metaDataCollection* m_metaDataCollection;
		std::condition_variable m_toSolodBlockCond;
		ringFixedQueue<appendingBlock*> m_unflushedBlocks;
		/*-------------------------static-----------------------*/
		char m_logDir[256];
		char m_logPrefix[256];
		/*-----------------------changeable---------------------*/
		bool m_redo;
		bool m_compress;
		uint32_t m_blockDefaultSize;
		int32_t m_redoFlushDataSize;
		int32_t m_redoFlushPeriod;
		uint32_t m_outdated;
		uint32_t m_maxUnflushedBlock;
		uint32_t m_fileSysPageSize;
		shared_mutex m_blockLock;//all read ,write thread use shared lock,purge thread use mutex lock,avoid the aba problem
		threadPool<blockManager, void> m_threadPool;
		std::atomic<uint32_t> m_firstBlockId;
		std::atomic<uint32_t> m_lastBlockId;
		std::atomic_int m_currentFlushThreadCount;
		uint64_t m_recordId;
		uint64_t m_tnxId;
	public:
		blockManager(const char* confPrefix, config* conf, bufferPool* pool, META::metaDataCollection* metaDataCollection) :m_status(BM_UNINIT), m_confPrefix(confPrefix), m_blocks(nullptr), m_maxBlockID(0)
			, m_config(conf), m_current(nullptr), m_pool(pool), m_metaDataCollection(metaDataCollection), m_threadPool(createThreadPool(32, this, &blockManager::flushThread, std::string("blockManagerFlush_").append(confPrefix).c_str())),m_firstBlockId(0),m_lastBlockId(0),m_currentFlushThreadCount(0), m_recordId(0),m_tnxId(0)
		{
			initConfig();
		}
		std::string updateConfig(const char* key, const char* value);
	private:
		int recoveryFromRedo(uint32_t from, uint32_t to);
		block* updateBasicBlockToSolidBlock(block* b);
		block* getBlock(uint32_t blockId);
		block* getBasciBlock(uint32_t blockId);
		int initConfig();
		bool createNewBlock();
		int flush(appendingBlock* block);
		void flushThread();
		int purge();
		int gc();
		static void purgeThread(blockManager* m);
	public:
		int insert(DATABASE_INCREASE::record* r);
		inline void begin()
		{
			if (0 == (++m_tnxId))
				m_tnxId++;
		}
		DLL_EXPORT void commit();;
		inline void genBlockFileName(uint64_t id,char *fileName)
		{
			sprintf(fileName, "%s%s%s.%lu", m_logDir, separatorChar, m_logPrefix,id);
		}
		DLL_EXPORT int start()//todo
		{
			m_status = BM_RUNNING;
			m_threadPool.createNewThread();
			return 0;
		}
		int stop() //todo
		{
			m_status = BM_STOPPED;
			m_threadPool.join();
			return 0;
		}
		int load();
		DLL_EXPORT inline page* allocPage(uint64_t size)
		{
			page* p = (page*)m_pool->allocByLevel(0);
			if (size > m_pool->maxSize())
				p->pageData = (char*)malloc(size);
			else
				p->pageData = (char*)m_pool->alloc(size);
			p->pageSize = size;
			p->pageUsedSize = 0;
			return p;
		}
		DLL_EXPORT inline void* allocMem(size_t size)
		{
			return m_pool->alloc(size);
		}
		DLL_EXPORT inline void freePage(page* p)
		{
			if (p->pageSize > m_pool->maxSize())
				free(p->pageData);
			else
				m_pool->free(p->pageData);
			m_pool->free(p);
		}
		void* allocMemForRecord(META::tableMeta* table, size_t size);
		bool checkpoint(uint64_t& timestamp, uint64_t logOffset);

	};
	class blockManagerIterator : public iterator
	{
	private:
		block* m_current;
		iterator* m_blockIter;
		blockManager* m_manager;
	public:
		/*
			find record by timestamp
			[IN]timestamp ,start time by micro second
			[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
			[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
		*/
		bool seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter);//timestamp not increase strictly,so we have to check all block
		bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter);
		bool seekByRecordId(uint64_t recordId);
		blockManagerIterator(uint32_t flag, filter* filter, blockManager* m_manager);
		~blockManagerIterator();
		inline bool valid()
		{
			return m_manager != nullptr && m_current != nullptr && m_blockIter != NULL;
		}
		status next();
		inline const void* value() const
		{
			return m_blockIter->value();
		}
		inline bool end()
		{
			return m_blockIter==nullptr?true:m_blockIter->end();
		}
	};
}
#endif /* BLOCKMANAGER_H_ */
