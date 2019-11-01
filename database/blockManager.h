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
#include "thread/threadPool.h"
namespace DATABASE
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
		enum class BLOCK_MANAGER_STATUS {
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
		META::metaDataBaseCollection* m_metaDataCollection;
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
		DLL_EXPORT blockManager(const char* confPrefix, config* conf, bufferPool* pool, META::metaDataBaseCollection* metaDataCollection);
		DLL_EXPORT std::string updateConfig(const char* key, const char* value);
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
		DLL_EXPORT int insert(DATABASE_INCREASE::record* r);
		DLL_EXPORT inline void begin()
		{
			if (0 == (++m_tnxId))
				m_tnxId++;
		}
		DLL_EXPORT void commit();;
		DLL_EXPORT inline void genBlockFileName(uint64_t id,char *fileName)
		{
			sprintf(fileName, "%s%s%s.%llu", m_logDir, separatorChar, m_logPrefix,id);
		}
		DLL_EXPORT int start()//todo
		{
			m_status = BLOCK_MANAGER_STATUS::BM_RUNNING;
			m_threadPool.createNewThread();
			return 0;
		}
		DLL_EXPORT int stop() //todo
		{
			m_status = BLOCK_MANAGER_STATUS::BM_STOPPED;
			m_threadPool.join();
			return 0;
		}
		DLL_EXPORT int load();
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
		DLL_EXPORT void* allocMemForRecord(META::tableMeta* table, size_t size);
		DLL_EXPORT bool checkpoint(uint64_t& timestamp, uint64_t &logOffset);
		DLL_EXPORT int compection(bool (*reduce)(const char*));

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
		DLL_EXPORT bool seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter);//timestamp not increase strictly,so we have to check all block
		DLL_EXPORT bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter);
		DLL_EXPORT bool seekByRecordId(uint64_t recordId);
		DLL_EXPORT blockManagerIterator(uint32_t flag, filter* filter, blockManager* m_manager);
		DLL_EXPORT ~blockManagerIterator();
		DLL_EXPORT inline bool valid()
		{
			return m_manager != nullptr && m_current != nullptr && m_blockIter != NULL;
		}
		DLL_EXPORT status next();
		DLL_EXPORT inline const void* value() const
		{
			return m_blockIter->value();
		}
		DLL_EXPORT inline bool end()
		{
			return m_blockIter==nullptr?true:m_blockIter->end();
		}
	};
}
#endif /* BLOCKMANAGER_H_ */
