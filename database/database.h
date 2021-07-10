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
#include "iterator.h"
#include "page.h"
#include "util/pageTable.h"
#include "util/config.h"
#include "glog/logging.h"
#include "message/record.h"
#include "filter.h"
#include "thread/shared_mutex.h"
#include "util/ringFixedQueue.h"
#include "thread/threadPool.h"
#include "globalInfo/global.h"
#include "util/valgrindTestUtil.h"
#include "util/dualLinkList.h"
#include "statistic.h"
namespace DATABASE
{
	constexpr auto C_INSTANCE_SCTION = "instance";
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
	class AppendingBlock;
	class Block;
	class Database
	{
		friend class DatabaseIterator;
		friend class DatabaseTimestampIterator;
		friend class DatabaseCheckpointIterator;
		friend class DatabaseRecordIdIterator;
		friend class threadPool<Database, void>;
	public:
		enum class BLOCK_MANAGER_STATUS {
			BM_RUNNING,
			BM_STOPPING,
			BM_STOPPED,
			BM_FAILED,
			BM_UNINIT
		};
	private:
		BLOCK_MANAGER_STATUS m_status;
		const char* m_confPrefix;
		pageTable<Block*> m_blocks;
		std::atomic<int> m_lastFlushedFileID;
		uint64_t m_maxBlockID;
		Config* m_config;
		Block* m_last;
		AppendingBlock* m_current;

		bufferPool* m_pool;
		META::MetaDataBaseCollection* m_metaDataCollection;
		std::condition_variable m_toSolodBlockCond;
		ringFixedQueue<AppendingBlock*> m_unflushedBlocks;
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
		std::mutex m_flushLock;
		threadPool<Database, void> m_threadPool;
		std::atomic<uint32_t> m_firstBlockId;
		std::atomic<uint32_t> m_lastBlockId;
		std::atomic_int m_currentFlushThreadCount;
		uint64_t m_recordId;
		uint64_t m_tnxId;
		globalLockDualLinkList m_pageLru;
		statistic m_statistic;
	public:
		DLL_EXPORT Database(const char* confPrefix, Config* conf, bufferPool* pool, META::MetaDataBaseCollection* metaDataCollection);
		DLL_EXPORT ~Database();
		DLL_EXPORT std::string updateConfig(const char* key, const char* value);
	private:
		int recoveryFromRedo(std::set<uint64_t>& redos, std::map<uint32_t, Block*>& recoveried);
		int pickRedo(std::map<uint32_t, Block*>& recoveried, Block* from, Block* to);
		Block* getBlock(uint32_t blockId);
		int checkSolidBlock(Block* b);
		Block* getBasciBlock(uint32_t blockId);
		int initConfig();
		int finishAppendingBlock(AppendingBlock* b);
		bool createNewBlock();
		int flush(AppendingBlock* block);
		void flushThread();
		int purge();
		int removeBlock(Block* b);
		int compaction(uint64_t from,uint64_t to,bool keepHistory);
		static void purgeThread(Database* m);
	public:
		DLL_EXPORT int fullGc();
		DLL_EXPORT const statistic* getStatistic();
		DLL_EXPORT int insert(RPC::Record* r);
		DLL_EXPORT inline void begin()
		{
			if (0 == (++m_tnxId))
				m_tnxId++;
		}
		DLL_EXPORT void commit();
		DLL_EXPORT inline void genBlockFileName(uint64_t id, char* fileName)
		{
			sprintf(fileName, "%s%s%s.%lu", m_logDir, separatorChar, m_logPrefix, id);
		}
		DLL_EXPORT int start()
		{
			m_status = BLOCK_MANAGER_STATUS::BM_RUNNING;
			m_threadPool.createNewThread();
			return 0;
		}
		DLL_EXPORT int stop();
		DLL_EXPORT int load();
		DLL_EXPORT int flushLogs();
		DLL_EXPORT Page* allocPage(uint64_t size);
		DLL_EXPORT inline void* allocMem(size_t size)
		{
#ifdef VLGRIND_TEST
			return basicBufferPool::allocDirect(size);
#else
			return m_pool->alloc(size);
#endif
		}
		DLL_EXPORT inline void freePage(Page* p)
		{
			if (p->pageData != nullptr)
				m_pool->free(p->pageData);
			m_pool->free(p);
		}
		DLL_EXPORT void* allocMemForRecord(META::TableMeta* table, size_t size);
		DLL_EXPORT bool checkpoint(uint64_t& timestamp, uint64_t& logOffset);
		DLL_EXPORT Iterator* createIndexIterator(uint32_t flag, const META::TableMeta* table, META::KEY_TYPE type, int keyId);
		DLL_EXPORT char* getRecord(const META::TableMeta* table, META::KEY_TYPE type, int keyId, const void* key);
	};
	class DatabaseIterator : public Iterator
	{
	protected:
		enum class DB_ITER_TYPE {
			TIMESTAMP_TYPE,
			CHECKPOINT_TYPE,
			RECORD_ID_TYPE,
		};
		Block* m_current;
		Iterator* m_blockIter;
		Database* m_database;
		DB_ITER_TYPE m_iterType;
	public:
		DLL_EXPORT DatabaseIterator(uint32_t flag, DB_ITER_TYPE type, Filter* filter, Database* db);
		DLL_EXPORT virtual ~DatabaseIterator();
		DLL_EXPORT inline bool valid()
		{
			return m_database != nullptr && m_current != nullptr && m_blockIter != nullptr;
		}
		DLL_EXPORT Status next();
		DLL_EXPORT inline const void* value()
		{
			return m_blockIter->value();
		}
		DLL_EXPORT inline const void* key() const
		{
			return m_blockIter->key();
		}
		DLL_EXPORT inline bool end()
		{
			return m_blockIter == nullptr ? true : m_blockIter->end();
		}
	};
	class DatabaseTimestampIterator :public DatabaseIterator
	{
	public:
		DatabaseTimestampIterator(uint32_t flag, Filter* filter, Database* db) :DatabaseIterator(flag, DB_ITER_TYPE::TIMESTAMP_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		~DatabaseTimestampIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
	class DatabaseCheckpointIterator :public DatabaseIterator
	{
	public:
		DatabaseCheckpointIterator(uint32_t flag, Filter* filter, Database* db) :DatabaseIterator(flag, DB_ITER_TYPE::CHECKPOINT_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		~DatabaseCheckpointIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
	class DatabaseRecordIdIterator :public DatabaseIterator
	{
	public:
		DatabaseRecordIdIterator(uint32_t flag, Filter* filter, Database* db) :DatabaseIterator(flag, DB_ITER_TYPE::RECORD_ID_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		~DatabaseRecordIdIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
}
#endif /* BLOCKMANAGER_H_ */
