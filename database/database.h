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
	class appendingBlock;
	class block;
	class database
	{
		friend class databaseIterator;
		friend class databaseTimestampIterator;
		friend class databaseCheckpointIterator;
		friend class databaseRecordIdIterator;
		friend class threadPool<database, void>;
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
		block* m_last;
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
		threadPool<database, void> m_threadPool;
		std::atomic<uint32_t> m_firstBlockId;
		std::atomic<uint32_t> m_lastBlockId;
		std::atomic_int m_currentFlushThreadCount;
		uint64_t m_recordId;
		uint64_t m_tnxId;
	public:
		DLL_EXPORT database(const char* confPrefix, config* conf, bufferPool* pool, META::metaDataBaseCollection* metaDataCollection);
		DLL_EXPORT std::string updateConfig(const char* key, const char* value);
	private:
		int recoveryFromRedo(std::set<uint64_t>& redos, std::map<uint32_t,block*> &recoveried);
		int pickRedo(std::map<uint32_t, block*>& recoveried, block* from, block* to);
		block* getBlock(uint32_t blockId);
		int checkSolidBlock(block* b);
		block* getBasciBlock(uint32_t blockId);
		int initConfig();
		bool createNewBlock();
		int flush(appendingBlock* block);
		void flushThread();
		int purge();
		int gc();
		int removeBlock(block* b);
		static void purgeThread(database* m);
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
		DLL_EXPORT iterator* createIndexIterator(uint32_t flag,const META::tableMeta * table,META::KEY_TYPE type,int keyId );
		DLL_EXPORT char* getRecord(const META::tableMeta* table, META::KEY_TYPE type, int keyId, const void* key);
	};
	class databaseIterator : public iterator
	{
	protected:
		enum class DB_ITER_TYPE {
			TIMESTAMP_TYPE,
			CHECKPOINT_TYPE,
			RECORD_ID_TYPE,
		};
		block* m_current;
		iterator* m_blockIter;
		database* m_database;
		DB_ITER_TYPE m_iterType;
	public:
		DLL_EXPORT databaseIterator(uint32_t flag, DB_ITER_TYPE type,filter* filter, database* db);
		DLL_EXPORT virtual ~databaseIterator();
		DLL_EXPORT inline bool valid()
		{
			return m_database != nullptr && m_current != nullptr && m_blockIter != nullptr;
		}
		DLL_EXPORT status next();
		DLL_EXPORT inline const void* value() const
		{
			return m_blockIter->value();
		}
		DLL_EXPORT inline const void* key() const
		{
			return m_blockIter->key();
		}
		DLL_EXPORT inline bool end()
		{
			return m_blockIter==nullptr?true:m_blockIter->end();
		}
	};
	class databaseTimestampIterator :public databaseIterator
	{
		databaseTimestampIterator(uint32_t flag, filter* filter, database* db) :databaseIterator(flag, DB_ITER_TYPE::TIMESTAMP_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		~databaseTimestampIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
	class databaseCheckpointIterator :public databaseIterator
	{
		databaseCheckpointIterator(uint32_t flag, filter* filter, database* db) :databaseIterator(flag, DB_ITER_TYPE::CHECKPOINT_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		~databaseCheckpointIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
	class databaseRecordIdIterator :public databaseIterator
	{
		databaseRecordIdIterator(uint32_t flag, filter* filter, database* db) :databaseIterator(flag, DB_ITER_TYPE::RECORD_ID_TYPE, filter, db)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		~databaseRecordIdIterator() {}
		DLL_EXPORT bool seek(const void* key);
	};
}
#endif /* BLOCKMANAGER_H_ */
