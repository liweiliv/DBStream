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
#include "../util/pageTable.h"
#include "../util/config.h"
#include "../glog/logging.h"
#include "../message/record.h"
#include "filter.h"
#include "../util/shared_mutex.h"
#include "../util/ringFixedQueue.h"
#include "../util/threadPool.h"
namespace META {
	class metaDataCollection;
}
namespace STORE
{
constexpr auto C_STORE_SCTION = "store";
constexpr auto C_LOG_DIR =  ".logDir";
constexpr auto C_LOG_PREFIX  =".logPrefix";
constexpr auto C_REDO  = ".redo";
constexpr auto C_COMPRESS = ".compress";
constexpr auto C_BLOCK_DEFAULT_SIZE = ".blockDefaultSize";
constexpr auto C_REDO_FLUSH_DATA_SIZE  =".redoFlushDataSize";
constexpr auto C_REDO_FLUSH_PERIOD = ".redoFlushPeriod";
constexpr auto C_OUTDATED  =".outdated";
constexpr auto C_MAX_UNFLUSHED_BLOCK  =".maxUnflushedBlock";
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
private:
	bool m_running;
	const char * m_confPrefix;
	pageTable m_blocks;
	std::atomic<int> m_lastFlushedFileID;
	uint64_t m_maxBlockID;
	config * m_config;
	appendingBlock * m_current;

	bufferPool *m_pool;
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
	threadPool<blockManager,void, blockManager*> m_threadPool;
	std::atomic<uint32_t> m_firstBlockId;
	std::atomic<uint32_t> m_lastBlockId;

	std::atomic_int m_currentFlushThreadCount;
public:
	blockManager(const char * confPrefix, config * conf, bufferPool* pool,META::metaDataCollection* metaDataCollection) :m_running(false), m_confPrefix(confPrefix), m_blocks(nullptr), m_maxBlockID(0)
		, m_config(conf), m_current(nullptr), m_pool(pool), m_metaDataCollection(metaDataCollection), m_threadPool(createThreadPool(32, this, &blockManager::flushThread, "blockManagerFlush"))
	{
		initConfig();
	}
	std::string updateConfig(const char *key, const char * value);
private:
	int recoveryFromRedo(uint32_t from,uint32_t to);
	block* getBlock(uint32_t blockId);
	block* checkBlockCanUse(block* b);
	int initConfig();
	bool createNewBlock();
	int insert(DATABASE_INCREASE::record* r);
	int flush(appendingBlock* block);
	void createFlushThread();
	void flushThread(blockManager* m);
	static void purgeThread(blockManager* m);
public:
	inline std::string getBlockFile(uint64_t id)
	{
		char numBuf[64];
		sprintf(numBuf, "%lu", id);
		return std::string(m_logDir).append(separatorChar).append(m_logPrefix).append(".").append(numBuf);
	}
	int start()//todo
	{
		createFlushThread();
		return 0;
	}
	int stop() //todo
	{

		return 0;
	}
	int load();
	inline page * allocPage(uint64_t size)
	{
		page *p = (page*)m_pool->allocByLevel(0);
		if (size > m_pool->maxSize())
			p->pageData = (char*)malloc(size);
		else
			p->pageData = (char*)m_pool->alloc(size);
		p->pageSize = size;
		p->pageUsedSize = 0;
		return p;
	}
	inline void *allocMem(size_t size)
	{
		return m_pool->alloc(size);
	}
	inline void freePage(page *p)
	{
		if (p->pageSize > m_pool->maxSize())
			free(p->pageData);
		else
			m_pool->free(p->pageData);
		m_pool->free(p);
	}
	void* allocMemForRecord(META::tableMeta* table, size_t size);
};
class blockManagerIterator: public iterator
{
private:
    filter* m_filter;
    block * m_current;
    iterator * m_blockIter;
    blockManager * m_manager;
public:
    inline bool valid()
    {
        return m_manager!=nullptr&&m_current!=nullptr&&m_blockIter!=NULL;
    }
    inline status next()
    {
        status s = m_blockIter->next();
        switch(s)
        {
        case OK:
            return OK;
        case ENDED:
        {
            block * nextBlock = static_cast<block*>((void*)m_manager->m_blocks.get(m_current->m_blockID));
            if(nextBlock==nullptr)
                return BLOCKED;
            if(nextBlock->m_flag&BLOCK_FLAG_APPENDING)
            {
                appendingBlockIterator * iter = new appendingBlockIterator(nextBlock,&m_filter);
                iterator * tmp = m_blockIter;
                m_blockIter = iter;
                delete tmp;
            }
            else
            {

            }
        }
        }
    }
};
}

#endif /* BLOCKMANAGER_H_ */
