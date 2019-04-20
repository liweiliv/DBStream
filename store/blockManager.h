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
#include "index.h"
#include "block.h"
#include "appendingBlock.h"
#include "iterator.h"
#include "../util/pageTable.h"
#include "config.h"
#include <glog/logging.h>
#include "../message/record.h"
#include "filter.h"
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

#define REAL_CONF_STRING(c) std::string(m_confPrefix).append(c).c_str()
class blockManagerIterator;
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
	uint64_t m_maxRecordId;
public:
    blockManager(const char * confPrefix,config * conf) :m_confPrefix(confPrefix),m_blocks(nullptr),
            m_running(false), m_config(conf)
    {
        initConfig();
    }
    std::string updateConfig(const char *key,const char * value)
    {
        if(strcmp(key, REAL_CONF_STRING(C_LOG_DIR))==0)
            return "config :logDir is static,can not change";
        else if(strcmp(key, REAL_CONF_STRING(C_LOG_PREFIX))==0)
            return "config :logPrefix is static,can not change";
        else if(strcmp(key, REAL_CONF_STRING(C_REDO))==0)
        {
            if(strcmp(value,"on"))
                m_redo = true;
            else if(strcmp(value,"off"))
                m_redo = false;
            else
                return "value of config :redo must be [on] or [off]";
        }
        else if(strcmp(key, REAL_CONF_STRING(C_COMPRESS))==0)
        {
            if(strcmp(value,"on"))
                m_compress = true;
            else if(strcmp(value,"off"))
                m_compress = false;
            else
                return "value of config :compress must be [on] or [off]";
        }
        else if(strcmp(key, REAL_CONF_STRING(C_BLOCK_DEFAULT_SIZE))==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if((value[idx]>'9'||value[idx]<'0'))
                    return "value of config :blockDefaultSize must be a number";
            }
            m_blockDefaultSize  = atol(value);
        }
        else if(strcmp(key, REAL_CONF_STRING(C_REDO_FLUSH_DATA_SIZE))==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if((value[idx]>'9'||value[idx]<'0')&&!(idx==0&&value[idx]=='-'))
                    return "value of config :redoFlushDataSize must be a number";
            }
            m_redoFlushDataSize  = atol(value);
        }
        else if(strcmp(key, REAL_CONF_STRING(C_REDO_FLUSH_PERIOD))==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if((value[idx]>'9'||value[idx]<'0')&&!(idx==0&&value[idx]=='-'))
                    return "value of config :redoFlushPeriod must be a number";
            }
            m_redoFlushPeriod  = atol(value);
        }
        else if(strcmp(key, REAL_CONF_STRING(C_OUTDATED))==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if((value[idx]>'9'||value[idx]<'0'))
                    return "value of config :outdated must be a number";
            }
            m_outdated = atol(value);
        }
        else if(strcmp(key, REAL_CONF_STRING(C_MAX_UNFLUSHED_BLOCK))==0)
        {
            for(int idx = strlen(value)-1;idx>=0;idx--)
            {
                if((value[idx]>'9'||value[idx]<'0'))
                    return "value of config :maxUnflushedBlock must be a number";
            }
            m_maxUnflushedBlock = atol(value);
        }
        else
            return std::string("unknown config:")+key;
        m_config->set("store",key,value);
        return std::string("update config:")+key+" success";
    }
private:
    int initConfig()
    {
        strcmp(m_logDir,m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_DIR),"data").c_str());
        strcmp(m_logPrefix,m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_PREFIX),"log.").c_str());
        m_redo = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_REDO),"off")=="on");
        m_compress = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_COMPRESS),"off")=="on");
        m_blockDefaultSize = atol(m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_BLOCK_DEFAULT_SIZE),"33554432").c_str());
        m_redoFlushDataSize = atol(m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_DATA_SIZE),"-1").c_str());
        m_redoFlushPeriod = atol(m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_PERIOD),"-1").c_str());
        m_outdated = atol(m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_OUTDATED),"86400").c_str());
        m_maxUnflushedBlock = atol(m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_UNFLUSHED_BLOCK),"8").c_str());
        return 0;
    }
public:
    int start()//todo
    {
        return 0;
    }
    int stop() //todo
    {
        return 0;
    }
private:
    bool createNewBlock()
    {
        int flag = BLOCK_FLAG_APPENDING;
        if (m_redo)
            flag |= BLOCK_FLAG_HAS_REDO;
        if (m_compress)
            flag |= BLOCK_FLAG_COMPRESS;
        appendingBlock *newBlock = new appendingBlock(flag,m_logDir,m_logPrefix,
                m_blockDefaultSize,m_redoFlushDataSize,
                m_redoFlushPeriod,m_maxRecordId);

        newBlock->m_blockID = ++m_maxBlockID;
        m_blocks.set(newBlock->m_blockID, newBlock);
        m_current->m_flag|=BLOCK_FLAG_FINISHED;

        m_current = newBlock;
        if (m_lastFlushedFileID.load(std::memory_order_relaxed)
                + m_maxUnflushedBlock > m_current->m_blockID)
        {
            while (m_lastFlushedFileID.load(std::memory_order_relaxed)
                    + m_maxUnflushedBlock > m_current->m_blockID)
            {
                if (!m_running)
                    return false;
				std::this_thread::yield();
            }
        }
        return true;
    }
    int insert(DATABASE_INCREASE::record * r)
    {
		if (m_current == nullptr && !createNewBlock())
		{
			LOG(ERROR) << "Fatal Error: insert record failed for create new block failed;" << "record id :" <<
				r->head->recordID << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logID;		
			return -1;
		}
        appendingBlock::appendingBlockStaus status = m_current->append(r);
        switch (status)
        {
        case appendingBlock::OK:
            return 0;
        case appendingBlock::FULL:
            m_current->m_flag |= BLOCK_FLAG_FINISHED;
            if(!createNewBlock())
                return -1;
            return insert(r);
        case appendingBlock::FAULT:
            LOG(ERROR)<<"Fatal Error: insert record to current  block failed;"<<"record id :"<<
            r->head->recordID<<",record offset:"<<r->head->logOffset<<
            "record LogID:"<<r->head->logID;
            return -1;
        case appendingBlock::ILLEGAL:
            LOG(ERROR)<<"Fatal Error: insert record to current  block failed,record is illegal;"<<"record id :"<<
            r->head->recordID<<",record offset:"<<r->head->logOffset<<
            "record LogID:"<<r->head->logID;
            return -1;
        default:
            LOG(ERROR)<<"Fatal Error: insert record to current  block failed,unknown error;"<<"record id :"<<
            r->head->recordID<<",record offset:"<<r->head->logOffset<<
            "record LogID:"<<r->head->logID;
            return -1;
        }
    }
    int recycleCache()
    {

    }
    int flush()
    {
        block *b = nullptr;
        if(m_lastFlushedFileID.load(std::memory_order_release)<0)
        {
            block * first = static_cast<block*>((void*)m_blocks.begin()),*last = static_cast<block*>((void*)m_blocks.end());
            if(first == nullptr|last==nullptr) //no block exist
                return 0;
            int id = first->m_blockID;
            for(;id<=last->m_blockID;id++)
            {
                b = static_cast<block*>((void*)m_blocks[id]);
                if(b == nullptr)
                    continue;
                if(!(b->m_flag&BLOCK_FLAG_FLUSHED))
                    break;
            }
            if(b == nullptr) //no block exist
            return 0;
            if(b->m_flag&BLOCK_FLAG_FLUSHED)//last file has been flushed ,return
            {
                m_lastFlushedFileID.store(b->m_blockID,std::memory_order_release);
                return 0;
            }
            m_lastFlushedFileID.store(b->m_blockID-1,std::memory_order_release);
        }
        do
        {
            if(nullptr==(b = static_cast<block*>((void*)m_blocks[m_lastFlushedFileID.load(std::memory_order_relaxed)+1])))
                break;
            if(!((b->m_flag&BLOCK_FLAG_FINISHED)&&!(b->m_flag&BLOCK_FLAG_FLUSHED)))
                break;
            if(b->flush())
            {
                LOG(ERROR)<<"Fatal Error: flush block ["<< b->m_blockID <<"]  failed";
                m_running = false;
                return -2;
            }
            m_lastFlushedFileID.store(b->m_blockID,std::memory_order_release);
        }while(m_running);
        return 0;
    }
    static void flushThread(blockManager * m)
    {
        while (m->m_running)
        {
            if(0!=m->flush())
            {
                LOG(ERROR)<<"flush block failed,flush thread exit";
				return;
            }
			std::this_thread::yield();//todo
        }
    }
    static void loadThread(void * argv)
    {

    }

    void purge()
    {
        while (m_running)
        {
			if (!iter.Valid() || iter.key()->minTime > now + 1000 * atol(m_config->get("store", "outdated", "7200").c_str())) //no block need purge
				return;

            ((block*)iter.key())->flush();
        }
    }
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
