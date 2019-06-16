/*
 * block_file_manager.h
 *
 *  Created on: 2017年3月10日
 *      Author: liwei
 */

#ifndef LIB_CALCULATE_ENGINE_BLOCK_FILE_MANAGER_H_
#define LIB_CALCULATE_ENGINE_BLOCK_FILE_MANAGER_H_
#include <stdint.h>
#include "db_chain.h"
#include "MempRing.h"
#include <string>
#include <string.h>
#include <pthread.h>

using namespace std;

class block_file_manager
{
private:
    struct block_file_index_info
    {
        uint64_t file_id;
        int owners;
        chain_node cn;
        void * extern_info;
        char file_name[1];

    };
    friend class iterator;
public:
    enum search_type
    {
        BM_SEARCH_EQUAL,
        BM_SEARCH_BEFORE,
        BM_SEARCH_AFTER
    };
    class iterator
    {
    private:
    	  friend class block_file_manager;
        block_file_index_info * m_block;
        block_file_manager * m_manager;
        iterator();
    public:
        iterator(block_file_manager * manager,uint64_t id,search_type type);
        iterator(const iterator &itor);
        ~iterator();
        const char *get_filename();
        uint64_t get_id();
        void * get_extern_info();
        int get_owner();
        bool next();
        bool prev();
        bool valid();
    };
private:
    chain_head m_head;
    memp_ring m_mp;
    std::string m_index_file;
    int m_index_fd;
    pthread_mutex_t m_file_lock;
    int m_err;
    block_file_index_info **m_index;

    int m_index_count;
    int32_t m_index_start_pos;
    int32_t m_index_end_pos;
    int (*m_clear_extern_info_func)(void*,bool clearFile);
    pthread_rwlock_t m_lock;
    int32_t m_index_volumn;
    std::string m_dataDIR;
private:
    inline void rlock()
    {
        pthread_rwlock_rdlock(&m_lock);
    }
    inline void wlock()
    {
        pthread_rwlock_rdlock(&m_lock);
    }
    inline void unlock()
    {
        pthread_rwlock_unlock(&m_lock);
    }

    int append_to_index_file(block_file_index_info *b);
    int rewrite_index_file();
    //find需要加引用计数,用完需要释放
    block_file_index_info * _find(uint64_t id,int type);
    int clear_first();
    int  load_index_file();
public:
    block_file_manager(const char * index_filename,int (*clear_extern_info_func)(void*,bool) = NULL,const char * dataDir = NULL);
    virtual ~block_file_manager();
    /*
     * 所有clear操作必须串行，不支持并发
     * 所有insert操作必须串行，不支持并发
     * clear与insert可以并发
     * clear优先级最低，insert优先级稍高与clear，find优先级最高，优先保障find，尽可能减少find上的消耗
     */

    int clear_to(uint64_t id);

    int insert(const char *filename,void * extern_info = NULL);
    bool  find(uint64_t id,string &file_name);
    uint64_t get_max();
    const char *get_max_file();
    uint64_t get_min();
    const char *get_min_file();
    bool exist(uint64_t id);
    bool exist(const char * filename);
    int get_err(){return m_err;}
    iterator begin();
    iterator rbegin();
    int init_externinfo(int id,void * extern_info);
    static uint64_t get_id(const char * block_name)
    {
        const char * lastPointPos=strrchr(block_name,'.');
        if(lastPointPos==NULL)
            return -1;
        return atol(lastPointPos+1);
    }
};


#endif /* LIB_CALCULATE_ENGINE_BLOCK_FILE_MANAGER_H_ */
