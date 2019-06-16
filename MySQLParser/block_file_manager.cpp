/*
 * block_file_manager.cpp
 *
 *  Created on: 2017年3月10日
 *      Author: liwei
 */

#include "block_file_manager.h"
#include "file_opt.h"
#include <pthread.h>
#include <fcntl.h>
#include <error.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include "atomic.h"
block_file_manager::iterator::iterator(block_file_manager * manager,
        uint64_t id, block_file_manager::search_type type)
{
    m_block = manager->_find(id, type);
    m_manager = manager;
}
block_file_manager::iterator::iterator()
{
    m_block = NULL;
    m_manager = NULL;
}
block_file_manager::iterator::iterator(const iterator &itor)
{
    m_block=itor.m_block;
    if(m_block!=NULL)
        atomic_inc((atomic_t*)&m_block->owners);
    m_manager=itor.m_manager;
}
block_file_manager::iterator::~iterator()
{
    if (m_block != NULL)
        atomic_sub(1, (atomic_t*)&m_block->owners);
}
const char *block_file_manager::iterator::get_filename()
{
    return m_block->file_name;
}
uint64_t block_file_manager::iterator::get_id()
{
    return m_block->file_id;
}
void * block_file_manager::iterator::get_extern_info()
{
	return m_block->extern_info;
}
int block_file_manager::iterator::get_owner()
{
	return atomic_read((atomic_t*)&m_block->owners);
}
bool block_file_manager::iterator::next()
{
    block_file_index_info *next =
            get_next_dt(m_block, block_file_index_info, cn);
    if (&next->cn ==  &m_manager->m_head.head)
    {
    	atomic_dec((atomic_t*)&m_block->owners);
    	m_block = NULL;
    	return false;
    }
    atomic_inc((atomic_t*)&next->owners); //先增加next的引用计数,由于持有m_block的引用计数,删除只能从后往前，next是安全的
    atomic_dec((atomic_t*)&m_block->owners); //释放掉当前的引用计数
    m_block = next;
    return true;
}
bool block_file_manager::iterator::prev()
{
    block_file_index_info *prev =
            get_prev_dt(m_block, block_file_index_info, cn);

    if (&prev->cn ==  &m_manager->m_head.head
            || __atomic_add_unless((atomic_t*)&prev->owners, 1, -1) == -1)
        return false;
    atomic_dec((atomic_t*)&m_block->owners);
    m_block = prev;
    return true;
}
bool block_file_manager::iterator::valid()
{
    return m_block != NULL;
}

int block_file_manager::append_to_index_file(block_file_index_info *b)
{
    if (m_index_fd <= 0 || m_err != 0)
        return -1;
    int len = strlen(b->file_name);
    b->file_name[len] = '\n';
    int wlen = 0;
    pthread_mutex_lock(&m_file_lock);
    int offset = lseek(m_index_fd, 0, SEEK_END);
    if ((wlen = file_write(m_index_fd, (unsigned char*) b->file_name, len + 1))
            != len + 1)
    {
        m_err = errno;
        if (wlen > 0)
        {
            if (0 != ftruncate(m_index_fd, offset)) //写入了残缺的部分，需要回滚掉
            {
                //todo print log
            }
        }
    }
    else
        fsync(m_index_fd);
    pthread_mutex_unlock(&m_file_lock);
    b->file_name[len] = '\0';
    return m_err;
}
int block_file_manager::rewrite_index_file()
{
    if (m_err != 0)
        return m_err;
    pthread_mutex_lock(&m_file_lock);
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        if (0 != ftruncate(m_index_fd, 0))
        {
            pthread_mutex_unlock(&m_file_lock);
            m_err = errno;
            return m_err;
        }
        fsync(m_index_fd);
        pthread_mutex_unlock(&m_file_lock);
        return 0;
    }
    char file_buf[512];
    int file_buf_off = 0, all_size = 0;
    lseek(m_index_fd, 0, SEEK_SET);
    for (int32_t idx = m_index_start_pos; idx <= m_index_end_pos; idx++)
    {
        int len = strlen(m_index[idx]->file_name);
        if (len + file_buf_off >= 511)
        {
            if ((file_write(m_index_fd, (unsigned char*) file_buf, file_buf_off))
                    != file_buf_off)
            {
                m_err = errno;
                if (0 != ftruncate(m_index_fd, all_size)) //写入了残缺的部分，需要回滚掉
                {
                    //todo print log
                }
                break;
            }
            all_size += file_buf_off;
            file_buf_off = 0;
        }
        memcpy(&file_buf[file_buf_off], m_index[idx]->file_name, len);
        file_buf[file_buf_off + len] = '\n';
        file_buf_off += len + 1;
    }
    if (file_buf_off != 0 && m_err == 0)
    {
        if ((file_write(m_index_fd, (unsigned char*) file_buf, file_buf_off))
                != file_buf_off)
            m_err = errno;
        else
            all_size += file_buf_off;
    }
    if (m_err == 0)
    {
        if (0 != ftruncate(m_index_fd, all_size))
        {
            m_err = errno;
            //todo print log
        }
        else
            fsync(m_index_fd);
    }
    pthread_mutex_unlock(&m_file_lock);
    return 0;
}
//find需要加引用计数,用完需要释放
block_file_manager::block_file_index_info * block_file_manager::_find(uint64_t id, int type)
{
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
        return NULL;
    }
    int start = m_index_start_pos, end = m_index_end_pos, idx=0;
    while (start <= end)
    {
        idx = (start + end) >> 1;
        if (m_index[idx]->file_id > id)
            end = idx - 1;
        else if (m_index[idx]->file_id < id)
            start = idx + 1;
        else
        {
        	/*引用计数在加一后还是<=0,意味着这个block已经处于删除状态*/
            if(atomic_add_return(1,(atomic_t*)&m_index[idx]->owners)<=0)
            {
            	atomic_dec((atomic_t*)&m_index[idx]->owners);
            	unlock();
            	return NULL;
            }
            unlock();
            return m_index[idx];
        }
    }
    if (type == BM_SEARCH_BEFORE)
    {
        if (start > end)
            start = end;
        if (start < m_index_start_pos)
        {
            unlock();
            return NULL;
        }
        if(atomic_add_return(1,(atomic_t*)&m_index[idx]->owners)<=0)
        {
        	atomic_dec((atomic_t*)&m_index[idx]->owners);
        	unlock();
        	return NULL;
        }
        unlock();
        return m_index[idx];
    }
    else if (type == BM_SEARCH_AFTER)
    {
        if (start > m_index_end_pos)
        {
            unlock();
            return NULL;
        }
        if(atomic_add_return(1,(atomic_t*)&m_index[idx]->owners)<=0)
        {
        	atomic_dec((atomic_t*)&m_index[idx]->owners);
        	unlock();
        	return NULL;
        }
        unlock();
        return m_index[start];
    }
    else
    {
        unlock();
        return NULL;
    }
}
int block_file_manager::clear_first()
{
    if (atomic_read((atomic_t*)&m_index_count) <= 1) //方便低锁设计，只剩下一个block时，不允许删除
        return 0;
    block_file_index_info * first = m_index[m_index_start_pos], *second =
            get_next_dt(first, block_file_index_info, cn);
    /*
     * 切断了位于second的iterator通过prev方法获得first的路径，但保留位于first上的iterator通过next获得second的路径
     * 自此除了当前正位于first上的iterator能继续使用first外，将不会再有iterator占有first
     */
    second->cn.prev = &m_head.head;
    while (atomic_sub_return(1, (atomic_t*)&first->owners) != -1) //在确认first上没有使用者后，才可以开始删除
    {
        atomic_inc((atomic_t*)&first->owners);
        usleep(1000);
    }
    //在有超过一个以上的block时，c_delete_node是安全的，但m_head的count将不准却，因为不会依赖count，所以无关紧要
    c_delete_node(&m_head, &first->cn);
    remove(m_dataDIR.empty()?first->file_name:(m_dataDIR+"/"+first->file_name).c_str()); //todo
    atomic_dec((atomic_t*)&m_index_count);

    /*
     * 只在更新m_index时才会加锁，对iterator的遍历不会有影响，但会影响创建iterator，不过用时非常短，影响有限
     */
    wlock();
    m_index_start_pos = (m_index_start_pos + 1);
    if (((atomic_t*)&m_index_count)->counter < m_index_volumn / 2 && ((atomic_t*)&m_index_count)->counter > 4096)
    {
        block_file_index_info **tmp =
                (block_file_index_info**) malloc(sizeof(block_file_index_info*)
                        * (((atomic_t*)&m_index_count)->counter + ((atomic_t*)&m_index_count)->counter / 2));
        m_index_volumn = ((atomic_t*)&m_index_count)->counter + ((atomic_t*)&m_index_count)->counter / 2;
        memcpy(tmp, &m_index[m_index_start_pos], sizeof(block_file_index_info*)
                * ((atomic_t*)&m_index_count)->counter);
        free(m_index);
        m_index = tmp;
        m_index_start_pos = 0;
        m_index_end_pos = ((atomic_t*)&m_index_count)->counter - 1;
    }
    else if (m_index_start_pos > m_index_volumn / 4 && m_index_start_pos > 256)
    {
        memcpy(m_index, &m_index[m_index_start_pos], ((atomic_t*)&m_index_count)->counter
                * sizeof(block_file_index_info*));
        m_index_start_pos = 0;
        m_index_end_pos = ((atomic_t*)&m_index_count)->counter - 1;
    }
    unlock();
    if(first->extern_info&&m_clear_extern_info_func)
    {
    	m_clear_extern_info_func(first->extern_info,true);
    	first->extern_info = NULL;
    }
    ring_free(&m_mp, first);
    return 0;
}
int block_file_manager::load_index_file()
{
    char file_name_buf[512];
    int read_size, remainder_size = 0;
    if (m_err || m_index_fd <= 0)
        return -1;
    lseek(m_index_fd, 0, SEEK_SET);
    for (;;)
    {
        read_size =
                file_read(m_index_fd, (unsigned char *) &file_name_buf[remainder_size], 511-remainder_size);
        if ((read_size += remainder_size) <= 0)
            return 0;
        file_name_buf[read_size]='\0';
        remainder_size = read_size;
        while (remainder_size > 0)
        {
            char * pos_start = &file_name_buf[read_size - remainder_size], *pos_end = strchr(pos_start, '\n');
            if (pos_end == NULL)
            {
                memcpy(file_name_buf, pos_start, remainder_size);
                break;
            }
            int len = pos_end - pos_start;
            block_file_index_info * b =
                    (block_file_index_info*) ring_alloc(&m_mp, sizeof(block_file_index_info)
                            + len);
            memcpy(b->file_name, pos_start, len);
            b->file_name[len] = '\0';
            b->file_id = get_id(b->file_name);
            b->extern_info = NULL;
            atomic_set((atomic_t*)&b->owners, 0);
            c_insert_in_end(&m_head, &b->cn);
            if (++m_index_end_pos >= m_index_volumn)
                m_index =
                        (block_file_index_info**) realloc(m_index, (m_index_volumn =
                                m_index_volumn * 2)
                                        * sizeof(block_file_index_info*));
            m_index[m_index_end_pos] = b;
            atomic_inc((atomic_t*)&m_index_count);
            remainder_size -= len + 1;
        }
    }
    return 0;
}
block_file_manager::block_file_manager(const char * index_filename,int (*clear_extern_info_func)(void*,bool),const char * dataDir)
{
    c_init_chain(&m_head);
    init_memp_ring(&m_mp, 128 * 1024);
    m_index_file = index_filename;
    pthread_rwlock_init(&m_lock, NULL);
    pthread_mutex_init(&m_file_lock, NULL);
    m_index_fd =
            open(m_index_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    m_err = 0;
    if (m_index_fd <= 0) //todo print log
        m_err = errno;
    m_index_volumn = 1024;
    m_index_start_pos = 0;
    m_index_end_pos = -1;
    atomic_set((atomic_t*)&m_index_count, 0);
    m_index = (block_file_index_info**) malloc(sizeof(block_file_index_info*)
            * m_index_volumn);
    m_clear_extern_info_func = clear_extern_info_func;
    if(dataDir!=NULL)
    	m_dataDIR = dataDir;
    load_index_file();
}
block_file_manager::~block_file_manager()
{
    rlock();
    rewrite_index_file();
    unlock();
    if(m_clear_extern_info_func!=NULL)
    {
        for(block_file_manager::iterator iter = begin();iter.valid();iter.next())
        {
        	void * extern_info = iter.get_extern_info();
        	if(extern_info!=NULL)
        		m_clear_extern_info_func(extern_info,false);
        }
    }
    if (m_index_fd > 0)
        close(m_index_fd);
    free(m_index);
    destroy_memp_ring(&m_mp);
    pthread_rwlock_destroy(&m_lock);
    pthread_mutex_destroy(&m_file_lock);
}
/*
 * 所有clear操作必须串行，不支持并发
 * 所有insert操作必须串行，不支持并发
 * clear与insert可以并发
 * clear优先级最低，insert优先级稍高与clear，find优先级最高，优先保障find，尽可能减少find上的消耗
 */

int block_file_manager::clear_to(uint64_t id)
{
    while (atomic_read((atomic_t*)&m_index_count) != 0)
    {
        rlock();
        if (m_index[m_index_start_pos]->file_id <= id)
        {
            unlock();
            if (0 != clear_first())
            {
                rlock();
                rewrite_index_file();
                unlock();
                return -1;
            }
        }
        else
        {
            rewrite_index_file();
            unlock();
            return 0;
        }
    }
    rlock();
    rewrite_index_file();
    unlock();
    return 0;
}

int block_file_manager::insert(const char *filename,void * extern_info)
{
    uint64_t id = get_id(filename);

    int len = strlen(filename);
    block_file_index_info * b = (block_file_index_info*)ring_alloc(&m_mp, sizeof(block_file_index_info) + len);
    b->extern_info = extern_info;
    b->file_id = get_id(filename);
    atomic_set((atomic_t*)&b->owners, 0);
    memcpy(b->file_name, filename, len);
    b->file_name[len] = '\0';

    wlock();
    if (((atomic_t*)&m_index_count)->counter != 0 && id <= m_index[m_index_end_pos]->file_id)
    {
        unlock();
        ring_free(&m_mp,b);
        return -1;
    }
    if (m_index_end_pos >= m_index_volumn - 1)
    {
        /*
         * memcpy在比较久的未来可能有一定风险，如果以后新的指令集或者其他新技术使得memcpy一次copy了超过256*sizeof(block_file_index_info*)的长度会导致异常
         */
        if (m_index_start_pos > m_index_volumn / 4 && m_index_start_pos > 256)
        {
            memcpy(m_index, &m_index[m_index_start_pos], ((atomic_t*)&m_index_count)->counter
                    * sizeof(block_file_index_info*));
            m_index_start_pos = 0;
            m_index_end_pos = ((atomic_t*)&m_index_count)->counter - 1;
        }
        else
            m_index =
                    (block_file_index_info**) realloc(m_index, (m_index_volumn =
                            m_index_volumn * 2)
                                    * sizeof(block_file_index_info*));
    }
    if (0 != append_to_index_file(b))
    {
        unlock();
        ring_free(&m_mp,b);
        return -2;
    }
    c_insert_in_end(&m_head, &b->cn); //自此iterator能遍历到
    m_index[++m_index_end_pos] = b; //find可以找到
    atomic_inc((atomic_t*)&m_index_count);
    unlock();
    return 0;
}
bool block_file_manager::find(uint64_t id, string &file_name)
{
    block_file_index_info * b = _find(id, BM_SEARCH_EQUAL);
    if (b == NULL)
    	return false;
    file_name = b->file_name;
    atomic_sub(1, (atomic_t*)&b->owners);
    return true;
}
uint64_t block_file_manager::get_max()
{
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
        return ULONG_MAX;
    }
    else
    {
        uint64_t id =m_index[m_index_end_pos]->file_id;
        unlock();
        return id;
    }
}
const char* block_file_manager::get_max_file()
{
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
        return NULL;
    }
    else
    {
        const char * file =m_index[m_index_end_pos]->file_name;
        unlock();
        return file;
    }
}
uint64_t block_file_manager::get_min()
{
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
        return ULONG_MAX;
    }
    else
    {
        uint64_t id =m_index[m_index_start_pos]->file_id;
        unlock();
        return id;
    }
}
const char* block_file_manager::get_min_file()
{
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
        return NULL;
    }
    else
    {
        const char* file =m_index[m_index_start_pos]->file_name;
        unlock();
        return file;
    }
}
bool block_file_manager::exist(uint64_t id)
{
    block_file_index_info * b;
    if ((b = _find(id, BM_SEARCH_EQUAL)) == NULL)
        return false;
    atomic_dec((atomic_t*)&b->owners);
    return true;
}
bool block_file_manager::exist(const char * filename)
{
    block_file_index_info * b;
    if ((b = _find(get_id(filename), BM_SEARCH_EQUAL)) == NULL)
        return false;
    atomic_dec((atomic_t*)&b->owners);
    return true;
}
block_file_manager::iterator block_file_manager::begin()
{
	block_file_manager::iterator iter;
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
    }
    else
    {
    	block_file_index_info *info =m_index[m_index_start_pos];

        while(atomic_add_return(1,(atomic_t*)&info->owners)<=0)
        {
        	atomic_dec((atomic_t*)&info->owners);
        	if(c_is_end(&m_head,&info->cn))
        	{
            	unlock();
            	return iter;
        	}
        	info=get_next_dt(info, block_file_index_info, cn);
        }
    	iter.m_block = info;
    	iter.m_manager = (block_file_manager*)this;
    }
    return iter;
}
block_file_manager::iterator block_file_manager::rbegin()
{
	block_file_manager::iterator iter;
    rlock();
    if (((atomic_t*)&m_index_count)->counter == 0)
    {
        unlock();
    }
    else
    {
        if(atomic_add_return(1,(atomic_t*)&m_index[m_index_end_pos]->owners)<=0)
        {
        	atomic_dec((atomic_t*)&m_index[m_index_end_pos]->owners);
        	unlock();
        	return iter;
        }
    	iter.m_block = m_index[m_index_end_pos];
    	iter.m_manager = (block_file_manager*)this;
    }
    return iter;
}
int block_file_manager::init_externinfo(int id,void * extern_info)
{
	block_file_index_info * b = _find(id, BM_SEARCH_EQUAL);
	if(b==NULL)
	{
		return -1;
	}
	if(b->extern_info&&m_clear_extern_info_func)
		m_clear_extern_info_func(b->extern_info,false);
	b->extern_info = extern_info;
	atomic_sub(1, (atomic_t*)&b->owners);
	return 0;
}
