#include "blockManager.h"
#include "appendingBlock.h"
#include "solidBlock.h"
#ifdef OS_WIN
#include <io.h>
#endif
#ifdef OS_LINUX
#include <dirent.h>
#include <unistd.h>
#include <sys/types.h>  
#include <sys/stat.h>  
#endif
namespace STORE {
	enum FLUSH_THREAD_STATUS {
		F_IDLE,
		F_RUNNING,
		F_JOIN_ABLE,
		F_DO_JOIN
	};
	std::string blockManager::updateConfig(const char* key, const char* value)
	{
		if (strcmp(key, REAL_CONF_STRING(C_LOG_DIR)) == 0)
			return "config :logDir is static,can not change";
		else if (strcmp(key, REAL_CONF_STRING(C_LOG_PREFIX)) == 0)
			return "config :logPrefix is static,can not change";
		else if (strcmp(key, REAL_CONF_STRING(C_REDO)) == 0)
		{
			if (strcmp(value, "on"))
				m_redo = true;
			else if (strcmp(value, "off"))
				m_redo = false;
			else
				return "value of config :redo must be [on] or [off]";
		}
		else if (strcmp(key, REAL_CONF_STRING(C_COMPRESS)) == 0)
		{
			if (strcmp(value, "on"))
				m_compress = true;
			else if (strcmp(value, "off"))
				m_compress = false;
			else
				return "value of config :compress must be [on] or [off]";
		}
		else if (strcmp(key, REAL_CONF_STRING(C_BLOCK_DEFAULT_SIZE)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0'))
					return "value of config :blockDefaultSize must be a number";
			}
			m_blockDefaultSize = atol(value);
		}
		else if (strcmp(key, REAL_CONF_STRING(C_REDO_FLUSH_DATA_SIZE)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0') && !(idx == 0 && value[idx] == '-'))
					return "value of config :redoFlushDataSize must be a number";
			}
			m_redoFlushDataSize = atol(value);
		}
		else if (strcmp(key, REAL_CONF_STRING(C_REDO_FLUSH_PERIOD)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0') && !(idx == 0 && value[idx] == '-'))
					return "value of config :redoFlushPeriod must be a number";
			}
			m_redoFlushPeriod = atol(value);
		}
		else if (strcmp(key, REAL_CONF_STRING(C_OUTDATED)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0'))
					return "value of config :outdated must be a number";
			}
			m_outdated = atol(value);
		}
		else if (strcmp(key, REAL_CONF_STRING(C_MAX_UNFLUSHED_BLOCK)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0'))
					return "value of config :maxUnflushedBlock must be a number";
			}
			m_maxUnflushedBlock = atol(value);

		}
		else if (strcmp(key, REAL_CONF_STRING(C_FILESYS_PAGE_SIZE)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0'))
					return "value of config :fileSysPageSize must be a number";
			}
			m_fileSysPageSize = atol(value);
		}
		else if (strcmp(key, REAL_CONF_STRING(C_MAX_FLUSH_THREAD)) == 0)
		{
			for (int idx = strlen(value) - 1; idx >= 0; idx--)
			{
				if ((value[idx] > '9' || value[idx] < '0'))
					return "value of config :maxFlushThread must be a number";
			}
			if (atol(value) > MAX_FLUSH_THREAD)
				return "value of config :maxFlushThread is to large";
			m_threadPool.updateCurrentMaxThread(
				m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_FLUSH_THREAD), 4, 1, MAX_FLUSH_THREAD));
		}
		else
			return std::string("unknown config:") + key;
		m_config->set("store", key, value);
		return std::string("update config:") + key + " success";
	}
	int blockManager::insert(DATABASE_INCREASE::record* r)
	{
		if (unlikely(m_status != BM_RUNNING))
			return -1;
		appendingBlock::appendingBlockStaus status = m_current->append(r);
		switch (status)
		{
		case appendingBlock::B_OK:
			return 0;
		case appendingBlock::B_FULL:
			if (!createNewBlock())
			{
				m_status = BM_FAILED;
				return -1;
			}
			return insert(r);
		case appendingBlock::B_FAULT:
			m_status = BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		case appendingBlock::B_ILLEGAL:
			m_status = BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed,record is illegal;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		default:
			m_status = BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed,unknown error;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		}
	}
	bool blockManager::createNewBlock()
	{
		appendingBlock* newBlock = new appendingBlock(BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0),
			m_blockDefaultSize, m_redoFlushDataSize,
			m_redoFlushPeriod, m_current->lastRecordId() + 1, this, m_metaDataCollection);

		newBlock->m_blockID = m_current->m_blockID+1;
		m_current->m_flag |= BLOCK_FLAG_FINISHED;
		if (m_current->m_flag & BLOCK_FLAG_HAS_REDO)
		{
			if (m_current->finishRedo() != 0)
			{
				delete newBlock;
				return false;
			}
		}
		while (!m_unflushedBlocks.push(m_current, 1000))
		{
			if (m_status!=BM_RUNNING)
			{
				delete newBlock;
				return false;
			}
			m_threadPool.createNewThread();
		}
		m_blocks.set(newBlock->m_blockID, newBlock);
		m_lastBlockId.store(newBlock->m_blockID, std::memory_order_acquire);
		m_current = newBlock;
		return true;
	}
	void* blockManager::allocMemForRecord(META::tableMeta* table, size_t size)
	{
		void* mem;
		appendingBlock::appendingBlockStaus status = m_current->allocMemForRecord(table, size, mem);
		if (likely(status == appendingBlock::B_OK))
			return mem;
		if (status == appendingBlock::B_FULL)
		{
			m_current->m_flag |= BLOCK_FLAG_FINISHED;
			if (!createNewBlock())
				return nullptr;
			return allocMemForRecord(table, size);
		}
		else
			return nullptr;
	}
	block* blockManager::checkBlockCanUse(block* b)
	{
		while (b->m_loading.load(std::memory_order_relaxed) == BLOCK_LOADING)
			std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
		if ((b->m_loading.load(std::memory_order_relaxed) == BLOCK_LOAD_FAILED))
		{
			b->unuse();
			return nullptr;
		}
		return b;
	}

	block* blockManager::getBlock(uint32_t blockId)
	{
		if (m_firstBlockId.load(std::memory_order_relaxed) > blockId || m_lastBlockId.load(std::memory_order_relaxed) < blockId)
			return nullptr;
		m_blockLock.lock_shared();
		block* b = static_cast<block*>(m_blocks.get(blockId));
		if (likely(b != nullptr && b->use()))
		{
			m_blockLock.unlock_shared();
			return checkBlockCanUse(b);
		}
		m_blockLock.unlock_shared();
		if (m_firstBlockId.load(std::memory_order_acquire) > blockId)
			return nullptr;
		block* tmp;
		solidBlock* s;
		s = new solidBlock(this, m_metaDataCollection);
		s->m_loading.store(BLOCK_LOADING, std::memory_order_acquire);
		m_blockLock.lock_shared();
RESET:
		tmp = static_cast<block*>(m_blocks.set(blockId, s));
		if (tmp->m_flag & BLOCK_FLAG_APPENDING || static_cast<solidBlock*>(tmp) != s)//has been setted,use this block
		{
			if (!tmp->use())//this block will been destroy
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				goto RESET;
			}
			else
			{
				m_blockLock.unlock_shared();
				delete s;
				checkBlockCanUse(tmp);
			}
		}
		s->use();
		m_blockLock.unlock_shared();
		if (0 != s->load(blockId))
		{
			s->m_loading.store(BLOCK_LOAD_FAILED, std::memory_order_acquire);
			m_blockLock.lock();
			m_blocks.erase(blockId);
			m_blockLock.unlock();
			s->unuse();
			s->unuse();
			return nullptr;
		}
		s->m_loading.store(BLOCK_NORMAL, std::memory_order_acquire);
		return s;
	}
	int blockManager::flush(appendingBlock* block)
	{
		solidBlock* sb = block->toSolidBlock();
		if (sb == nullptr)
		{
			LOG(ERROR) << "trans block" << block->m_blockID << " to solid block failed";
			return -1;
		}
		delete block;
		if (0 != sb->writeToFile())
		{
			LOG(ERROR) << "flush block" << block->m_blockID << " to solid block file failed";
			delete sb;
			return -1;
		}
		if (block->m_flag & BLOCK_FLAG_HAS_REDO)
		{
			block->finishRedo();
			remove(getBlockFile(block->m_blockID).append(".redo").c_str());
		}
		assert(m_blocks.update(block->m_blockID, sb));
		block->unuse();
		return 0;
	}
	void blockManager::flushThread()
	{
		appendingBlock* block;
		uint32_t idleRound = 0;
		while (m_status==BM_RUNNING&&idleRound<1000)
		{
			if (!m_unflushedBlocks.pop(block, 10))
			{
				idleRound++;
				continue;
			}
			if (0 != flush(block))
			{
				m_status = BM_FAILED;
				return;
			}
			idleRound >>= 1;
		}
	}
	int blockManager::purge()
	{
		block* b;
		for (uint32_t blockId = m_lastBlockId.load(std::memory_order_relaxed); blockId < m_lastBlockId.load(std::memory_order_relaxed) - 1; blockId++)
		{
			std::string fileName = getBlockFile(blockId);
			if (checkFileExist(fileName.c_str(),0) == 0)
			{
				if(getFileTime(fileName.c_str())
				m_blockLock.lock();
				if (nullptr == (b = static_cast<block*>(m_blocks.get(blockId))))
				{
					m_lastBlockId++;
					remove(getBlockFile(blockId).c_str());
				}
			}
		}
	}

	static void purgeThread(blockManager* m)
	{

	}
	int blockManager::recoveryFromRedo(uint32_t from,uint32_t to)
	{
		for (uint32_t blockId = from; blockId <= to; blockId++)
		{	
			if (checkFileExist(getBlockFile(blockId).append(".redo").c_str(), 0) < 0)
				return 1;
			appendingBlock* block = new appendingBlock(BLOCK_FLAG_APPENDING,
				m_blockDefaultSize, m_redoFlushDataSize,
				m_redoFlushPeriod, 0, this, m_metaDataCollection);
			block->m_blockID = blockId;
			int ret = block->recoveryFromRedo();
			if (ret == -1)
			{
				delete block;
				return 1;
			}
			else if (ret == 1)
			{
				m_blocks.set(block->m_blockID, block);
				m_current = block;
				m_lastBlockId.store(blockId, std::memory_order_relaxed);
				return 1;
			}
			solidBlock* sb = block->toSolidBlock();
			if (sb == nullptr)
			{
				LOG(ERROR) << "recory from redo failed fo trans block" << blockId << " to solid block failed";
				return 1;
			}
			delete block;
			if (0 != sb->writeToFile())
			{
				LOG(ERROR) << "recory from redo failed fo flush block" << blockId << " to solid block file failed";
				delete sb;
				return 1;
			}
			remove(getBlockFile(blockId).append(".redo").c_str());
			m_blocks.set(sb->m_blockID, sb);//todo ,avoid out of memory
			m_lastBlockId.store(blockId, std::memory_order_relaxed);
		}
		return 0;
	}

	int blockManager::load()
	{
		int prefixSize = strlen(m_logPrefix);
		std::set<uint64_t> ids;
		std::set<uint64_t> redos;
#ifdef OS_WIN
		WIN32_FIND_DATA findFileData;
		std::string findString(m_logDir);
		findString.append("\\").append(m_logPrefix).append(".*");
		HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
		if (INVALID_HANDLE_VALUE == hFind)
		{
			LOG(ERROR) << "open data dir:" << m_logDir << " failed,errno:" << errno << "," << strerror(errno);
			return -1;
		}
		do
		{
			if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
				continue;
			const char* fileName = findFileData.cFileName;
#endif
#ifdef OS_LINUX
			DIR* dir = opendir(m_logDir);
			if (dir == nullptr)
			{
				LOG(ERROR) << "open data dir:" << m_logDir << " failed,errno:" << errno << "," << strerror(errno);
				return -1;
			}
			dirent* file;
			while ((file = readdir(dir)) != nullptr)
			{
				if (ptr->d_type != 8)
					continue;
				const char* fileName = ptr->d_name;
#endif
				if (strncmp(fileName, m_logPrefix, prefixSize) != 0)
					continue;
				if (fileName[prefixSize] != '.')
					continue;
				const char* pos = fileName + prefixSize + 1;
				uint64_t id = 0;
				while (*pos <= '9' && *pos >= '0')
				{
					id = id * 10 + *pos - '0';
					pos++;
				}
				if (*pos == '\0')
					ids.insert(id);
				else if(strcmp(pos, ".redo") == 0)
					redos.insert(id);
#ifdef OS_WIN
			}while (FindNextFile(hFind, &findFileData));
			FindClose(hFind);
#endif
#ifdef OS_LINUX
		}
		closedir(dir);
#endif
		if (ids.size() > 0||redos.size()>0)
		{
			int64_t prev = -1;
			for (std::set<uint64_t>::iterator iter = ids.begin(); iter != ids.end(); iter++)
			{
				if (prev == -1)
				{
					m_firstBlockId.store(*iter, std::memory_order_relaxed);
					m_lastBlockId.store(*iter, std::memory_order_relaxed);
				}
				if (prev != -1 && *iter != prev + 1)
				{
					int ret = recoveryFromRedo(prev + 1, *iter - 1);
					if (ret == 1)
					{
						for (iter++; iter != ids.end(); iter++)
							rename(getBlockFile(*iter).c_str(), getBlockFile(*iter).append(".bak").c_str());
						break;
					}
				}
				prev = *iter;
				m_lastBlockId.store(*iter, std::memory_order_relaxed);
			}
			if (!redos.empty() && *redos.rbegin() > m_lastBlockId.load(std::memory_order_relaxed))
			{
				recoveryFromRedo(m_lastBlockId.load(std::memory_order_relaxed), * redos.rbegin());
				for (std::set<uint64_t>::iterator iter = redos.begin(); iter != redos.end(); iter++)
				{
					if (m_current == nullptr || m_current->m_blockID != *iter)
						rename(getBlockFile(*iter).append(".redo").c_str(), getBlockFile(*iter).append(".redo.bak").c_str());
				}
			}
			if (m_current == nullptr)
				goto CREATE_CURRENT;
			return 0;
		}
		else //empty block dir
		{
			m_firstBlockId.store(0, std::memory_order_relaxed);
			m_lastBlockId.store(0, std::memory_order_relaxed);
		}
	CREATE_CURRENT:
		uint64_t prevLastRecordId = 0;
		if (m_lastBlockId.load(std::memory_order_relaxed) > 0)
		{
			block* sb = getBlock(m_lastBlockId.load(std::memory_order_relaxed));
			if (sb == nullptr)
			{
				LOG(ERROR) << "load blocks failed for load " << m_lastBlockId.load(std::memory_order_relaxed) << " to failed";
				return -1;
			}
			prevLastRecordId = sb->m_minRecordId + sb->m_recordCount;
		}
		m_current = new appendingBlock(BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0), m_blockDefaultSize,
			m_redoFlushDataSize, m_redoFlushPeriod, prevLastRecordId+1, this, m_metaDataCollection);
		m_current->m_blockID = m_lastBlockId.load(std::memory_order_relaxed) + 1;
		m_blocks.set(m_lastBlockId.load(std::memory_order_relaxed) + 1, m_current);
		m_lastBlockId.fetch_add(1, std::memory_order_relaxed);
		if(m_firstBlockId.load(std::memory_order_relaxed)==0)
			m_firstBlockId.store(1, std::memory_order_relaxed);
		return 0;
	}
	int blockManager::initConfig()
	{
		strncpy(m_logDir,m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_DIR), "data").c_str(),256);

		strncpy(m_logPrefix, m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_PREFIX), "log.").c_str(),256);

		m_redo = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_REDO), "off") == "on");

		m_compress = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_COMPRESS), "off") == "on");

		m_blockDefaultSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_BLOCK_DEFAULT_SIZE), 33554432, 32 * 1024, 8 * 1024 * 1024 * 1024);

		m_redoFlushDataSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_DATA_SIZE), -1,-1, m_blockDefaultSize);

		m_redoFlushPeriod = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_PERIOD), -1,-1,100000000);

		m_outdated = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_OUTDATED), 86400,0, 864000);

		m_maxUnflushedBlock = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_UNFLUSHED_BLOCK), 8,0,1000);

		m_fileSysPageSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_FILESYS_PAGE_SIZE), 512,0,1024*1024);

		m_threadPool.updateCurrentMaxThread(
			m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_FLUSH_THREAD), 4, 1, MAX_FLUSH_THREAD));
		return 0;
	}
}
