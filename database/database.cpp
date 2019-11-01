#include "database.h"
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
namespace DATABASE {
	enum FLUSH_THREAD_STATUS {
		F_IDLE,
		F_RUNNING,
		F_JOIN_ABLE,
		F_DO_JOIN
	};
	DLL_EXPORT std::string database::updateConfig(const char* key, const char* value)
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
	DLL_EXPORT void database::commit()
	{
		if (likely(m_status == BLOCK_MANAGER_STATUS::BM_RUNNING))
			m_current->commit();
	}
	DLL_EXPORT database::database(const char* confPrefix, config* conf, bufferPool* pool, META::metaDataBaseCollection* metaDataCollection) :m_status(BLOCK_MANAGER_STATUS::BM_UNINIT), m_confPrefix(confPrefix), m_blocks(nullptr), m_maxBlockID(0)
		, m_config(conf), m_current(nullptr), m_pool(pool), m_metaDataCollection(metaDataCollection), m_threadPool(createThreadPool(32, this, &database::flushThread, std::string("databaseFlush_").append(confPrefix).c_str())), m_firstBlockId(0), m_lastBlockId(0), m_currentFlushThreadCount(0), m_recordId(0), m_tnxId(0)
	{
		initConfig();
	}
	DLL_EXPORT int database::insert(DATABASE_INCREASE::record* r)
	{
		if (unlikely(m_status != BLOCK_MANAGER_STATUS::BM_RUNNING))
			return -1;
		r->head->txnId = m_tnxId;
		r->head->recordId = m_recordId++;
		appendingBlock::appendingBlockStaus status = m_current->append(r);
		switch (status)
		{
		case appendingBlock::B_OK:
			return 0;
		case appendingBlock::B_FULL:
			if (!createNewBlock())
			{
				m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
				return -1;
			}
			return insert(r);
		case appendingBlock::B_FAULT:
			m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		case appendingBlock::B_ILLEGAL:
			m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed,record is illegal;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		default:
			m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed,unknown error;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		}
	}
	bool database::createNewBlock()
	{
		appendingBlock* newBlock = new appendingBlock(BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0),
			m_blockDefaultSize, m_redoFlushDataSize,
			m_redoFlushPeriod, m_recordId, this, m_metaDataCollection);

		newBlock->m_blockID = m_current->m_blockID + 1;
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
			if (m_status != BLOCK_MANAGER_STATUS::BM_RUNNING)
			{
				delete newBlock;
				return false;
			}
			m_threadPool.createNewThread();
		}
		m_blocks.set(newBlock->m_blockID, newBlock);
		m_lastBlockId.store(newBlock->m_blockID, std::memory_order_release);
		m_current = newBlock;
		return true;
	}
	DLL_EXPORT void* database::allocMemForRecord(META::tableMeta* table, size_t size)
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
	DLL_EXPORT bool database::checkpoint(uint64_t& timestamp, uint64_t &logOffset)
	{
		block* last = nullptr;
		uint32_t blockId = m_lastBlockId.load(std::memory_order_relaxed);
		if (blockId < 1)
			return false;
		while ((last = getBasciBlock(blockId)) != nullptr)
		{
			if (last->m_recordCount == 0)
			{
				last->unuse();
				blockId--;
				continue;
			}
			timestamp = last->m_maxTime;
			logOffset = last->m_maxLogOffset;
			last->unuse();
			return true;
		}
		return false;
	}
	DLL_EXPORT int database::compection(bool (*reduce)(const char*))//todo
	{
		return 0;
	}
	block* database::updateBasicBlockToSolidBlock(block* b)
	{
		while (unlikely(!(b->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID))))//it is a basic blcok
		{
			solidBlock* sb = new solidBlock(b);
			sb->use();
			if (unlikely(!m_blocks.updateCas(b->m_blockID, sb, b)))
			{
				delete sb;
				continue;
			}
			m_blockLock.unlock_shared();
			if (unlikely(0 != sb->load(b->m_blockID)))
			{
				LOG(ERROR) << "load block" << b->m_blockID << " failed";
				m_blocks.update(b->m_blockID, b);
				sb->unuse();
				sb->unuse();
				return nullptr;
			}
			b->unuse();
			return sb;
		}
		return b;
	}
	block* database::getBasciBlock(uint32_t blockId)
	{
		if (m_lastBlockId.load(std::memory_order_relaxed) < 1 || m_firstBlockId.load(std::memory_order_relaxed) > blockId || m_lastBlockId.load(std::memory_order_relaxed) < blockId)
			return nullptr;
		m_blockLock.lock_shared();
		block* b = static_cast<block*>(m_blocks.get(blockId));
		if (likely(b != nullptr && b->use()))
		{
			m_blockLock.unlock_shared();
			return b;
		}
		m_blockLock.unlock_shared();
		if (m_firstBlockId.load(std::memory_order_acquire) > blockId)
			return nullptr;
		block* tmp;
		b = block::loadFromFile(blockId, this,m_metaDataCollection);
		if (b == nullptr)
		{
			LOG(ERROR) << "load block" << blockId << " failed";
			return nullptr;
		}
		b->use();
		m_blockLock.lock_shared();
		if (m_firstBlockId.load(std::memory_order_acquire) > blockId)
			return nullptr;
	RESET:
		tmp = m_blocks.set(blockId, b);
		if ((!(tmp->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID))) && tmp == b)
		{
			m_blockLock.unlock_shared();
			return b;
		}
		else
		{
			if (!tmp->use())
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				goto RESET;
			}
			m_blockLock.unlock_shared();
			delete b;
			return tmp;
		}
	}

	block* database::getBlock(uint32_t blockId)
	{
		if (m_firstBlockId.load(std::memory_order_relaxed) > blockId || m_lastBlockId.load(std::memory_order_relaxed) < blockId)
			return nullptr;
		m_blockLock.lock_shared();
		block* b = static_cast<block*>(m_blocks.get(blockId));
		if (likely(b != nullptr && b->use()))
		{
			if (unlikely(!(b->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID))))//it is a basic blcok
			{
				b->unuse();
				return updateBasicBlockToSolidBlock(b);
			}
			m_blockLock.unlock_shared();
			return b;
		}
		m_blockLock.unlock_shared();
		if (m_firstBlockId.load(std::memory_order_acquire) > blockId)
			return nullptr;
		block* tmp;
		solidBlock* s = new solidBlock(this, m_metaDataCollection,0);
		s->m_blockID = blockId;
		if (s->loadFromFile()!=0)
		{
			LOG(ERROR) << "load block" << blockId << " failed";
			delete s;
			return nullptr;
		}
		s->use();
		m_blockLock.lock_shared();
	RESET:
		tmp = m_blocks.set(blockId, s);
		if ((tmp->m_flag & BLOCK_FLAG_APPENDING) || ((tmp->m_flag & BLOCK_FLAG_SOLID) && static_cast<solidBlock*>(tmp) != s))//has been setted,use this block
		{
			if (!tmp->use())//this block will been destroied
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				goto RESET;
			}
			else
			{
				m_blockLock.unlock_shared();
				delete s;
			}
		}
		else if (unlikely(!(tmp->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID))))
		{
			return updateBasicBlockToSolidBlock(b);
		}

		m_blockLock.unlock_shared();
		if (0 != s->load(blockId))
		{
			m_blockLock.lock();
			m_blocks.erase(blockId);
			m_blockLock.unlock();
			s->unuse();//this unuse to free use 
			s->unuse();//this unuse will destroy the block
			return nullptr;
		}
		return s;
	}
	int database::flush(appendingBlock* block)
	{
		solidBlock* sb = block->toSolidBlock();
		if (sb == nullptr)
		{
			LOG(ERROR) << "trans block" << block->m_blockID << " to solid block failed";
			return -1;
		}
		if (0 != sb->writeToFile())
		{
			LOG(ERROR) << "flush block" << block->m_blockID << " to solid block file failed";
			delete sb;
			return -1;
		}
		if (block->m_flag & BLOCK_FLAG_HAS_REDO)
		{
			block->finishRedo();
			char fileName[512];
			genBlockFileName(block->m_blockID, fileName);
			strcat(fileName, ".redo");
			remove(fileName);
		}
		assert(m_blocks.update(block->m_blockID, sb));
		block->unuse();
		return 0;
	}
	void database::flushThread()
	{
		appendingBlock* block;
		uint32_t idleRound = 0;
		while (likely(m_status == BLOCK_MANAGER_STATUS::BM_RUNNING))
		{
			if (!m_unflushedBlocks.pop(block, 1000))
			{
				if (++idleRound > 1000)
				{
					if (m_threadPool.quitIfThreadMoreThan(1))
						return;
				}
				continue;
			}
			if (0 != flush(block))
			{
				m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
				return;
			}
			idleRound >>= 1;
		}
	}
	int database::purge()
	{
		block* b;
		/*end is < m_lastBlockId.load(std::memory_order_relaxed) - 1,so we keep at least two block in disk*/
		for (uint32_t blockId = m_lastBlockId.load(std::memory_order_relaxed); blockId < m_lastBlockId.load(std::memory_order_relaxed) - 1; blockId++)
		{
			char fileName[512];
			genBlockFileName(blockId, fileName);
			if (checkFileExist(fileName, 0) == 0)
			{
				if (getFileTime(fileName) < time(nullptr) - m_outdated)
					return 0;
				m_blockLock.lock();
				if (nullptr != (b = static_cast<block*>(m_blocks.get(blockId))))
				{
					m_blocks.erase(blockId);
					b->unuse();
				}
				m_lastBlockId++;
				m_blockLock.unlock();
				if (0 != remove(fileName))
				{
					m_lastBlockId--;
					LOG(ERROR) << "remove block file:" << fileName << " failed for errno:" << errno << "," << strerror(errno);
					m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
					return -1;
				}
			}
			else
				m_lastBlockId++;
		}
		return 0;
	}
	int database::gc()//todo
	{
		for (block* b = static_cast<block*>(m_blocks.begin()); b != nullptr; b = static_cast<block*>(m_blocks.get(b->m_blockID + 1)))
		{
		}
		return 0;
	}

	void database::purgeThread(database* db)
	{
		db->purge();
	}
	int database::recoveryFromRedo(uint32_t from, uint32_t to)
	{
		LOG(WARNING) << "try recovery block " << from << " to " << to << " from redo";
		for (uint32_t blockId = from; blockId <= to; blockId++)
		{
			char fileName[512];
			genBlockFileName(blockId, fileName);
			strcat(fileName, ".redo");
			if (checkFileExist(fileName, 0) < 0)
			{
				LOG(WARNING) << "redo file of block " << blockId << " is not exist ";
				return 1;
			}
			appendingBlock* block = new appendingBlock(BLOCK_FLAG_APPENDING,
				m_blockDefaultSize, m_redoFlushDataSize,
				m_redoFlushPeriod, 0, this, m_metaDataCollection);
			block->m_blockID = blockId;
			int ret = block->recoveryFromRedo();
			if (ret == -1)
			{
				LOG(ERROR) << "block :" << blockId << " recory from redo failed ";
				delete block;
				return 1;
			}
			else if (ret == 1)
			{
				LOG(ERROR) << "block :" << blockId << " recory from redo success ,but this redo has not finish";
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
			remove(fileName);
			m_blocks.set(sb->m_blockID, sb);//todo ,avoid out of memory
			m_lastBlockId.store(blockId, std::memory_order_relaxed);
			LOG(ERROR) << "block :" << blockId << " recory from redo success ";
		}
		return 0;
	}

	int database::load()
	{
		int prefixSize = strlen(m_logPrefix);
		std::set<uint64_t> ids;
		std::set<uint64_t> redos;
		errno = 0;
#ifdef OS_WIN
		WIN32_FIND_DATA findFileData;
		std::string findString(m_logDir);
		findString.append("\\").append(m_logPrefix).append(".*");
		CreateDirectory(m_logDir, nullptr);
		HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
		if (INVALID_HANDLE_VALUE == hFind && errno != 0)
		{
			LOG(ERROR) << "open data dir:" << m_logDir << " failed,errno:" << errno << "," << strerror(errno);
			return -1;
		}
		if (INVALID_HANDLE_VALUE != hFind)
		{
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
					mkdir(m_logDir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
					goto CREATE_CURRENT;
					LOG(ERROR) << "open data dir:" << m_logDir << " failed,errno:" << errno << "," << strerror(errno);
					return -1;
				}
				dirent* file;
				while ((file = readdir(dir)) != nullptr)
				{
					if (file->d_type != 8)
						continue;
					const char* fileName = file->d_name;
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
					else if (strcmp(pos, ".redo") == 0)
						redos.insert(id);
#ifdef OS_WIN
			} while (FindNextFile(hFind, &findFileData));
			FindClose(hFind);
		}
#endif
#ifdef OS_LINUX
		}
		closedir(dir);
#endif
		if (ids.size() > 0 || redos.size() > 0)
		{
			int64_t prev = -1;
			char fileName[512];

			for (std::set<uint64_t>::iterator iter = ids.begin(); iter != ids.end(); iter++)
			{
				if (prev == -1)
				{
					m_firstBlockId.store(*iter, std::memory_order_relaxed);
					m_lastBlockId.store(*iter, std::memory_order_relaxed);
				}
				if (prev != -1 && *iter != (uint64_t)(prev + 1))
				{
					LOG(ERROR) << "block index is not increase strictly,block from:" << prev + 1 << " to " << *iter - 1 << " are not exist";
					int ret = recoveryFromRedo(prev + 1, *iter - 1);
					if (ret == 1)
					{
						LOG(ERROR) << "stop load remind block,and move remind blocks to .bak file";
						for (iter++; iter != ids.end(); iter++)
						{
							genBlockFileName(*iter, fileName);
							rename(fileName, std::string(fileName).append(".bak").c_str());
						}
						break;
					}
				}
				block* b = block::loadFromFile(*iter, this,m_metaDataCollection);
				if (nullptr == b)
				{
					LOG(ERROR) << "load block basic info from block:" << (*iter) << " failed,stop load remind block,and move remind block to .bak file";
					for (; iter != ids.end(); iter++)
					{
						genBlockFileName(*iter, fileName);
						rename(fileName, std::string(fileName).append(".bak").c_str());
					}
					break;
				}
				m_blocks.set(*iter, b);
				prev = *iter;
				m_lastBlockId.store(*iter, std::memory_order_relaxed);
			}
			if (!redos.empty() && *redos.rbegin() > m_lastBlockId.load(std::memory_order_relaxed))
			{
				recoveryFromRedo(m_lastBlockId.load(std::memory_order_relaxed), *redos.rbegin());
				for (std::set<uint64_t>::iterator iter = redos.begin(); iter != redos.end(); iter++)
				{
					if (m_current == nullptr || m_current->m_blockID != *iter)
					{
						genBlockFileName(*iter, fileName);
						rename(std::string(fileName).append(".redo").c_str(), std::string(fileName).append(".redo.bak").c_str());
					}
				}
			}
			block* last = m_blocks.end();
			if (likely(last != nullptr))
			{
				m_tnxId = last->m_maxTxnId + 1;
				m_recordId = last->m_minRecordId + last->m_recordCount;
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
		m_current = new appendingBlock(BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0), m_blockDefaultSize,
			m_redoFlushDataSize, m_redoFlushPeriod, m_recordId, this, m_metaDataCollection);
		m_current->m_blockID = m_lastBlockId.load(std::memory_order_relaxed) + 1;
		m_blocks.set(m_lastBlockId.load(std::memory_order_relaxed) + 1, m_current);
		m_lastBlockId.fetch_add(1, std::memory_order_relaxed);
		if (m_firstBlockId.load(std::memory_order_relaxed) == 0)
			m_firstBlockId.store(1, std::memory_order_relaxed);
		return 0;
	}
	int database::initConfig()
	{
		strncpy(m_logDir, m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_DIR), "data").c_str(), 256);

		strncpy(m_logPrefix, m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_PREFIX), "log").c_str(), 256);

		m_redo = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_REDO), "off") == "on");

		m_compress = (m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_COMPRESS), "on") == "on");

		m_blockDefaultSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_BLOCK_DEFAULT_SIZE), 33554432, 32 * 1024, 1024 * 1024 * 1024);

		m_redoFlushDataSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_DATA_SIZE), -1, -1, m_blockDefaultSize);

		m_redoFlushPeriod = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_REDO_FLUSH_PERIOD), -1, -1, 100000000);

		m_outdated = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_OUTDATED), 86400, 0, 864000);

		m_maxUnflushedBlock = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_UNFLUSHED_BLOCK), 8, 0, 1000);

		m_fileSysPageSize = m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_FILESYS_PAGE_SIZE), 512, 0, 1024 * 1024);

		m_threadPool.updateCurrentMaxThread(
			m_config->getLong(C_STORE_SCTION, REAL_CONF_STRING(C_MAX_FLUSH_THREAD), 4, 1, MAX_FLUSH_THREAD));
		return 0;
	}
}
