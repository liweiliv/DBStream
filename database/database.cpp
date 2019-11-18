#include "database.h"
#include "appendingBlock.h"
#include "solidBlock.h"
#include "kWaySortIterator.h"
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
	static int destroyMemBlock(block * b)
	{
		delete b;
		return 0;
	}
	DLL_EXPORT database::database(const char* confPrefix, config* conf, bufferPool* pool, META::metaDataBaseCollection* metaDataCollection) :m_status(BLOCK_MANAGER_STATUS::BM_UNINIT), m_confPrefix(confPrefix), m_blocks(destroyMemBlock), m_maxBlockID(0)
		, m_config(conf), m_last(nullptr), m_current(nullptr), m_pool(pool), m_metaDataCollection(metaDataCollection), m_threadPool(createThreadPool(32, this, &database::flushThread, std::string("databaseFlush_").append(confPrefix).c_str())), m_firstBlockId(0), m_lastBlockId(0), m_currentFlushThreadCount(0), m_recordId(0), m_tnxId(0)
	{
		initConfig();
	}
	DLL_EXPORT database::~database()
	{
		if(m_status == BLOCK_MANAGER_STATUS::BM_RUNNING)
			stop();
		m_blocks.clear();
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
		case appendingBlock::appendingBlockStaus::B_OK:
			return 0;
		case appendingBlock::appendingBlockStaus::B_FULL:
			if (!createNewBlock())
			{
				m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
				return -1;
			}
			return insert(r);
		case appendingBlock::appendingBlockStaus::B_FAULT:
			m_status = BLOCK_MANAGER_STATUS::BM_FAILED;
			LOG(ERROR) << "Fatal Error: insert record to current  block failed;" << "record id :" <<
				r->head->recordId << ",record offset:" << r->head->logOffset <<
				"record LogID:" << r->head->logOffset;
			return -1;
		case appendingBlock::appendingBlockStaus::B_ILLEGAL:
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
	int database::finishAppendingBlock(appendingBlock* b)
	{
		if(unlikely(b->m_flag&BLOCK_FLAG_FLUSHED))
			return 0;
		if (b->m_flag & BLOCK_FLAG_HAS_REDO)
		{
			if (b->finishRedo() != 0)
			{
				return -1;
			}
		}
		while (!m_unflushedBlocks.push(b, 1000))
		{
			if (m_status != BLOCK_MANAGER_STATUS::BM_RUNNING)
			{
				return -1;
			}
			m_threadPool.createNewThread();
		}
		return 0;
	}
	bool database::createNewBlock()
	{
		appendingBlock* tmp = m_current,*newBlock = new appendingBlock(m_current->m_blockID + 1,BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0),
			m_blockDefaultSize, m_redoFlushDataSize,
			m_redoFlushPeriod, m_recordId, this, m_metaDataCollection);

		newBlock->prev.store(m_current, std::memory_order_relaxed);
		newBlock->m_prevBlockID = m_current->m_blockID;
		m_blocks.set(newBlock->m_blockID, newBlock);

		m_current->next.store(newBlock, std::memory_order_release);
		m_lastBlockId.store(newBlock->m_blockID, std::memory_order_release);
		m_current = newBlock;
		barrier;

		if(0!=finishAppendingBlock(tmp))
		{
			m_current = tmp;
			delete newBlock;
			return false;
		}
		return true;
	}
	DLL_EXPORT void* database::allocMemForRecord(META::tableMeta* table, size_t size)
	{
		void* mem;
		appendingBlock::appendingBlockStaus status = m_current->allocMemForRecord(table, size, mem);
		if (likely(status == appendingBlock::appendingBlockStaus::B_OK))
			return mem;
		if (status == appendingBlock::appendingBlockStaus::B_FULL)
		{
			m_current->m_flag |= BLOCK_FLAG_FINISHED;
			if (!createNewBlock())
				return nullptr;
			return allocMemForRecord(table, size);
		}
		else
			return nullptr;
	}
	DLL_EXPORT bool database::checkpoint(uint64_t& timestamp, uint64_t& logOffset)
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
#define CREATE_KWEY_ITER(type,flag) do{\
	if((flag)&ITER_FLAG_DESC){return new kWaySortIterator<type,decreaseCompare<type> >(iterArray,idx);}\
	else {return new kWaySortIterator<type,increaseCompare<type> >(iterArray,idx);}\
	}while(0);

	DLL_EXPORT iterator* database::createIndexIterator(uint32_t flag,const META::tableMeta* table, META::KEY_TYPE type, int keyId)
	{
		int blockId;
		block* from,*to;
		while ((from = getBlock(blockId = m_firstBlockId.load(std::memory_order_relaxed))) == nullptr)
		{
			if (blockId == (int)m_firstBlockId.load(std::memory_order_relaxed))
				return nullptr;
		}
		barrier;
		to = m_current;
		to->use();
		std::list<blockIndexIterator*> iters;
		block* b = from;
		while (b->m_blockID <= to->m_blockID)
		{
			blockIndexIterator* iter = nullptr;
			if (b->m_flag & BLOCK_FLAG_SOLID)
			{
				if (checkSolidBlock(b) != 0)
				{
					for (std::list<blockIndexIterator*>::iterator i = iters.begin(); i != iters.end(); iter++)
						delete* i;
					return nullptr;//todo
				}
				iter = static_cast<solidBlock*>(b)->createIndexIterator(flag,table, type, keyId);
			}
			else
				iter = static_cast<appendingBlock*>(b)->createIndexIterator(flag,table, type, keyId);
			if (iter != nullptr)
				iters.push_back(iter);
			else
				b->unuse();
			b = b->next.load(std::memory_order_acquire);
			b->use();
		}
		if (iters.empty())
			return nullptr;
		blockIndexIterator** iterArray = new blockIndexIterator * [iters.size()];
		int idx = 0;
		for (std::list<blockIndexIterator*>::iterator i = iters.begin(); i != iters.end(); i++)
			iterArray[idx++] = *i;
		switch ((*iters.begin())->keyType())
		{
		case META::COLUMN_TYPE::T_UNION:
			CREATE_KWEY_ITER(META::unionKey, flag);
			break;
		case META::COLUMN_TYPE::T_INT8:
			CREATE_KWEY_ITER(int8_t, flag);
			break;
		case META::COLUMN_TYPE::T_UINT8:
			CREATE_KWEY_ITER(uint8_t, flag);
			break;
		case META::COLUMN_TYPE::T_INT16:
			CREATE_KWEY_ITER(int16_t, flag);
			break;
		case META::COLUMN_TYPE::T_UINT16:
			CREATE_KWEY_ITER(uint16_t, flag);
			break;
		case META::COLUMN_TYPE::T_INT32:
			CREATE_KWEY_ITER(int32_t, flag);
			break;
		case META::COLUMN_TYPE::T_UINT32:
			CREATE_KWEY_ITER(uint32_t, flag);
			break;
		case META::COLUMN_TYPE::T_INT64:
			CREATE_KWEY_ITER(int64_t, flag);
			break;
		case META::COLUMN_TYPE::T_TIMESTAMP:
		case META::COLUMN_TYPE::T_UINT64:
			CREATE_KWEY_ITER(uint64_t, flag);
			break;
		case META::COLUMN_TYPE::T_FLOAT:
			CREATE_KWEY_ITER(float, flag);
			break;
		case META::COLUMN_TYPE::T_DOUBLE:
			CREATE_KWEY_ITER(double, flag);
			break;
		case META::COLUMN_TYPE::T_BLOB:
		case META::COLUMN_TYPE::T_STRING:
			CREATE_KWEY_ITER(META::binaryType, flag);
			break;
		default:
			abort();
		}
		return nullptr;
	}
	DLL_EXPORT char* database::getRecord(const META::tableMeta* table, META::KEY_TYPE type, int keyId, const void* key)
	{
		char* record;
		for (block* b = m_current,*prev = nullptr;b!=nullptr;)
		{
			if (!b->use())
			{
				if (prev == nullptr)
					b = prev->prev.load(std::memory_order_relaxed);
				else
					b = m_current;
				continue;
			}
			if (b->m_flag & BLOCK_FLAG_APPENDING)
			{
				if (nullptr != (record = static_cast<appendingBlock*>(b)->getRecord(table, type, keyId, key)))
				{
					b->unuse();
					return record;
				}
			}
			else
			{
				if (checkSolidBlock(b) != 0)
				{
					b->unuse();
					return nullptr;//todo
				}
				if (nullptr != (record = static_cast<solidBlock*>(b)->getRecord(table, type, keyId, key)))
				{
					b->unuse();
					return record;
				}
			}
			b->unuse();
			prev = b;
			b = b->prev.load(std::memory_order_relaxed);
		}
		return nullptr;
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
		b = block::loadFromFile(blockId, this, m_metaDataCollection);
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
	int database::checkSolidBlock(block* b)
	{
		if (unlikely((b->m_flag & BLOCK_FLAG_SOLID) &&
			static_cast<solidBlock*>(b)->m_loading.load(std::memory_order_relaxed) <= static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADING_FIRST_PAGE)))
		{
			if (0 != static_cast<solidBlock*>(b)->load())
			{
				m_blockLock.unlock_shared();
				LOG(ERROR) << "lazy load solid block:" << b->m_blockID << " failed";
				return -1;
			}
		}
		return 0;
	}

	block* database::getBlock(uint32_t blockId)
	{
		if (m_firstBlockId.load(std::memory_order_relaxed) > blockId || m_lastBlockId.load(std::memory_order_relaxed) < blockId)
			return nullptr;
		m_blockLock.lock_shared();
		block* b = static_cast<block*>(m_blocks.get(blockId));
		if (likely(b != nullptr && b->use()))
		{
			if (checkSolidBlock(b) != 0)
			{
				b->unuse();
				m_blockLock.unlock_shared();
				return nullptr;
			}
			m_blockLock.unlock_shared();
			return b;
		}
		else
		{
			LOG(WARNING) << "block :"<<blockId<<" not exist";
			return nullptr;
		}
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
		m_flushLock.lock();
		sb->next.store(block->next.load(std::memory_order_relaxed),std::memory_order_relaxed);
		sb->prev.store(block->prev.load(std::memory_order_relaxed),std::memory_order_relaxed);
		if(block->prev.load(std::memory_order_relaxed)!=nullptr)
			block->prev.load(std::memory_order_relaxed)->next.store(sb,std::memory_order_relaxed);
		if(block->next.load(std::memory_order_relaxed)!=nullptr)
			block->next.load(std::memory_order_relaxed)->prev.store(sb,std::memory_order_relaxed);
		m_flushLock.unlock();
		LOG(INFO) << "trans appending block " << block->m_blockID << " to solidblock success";
		if (block->m_flag & BLOCK_FLAG_HAS_REDO)
		{
			block->finishRedo();
			char fileName[524];
			genBlockFileName(block->m_blockID, fileName);
			strcat(fileName, ".redo");
			remove(fileName);
		}
		assert(m_blocks.update(block->m_blockID, sb));
		block->m_flag |= BLOCK_FLAG_FLUSHED;
		block->unuse();
		return 0;
	}
	void database::flushThread()
	{
		appendingBlock* block;
		uint32_t idleRound = 0;
		while (likely(m_status == BLOCK_MANAGER_STATUS::BM_RUNNING||m_status == BLOCK_MANAGER_STATUS::BM_STOPPING))
		{
			if (!m_unflushedBlocks.pop(block, 1000))
			{
				if (m_status >= BLOCK_MANAGER_STATUS::BM_STOPPING||++idleRound > 1000)
				{
					if (m_threadPool.quitIfThreadMoreThan(1))
						return;
					else if(m_status >= BLOCK_MANAGER_STATUS::BM_STOPPING)//this is the last flush thread
					{
						if (!m_unflushedBlocks.pop(block, 100))
							return;
					}
				}
				else
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
			char fileName[524];
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
	int database::removeBlock(block* b)
	{
		if (b->prev.load(std::memory_order_relaxed) != nullptr)
			b->prev.load(std::memory_order_relaxed)->prev.store(b->next.load(std::memory_order_relaxed), std::memory_order_release);
		if (b->next.load(std::memory_order_relaxed) != nullptr)
			b->next.load(std::memory_order_relaxed)->prev.store(b->prev.load(std::memory_order_relaxed), std::memory_order_release);
		m_blocks.erase(b->m_blockID);
		int id = b->m_blockID;
		b->unuse();
		char fileName[524];
		genBlockFileName(id, fileName);
		remove(fileName);
		return 0;
	}

	void database::purgeThread(database* db)
	{
		db->purge();
	}

	int database::recoveryFromRedo(std::set<uint64_t>& redos, std::map<uint32_t, struct block*>& recoveried)
	{
		uint32_t activeCount = 0, lastActiveId = 0;
		for (std::set<uint64_t>::iterator iter = redos.begin(); iter != redos.end(); iter++)
		{
			char fileName[524];
			genBlockFileName(*iter, fileName);
			if (checkFileExist(fileName, 0) < 0)
			{
				LOG(WARNING) << "file of block " << *iter << " exist,remove redo file ";
				strcat(fileName, ".redo");
				remove(fileName);
				continue;
			}
			strcat(fileName, ".redo");
			if (checkFileExist(fileName, 0) < 0)
			{
				LOG(WARNING) << "redo file of block " << *iter << " is not exist ";
				return 1;
			}
			appendingBlock* block = new appendingBlock(*iter,BLOCK_FLAG_APPENDING,
				m_blockDefaultSize, m_redoFlushDataSize,
				m_redoFlushPeriod, 0, this, m_metaDataCollection);
			int ret = block->recoveryFromRedo();
			if (ret == -1)
			{
				LOG(ERROR) << "block :" << *iter << " recory from redo failed ";
				delete block;
				continue;
			}
			else if (ret == 1)
			{
				LOG(ERROR) << "block :" << *iter << " recory from redo success ,but this redo has not finish";
				recoveried.insert(std::pair<uint32_t, struct block*>(block->m_blockID, block));//todo ,avoid out of memory
				return 1;
			}
			solidBlock* sb = block->toSolidBlock();
			if (sb == nullptr)
			{
				LOG(ERROR) << "recory from redo failed fo trans block" << *iter << " to solid block failed";
				continue;
			}
			delete block;
			if (0 != sb->writeToFile())
			{
				LOG(ERROR) << "recory from redo failed fo flush block" << *iter << " to solid block file failed";
				delete sb;
				continue;
			}
			remove(fileName);
			recoveried.insert(std::pair<uint32_t, struct block*>(sb->m_blockID, sb));
			if (lastActiveId == 0)//avoid out of memory
				lastActiveId = sb->m_blockID;
			if (++activeCount > 4)
			{
				std::map<uint32_t, struct block*>::iterator lastActive = recoveried.find(lastActiveId);
				static_cast<solidBlock*>(lastActive->second)->gc();
				lastActive++;
				lastActiveId = lastActive->first;
				activeCount--;
			}
			LOG(ERROR) << "block :" << *iter << " recory from redo success ";
		}
		return 0;
	}
	int database::pickRedo(std::map<uint32_t, block*>& recoveried, block* from, block* to)
	{
		std::map<uint32_t, block*>::iterator iter = recoveried.upper_bound(from->m_blockID);
		if (iter == recoveried.end())
		{
			LOG(ERROR) << "pick redo from block:" << from->m_blockID << " failed for next block not exist in redo";
			return -1;
		}
		while (iter != recoveried.end() && (to == nullptr || iter->first < to->m_blockID))
		{
			if (iter->second->m_prevBlockID != from->m_blockID)
			{
				LOG(ERROR) << "pick redo from block:" << from->m_blockID << " failed for prevBlockID of next block is " << iter->second->m_prevBlockID;
				return -1;
			}
			from->next.store(iter->second, std::memory_order_relaxed);
			iter->second->prev.store(from, std::memory_order_relaxed);
			from = iter->second;
			m_blocks.set(from->m_blockID, from);
			recoveried.erase(iter++);
		}
		if (to != nullptr && to->m_prevBlockID != from->m_blockID)
		{
			LOG(ERROR) << "pick redo from block:" << from->m_blockID << " failed for prevBlockID of next block is " << to->m_prevBlockID;
			return -1;
		}
		return 0;
	}

	DLL_EXPORT int database::stop()
	{
		if(m_current!=nullptr)
		{
			m_current->m_flag |= BLOCK_FLAG_FINISHED;
			finishAppendingBlock(m_current);
		}
		m_status = BLOCK_MANAGER_STATUS::BM_STOPPING;
		m_threadPool.join();
		return 0;
	}
	int database::load()
	{
		int prefixSize = strlen(m_logPrefix);
		std::set<uint64_t> ids;
		std::set<uint64_t> redos;
		errno = 0;
		std::map<uint32_t, block*> recovried;
		block* prevBlock = nullptr;
		int64_t prev = -1;
		block* last = nullptr;
		char fileName[524];
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
				dirent* file;
				if (dir == nullptr)
				{
					if(0!=mkdir(m_logDir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))
					{
						LOG(ERROR) << "open data dir:" << m_logDir << " failed,errno:" << errno << "," << strerror(errno);
						return -1;
					}
					else
						goto CREATE_CURRENT;
				}
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

		if (!redos.empty())
		{
			recoveryFromRedo(redos, recovried);
			if (!recovried.empty() && (ids.empty() || *ids.begin() > recovried.begin()->first))
			{
				prevBlock = recovried.begin()->second;
				recovried.erase(prevBlock->m_blockID);
				m_last = prevBlock;
				prev = prevBlock->m_blockID;
				m_firstBlockId.store(prevBlock->m_blockID, std::memory_order_relaxed);
				m_lastBlockId.store(prevBlock->m_blockID, std::memory_order_relaxed);
				m_blocks.set(prevBlock->m_blockID, prevBlock);
			}
		}
		if (ids.size() > 0 || redos.size() > 0)
		{
			for (std::set<uint64_t>::iterator iter = ids.begin(); iter != ids.end(); iter++)
			{

				block* b = block::loadFromFile(*iter, this, m_metaDataCollection);
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
				if (prev == -1)
				{
					m_firstBlockId.store(*iter, std::memory_order_relaxed);
					m_lastBlockId.store(*iter, std::memory_order_relaxed);
					m_last = b;
				}
				else
				{
					if (b->m_prevBlockID != prev)
					{
						if (prev > b->m_prevBlockID)
						{
							block* tmp;
							for (tmp = prevBlock; tmp->m_blockID > b->m_prevBlockID; tmp = tmp->prev.load(std::memory_order_relaxed));
							if (tmp != nullptr && tmp->m_blockID != b->m_prevBlockID)
							{

								LOG(ERROR) << "database :" << m_logPrefix << " has lost some block file from block id:" << tmp->m_blockID << " to " << b->m_prevBlockID;
								if (pickRedo(recovried, prevBlock, b) != 0)
								{
									delete b;
									return -1;
								}
							}
							for (; prevBlock->m_blockID > b->m_prevBlockID; )//maybe prev shutdown of database is doing compection,new file has created ,but old file has not erase
							{
								tmp = prevBlock->prev.load(std::memory_order_relaxed);
								removeBlock(prevBlock);
								prevBlock = tmp;
							}
						}
						else
						{
							LOG(ERROR) << "block index is not increase strictly,block from:" << prev + 1 << " to " << *iter - 1 << " are not exist";
							if (pickRedo(recovried, prevBlock, b) != 0)
							{
								delete b;
								LOG(ERROR) << "stop load remind block,and move remind blocks to .bak file";
								for (iter++; iter != ids.end(); iter++)
								{
									genBlockFileName(*iter, fileName);
									rename(fileName, std::string(fileName).append(".bak").c_str());
								}
								break;
							}
						}
					}
					prevBlock->next.store(b, std::memory_order_relaxed);
				}
				b->prev.store(prevBlock, std::memory_order_relaxed);
				m_blocks.set(*iter, b);
				prev = *iter;
				prevBlock = b;
				m_lastBlockId.store(*iter, std::memory_order_relaxed);
			}
		}
		if (!recovried.empty() && *redos.rbegin() > m_lastBlockId.load(std::memory_order_relaxed))
		{
			pickRedo(recovried, prevBlock, nullptr);
			for (std::map<uint32_t, block*>::iterator iter = recovried.begin(); iter != recovried.end(); iter++)
			{
				genBlockFileName(iter->first, fileName);
				rename(std::string(fileName).append(".redo").c_str(), std::string(fileName).append(".redo.bak").c_str());
				delete iter->second;
			}
		}
		if (likely((last = m_blocks.end()) != nullptr))
		{
			m_tnxId = last->m_maxTxnId + 1;
			m_recordId = last->m_minRecordId + last->m_recordCount;
			if (m_current != nullptr)
				return 0;
		}
		else //empty block dir
		{
			m_tnxId = 1;
			m_recordId = 1;
			m_firstBlockId.store(0, std::memory_order_relaxed);
			m_lastBlockId.store(0, std::memory_order_relaxed);
		}
CREATE_CURRENT:
		m_current = new appendingBlock(m_lastBlockId.load(std::memory_order_relaxed) + 1,BLOCK_FLAG_APPENDING | (m_redo ? BLOCK_FLAG_HAS_REDO : 0) | (m_compress ? BLOCK_FLAG_COMPRESS : 0), m_blockDefaultSize,
			m_redoFlushDataSize, m_redoFlushPeriod, m_recordId, this, m_metaDataCollection);
		if (m_blocks.end() != nullptr)
		{
			m_blocks.end()->next.store(m_current, std::memory_order_relaxed);
			m_current->prev.store(m_blocks.end(), std::memory_order_relaxed);
			m_current->m_prevBlockID = m_blocks.end()->m_blockID;
		}
		else
			m_last = m_current;
		m_blocks.set(m_lastBlockId.load(std::memory_order_relaxed) + 1, m_current);
		m_lastBlockId.fetch_add(1, std::memory_order_relaxed);
		if (m_firstBlockId.load(std::memory_order_relaxed) == 0)
			m_firstBlockId.store(1, std::memory_order_relaxed);
		return 0;
	}
	int database::initConfig()
	{
		strncpy(m_logDir, m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_DIR), "data").c_str(), 255);

		strncpy(m_logPrefix, m_config->get(C_STORE_SCTION, REAL_CONF_STRING(C_LOG_PREFIX), "log").c_str(), 255);

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
