#include "database.h"
#include "appendingBlock.h"
#include "solidBlock.h"
namespace DATABASE
{

	/*
		find record by timestamp
		[IN]timestamp ,start time by micro second
		[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
		[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
	*/
	DLL_EXPORT bool databaseTimestampIterator::seek(const void * key)//timestamp not increase strictly,so we have to check all block
	{
		uint64_t timestamp = *(const uint64_t*)key;
		m_status = status::UNINIT;
		m_errInfo.clear();
		uint32_t blockId;
		while ((m_current = m_database->getBasciBlock(blockId = m_database->m_firstBlockId.load(std::memory_order_relaxed))) == nullptr)
		{
			if (blockId == m_database->m_firstBlockId.load(std::memory_order_relaxed))
				return false;
		}
		do {
			if (m_current->m_minTime > timestamp)
			{
				if ((blockId = m_current->m_blockID) == m_database->m_lastBlockId.load(std::memory_order_relaxed))
					return false;
				m_current->unuse();
				if (nullptr == (m_current = m_database->getBasciBlock(blockId + 1)))
					return false;
			}
			else
				break;
		} while (1);
		if (!(m_current->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID)))
		{
			block* tmp = m_database->getBlock(m_current->m_blockID);
			m_current->unuse();
			if (tmp == nullptr)
				return false;
			m_current = tmp;
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockIterator* iter = new appendingBlockTimestampIterator(static_cast<appendingBlock*>(m_current), m_filter, m_flag);
			if (!iter->seek(&timestamp))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockTimestampIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seek(&timestamp))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = status::OK;
		return true;
	}
	DLL_EXPORT bool databaseCheckpointIterator::seek(const void * key)
	{
		uint64_t logOffset = *(const uint64_t*)key;
		m_status = status::UNINIT;
		m_errInfo.clear();
		int64_t _s, s, _e, e,m;
RESEEK:
		_s = m_database->m_firstBlockId.load(std::memory_order_relaxed);
		_e = m_database->m_lastBlockId.load(std::memory_order_relaxed);
		s = _s;
		e = _e;
		while (s <= e)
		{
			m = (s + e) >> 1;
			m_current = m_database->getBasciBlock(m);
			if (m_current == nullptr)
				goto RESEEK;
			if (logOffset > m_current->m_maxLogOffset)
				s = m + 1;
			else if (logOffset < m_current->m_minLogOffset)
				e = m - 1;
			else
				goto FIND;
			m_current->unuse();
		}
		if (e < _s)
			m = _s;
		if (s > _e)
			m = _e;

		m_current = m_database->getBasciBlock(m);
		if (m_current == nullptr)
			goto RESEEK;
		if (m_current->m_maxLogOffset >= logOffset && m_current->m_minLogOffset <= logOffset)
			goto FIND;
		if (m_current->m_maxLogOffset < logOffset)
		{
			if ((m_flag & ITER_FLAG_DESC))
				goto FIND;
			else
			{
				m_current->unuse();
				if (m == _e)
					return false;
				m_current = m_database->getBasciBlock(++m);
				if (m_current == nullptr)
					goto RESEEK;
				if (m_current->m_minLogOffset >= logOffset)
					goto FIND;
				else
				{
					m_current->unuse();
					return false;
				}
			}
		}
		else if (m_current->m_minLogOffset > logOffset)
		{
			if (!(m_flag & ITER_FLAG_DESC))
				goto FIND;
			else
			{
				m_current->unuse();
				if (m == _s)
					return false;
				m_current = m_database->getBasciBlock(--m);
				if (m_current == nullptr)
					goto RESEEK;
				if (m_current->m_maxLogOffset <= logOffset)
					goto FIND;
				else
				{
					m_current->unuse();
					return false;
				}
			}
		}
	FIND:
		if ((m_current->m_flag & BLOCK_FLAG_SOLID)&&static_cast<solidBlock*>(m_current)->m_loading.load(std::memory_order_relaxed)<= BLOCK_LOAD_STATUS::BLOCK_LOADING_FIRST_PAGE)
		{
			if (0 != static_cast<solidBlock*>(m_current)->load())
			{
				m_current->unuse();
				return false;
			}
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockCheckpointIterator* iter = new appendingBlockCheckpointIterator(static_cast<appendingBlock*>(m_current), m_filter, m_flag);
			if (!iter->seek(&logOffset))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockCheckpointIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seek(&logOffset))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = status::OK;
		return true;
	}
	DLL_EXPORT bool databaseRecordIdIterator::seek(const void *key)
	{
		uint64_t recordId = *(const uint64_t*)key;
		m_status = status::UNINIT;
		m_errInfo.clear();
		int64_t s = m_database->m_firstBlockId.load(std::memory_order_relaxed), e = m_database->m_lastBlockId.load(std::memory_order_relaxed), m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			m_current = m_database->getBasciBlock(m);
			if (m_current == nullptr)
			{
				if (m < (int64_t)m_database->m_firstBlockId.load(std::memory_order_relaxed))
				{
					s = m_database->m_firstBlockId.load(std::memory_order_relaxed);
					e = m_database->m_lastBlockId.load(std::memory_order_relaxed);
					continue;
				}
				else
					return false;
			}
			if (recordId > m_current->lastRecordId())
				s = m + 1;
			else if (recordId < m_current->m_minRecordId)
				e = m - 1;
			else
				goto FIND;
		}
		m_current->unuse();
		m_current = nullptr;
		return false;
	FIND:
		if (!(m_current->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID)))
		{
			block* tmp = m_database->getBlock(m_current->m_blockID);
			m_current->unuse();
			if (tmp == nullptr)
				return false;
			m_current = tmp;
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockIterator* iter = new appendingBlockRecordIdIterator(static_cast<appendingBlock*>(m_current), m_filter, m_flag);
			if (!iter->seek(&recordId))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockRecordIdIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seek(&recordId))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = status::OK;
		return true;
	}
	DLL_EXPORT databaseIterator::databaseIterator(uint32_t flag, DB_ITER_TYPE type, filter* filter, database* db) :iterator(flag, filter), m_current(nullptr), m_blockIter(nullptr), m_database(db), m_iterType(type)
	{
		m_keyType = META::COLUMN_TYPE::T_UINT64;
	}
	DLL_EXPORT databaseIterator::~databaseIterator()
	{
		if (m_blockIter)
			delete m_blockIter;
	}
	DLL_EXPORT iterator::status databaseIterator::next()
	{
		status s = m_blockIter->next();
		if (unlikely(s == status::ENDED))
		{
			do {
				block* nextBlock;
				do
				{
					if (!(m_flag & ITER_FLAG_DESC))
					{
						if(nullptr==(nextBlock = m_current->next.load(std::memory_order_relaxed)))
							return status::BLOCKED;

					}
					else
					{
						if (nullptr == (nextBlock = m_current->prev.load(std::memory_order_relaxed)))
							return status::ENDED;
					}
					if (nextBlock->use())
						break;
				} while (1);

				if (nextBlock->m_flag & BLOCK_FLAG_APPENDING)
				{
					if (m_current->m_flag & BLOCK_FLAG_APPENDING)
					{
						static_cast<appendingBlockIterator*>(m_blockIter)->resetBlock(static_cast<appendingBlock*>(nextBlock));
					}
					else
					{
						appendingBlockIterator* iter;
						switch(m_iterType)
						{
						case DB_ITER_TYPE::TIMESTAMP_TYPE:
							iter = new appendingBlockRecordIdIterator(static_cast<appendingBlock*>(nextBlock), m_filter, m_flag);
							break;
						case DB_ITER_TYPE::CHECKPOINT_TYPE:
							iter = new appendingBlockRecordIdIterator(static_cast<appendingBlock*>(nextBlock), m_filter, m_flag);
							break;
						case DB_ITER_TYPE::RECORD_ID_TYPE:
							iter = new appendingBlockRecordIdIterator(static_cast<appendingBlock*>(nextBlock), m_filter, m_flag);
							break;
						default:
							abort();
						}
						iterator* tmp = m_blockIter;
						m_blockIter = iter;
						delete tmp;
					}
				}
				else
				{
					if (m_current->m_flag & BLOCK_FLAG_SOLID)
					{
						static_cast<solidBlockIterator*>(m_blockIter)->resetBlock(static_cast<solidBlock*>(nextBlock));
					}
					else
					{
						solidBlockIterator* iter ;
						switch (m_iterType)
						{
						case DB_ITER_TYPE::TIMESTAMP_TYPE:
							iter = new solidBlockRecordIdIterator(static_cast<solidBlock*>(nextBlock), m_flag, m_filter);
							break;
						case DB_ITER_TYPE::CHECKPOINT_TYPE:
							iter = new solidBlockRecordIdIterator(static_cast<solidBlock*>(nextBlock), m_flag, m_filter);
							break;
						case DB_ITER_TYPE::RECORD_ID_TYPE:
							iter = new solidBlockRecordIdIterator(static_cast<solidBlock*>(nextBlock), m_flag, m_filter);
							break;
						default:
							abort();
						}
						iterator* tmp = m_blockIter;
						m_blockIter = iter;
						delete tmp;
					}
				}
				m_current = nextBlock;
				if (m_blockIter->getStatus() == status::ENDED)
					continue;
				else
					return m_status = m_blockIter->getStatus();
			} while (1);
		}
		else
			return m_status = s;
	}
}
