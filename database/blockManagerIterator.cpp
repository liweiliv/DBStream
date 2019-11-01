#include "blockManager.h"
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
	DLL_EXPORT bool blockManagerIterator::seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter)//timestamp not increase strictly,so we have to check all block
	{
		m_status = UNINIT;
		m_errInfo.clear();
		uint32_t blockId;
		while ((m_current = m_manager->getBasciBlock(blockId = m_manager->m_firstBlockId.load(std::memory_order_relaxed))) == nullptr)
		{
			if (blockId == m_manager->m_firstBlockId.load(std::memory_order_relaxed))
				return false;
		}
		do {
			if (m_current->m_minTime > timestamp)
			{
				if ((blockId = m_current->m_blockID) == m_manager->m_lastBlockId.load(std::memory_order_relaxed))
					return false;
				m_current->unuse();
				if (nullptr == (m_current = m_manager->getBasciBlock(blockId + 1)))
					return false;
			}
			else
				break;
		} while (1);
		if (!(m_current->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID)))
		{
			block* tmp = m_manager->getBlock(m_current->m_blockID);
			m_current->unuse();
			if (tmp == nullptr)
				return false;
			m_current = tmp;
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockIterator* iter = new appendingBlockIterator(static_cast<appendingBlock*>(m_current), m_filter, 0, m_flag);
			if (!iter->seekByTimestamp(timestamp, interval, equalOrAfter))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seekByTimestamp(timestamp, interval, equalOrAfter))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = OK;
		return true;
	}
	DLL_EXPORT bool blockManagerIterator::seekByLogOffset(uint64_t logOffset, bool equalOrAfter)
	{
		m_status = UNINIT;
		m_errInfo.clear();
		int64_t s = m_manager->m_firstBlockId.load(std::memory_order_relaxed), e = m_manager->m_lastBlockId.load(std::memory_order_relaxed), m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			m_current = m_manager->getBasciBlock(m);
			if (m_current == nullptr)
			{
				if (m < (int64_t)m_manager->m_firstBlockId.load(std::memory_order_relaxed))
				{
					s = m_manager->m_firstBlockId.load(std::memory_order_relaxed);
					e = m_manager->m_lastBlockId.load(std::memory_order_relaxed);
					continue;
				}
				else
					return false;
			}
			if (logOffset > m_current->m_maxLogOffset)
				s = m + 1;
			else if (logOffset < m_current->m_minLogOffset)
				e = m - 1;
			else
				goto FIND;
		}
		if (equalOrAfter)
			return false;
		if (e < 0)
			m = 0;
	FIND:
		if (!(m_current->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID)))
		{
			block* tmp = m_manager->getBlock(m_current->m_blockID);
			m_current->unuse();
			if (tmp == nullptr)
				return false;
			m_current = tmp;
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockIterator* iter = new appendingBlockIterator(static_cast<appendingBlock*>(m_current), m_filter, 0, m_flag);
			if (!iter->seekByLogOffset(logOffset, equalOrAfter))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seekByLogOffset(logOffset, equalOrAfter))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = OK;
		return true;
	}
	DLL_EXPORT bool blockManagerIterator::seekByRecordId(uint64_t recordId)
	{
		m_status = UNINIT;
		m_errInfo.clear();
		int64_t s = m_manager->m_firstBlockId.load(std::memory_order_relaxed), e = m_manager->m_lastBlockId.load(std::memory_order_relaxed), m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			m_current = m_manager->getBasciBlock(m);
			if (m_current == nullptr)
			{
				if (m < (int64_t)m_manager->m_firstBlockId.load(std::memory_order_relaxed))
				{
					s = m_manager->m_firstBlockId.load(std::memory_order_relaxed);
					e = m_manager->m_lastBlockId.load(std::memory_order_relaxed);
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
	FIND:
		if (!(m_current->m_flag & (BLOCK_FLAG_APPENDING | BLOCK_FLAG_SOLID)))
		{
			block* tmp = m_manager->getBlock(m_current->m_blockID);
			m_current->unuse();
			if (tmp == nullptr)
				return false;
			m_current = tmp;
		}
		if (m_current->m_flag & BLOCK_FLAG_APPENDING)
		{
			appendingBlockIterator* iter = new appendingBlockIterator(static_cast<appendingBlock*>(m_current), m_filter, 0, m_flag);
			if (!iter->seekByRecordId(recordId))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		else
		{
			solidBlockIterator* iter = new solidBlockIterator(static_cast<solidBlock*>(m_current), m_flag, m_filter);
			if (!iter->seekByRecordId(recordId))
			{
				delete iter;//delete will call m_current->unuse();
				return false;
			}
			m_blockIter = iter;
		}
		m_status = OK;
		return true;
	}
	DLL_EXPORT blockManagerIterator::blockManagerIterator(uint32_t flag, filter* filter, blockManager* m_manager) :iterator(flag, filter), m_current(nullptr), m_blockIter(nullptr), m_manager(m_manager)
	{
	}
	DLL_EXPORT blockManagerIterator::~blockManagerIterator()
	{
		if (m_blockIter)
			delete m_blockIter;
	}
	DLL_EXPORT iterator::status blockManagerIterator::next()
	{
		status s = m_blockIter->next();
		if (unlikely(s == ENDED))
		{
			do {
				block* nextBlock = m_manager->getBlock(m_current->m_blockID + 1);
				if (nextBlock == nullptr)
					return BLOCKED;
				if (nextBlock->m_flag & BLOCK_FLAG_APPENDING)
				{
					if (m_current->m_flag & BLOCK_FLAG_APPENDING)
					{
						static_cast<appendingBlockIterator*>(m_blockIter)->resetBlock(static_cast<appendingBlock*>(nextBlock), 0);
					}
					else
					{
						appendingBlockIterator* iter = new appendingBlockIterator(static_cast<appendingBlock*>(nextBlock), m_filter, 0, m_flag);
						iterator* tmp = m_blockIter;
						m_blockIter = iter;
						delete tmp;
					}
				}
				else
				{
					if (m_current->m_flag & BLOCK_FLAG_SOLID)
					{
						static_cast<solidBlockIterator*>(m_blockIter)->resetBlock(static_cast<solidBlock*>(nextBlock), 0);
					}
					else
					{
						solidBlockIterator* iter = new solidBlockIterator(static_cast<solidBlock*>(nextBlock), m_flag, m_filter);
						iterator* tmp = m_blockIter;
						m_blockIter = iter;
						delete tmp;
					}
				}
				m_current = nextBlock;
				if (m_blockIter->m_status == ENDED)
					continue;
				else
					return m_status = m_blockIter->m_status;
			} while (1);
		}
		else
			return m_status = s;
	}

}
