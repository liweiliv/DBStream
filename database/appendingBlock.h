/*
 *  * appendingBlock.h
 *   *
 *    *  Created on: 2019年1月7日
 *     *      Author: liwei
 *      */
#include <atomic>
#include <time.h>
#include "util/file.h"
#include "util/skiplist.h"
#include "meta/metaDataBaseCollection.h"
#include "util/unorderMapUtil.h"
#include "message/record.h"
#include "filter.h"
#include "iterator.h"
#include "block.h"
#include <string.h>
#include <map>
#include "glog/logging.h"
#include "meta/metaData.h"
#include "appendingIndex.h"
#include "util/arrayList.h"
#include "thread/barrier.h"
#include "lz4/lib/lz4.h"
#include "solidBlock.h"
#include "thread/cond.h"
namespace DATABASE
{
	class appendingBlockIterator;
	class appendingBlockLineIterator;
	static constexpr int maxRecordInBlock = 1024 * 128;
#pragma pack(1)
	class AppendingBlock : public Block
	{
	public:
		enum class appendingBlockStaus
		{
			B_OK, B_FULL, B_ILLEGAL, B_FAULT
		};
	private:
		struct tableData
		{
			uint64_t blockID;
			const META::TableMeta* meta;
			appendingIndex* primaryKey;
			appendingIndex** uniqueKeys;
			arrayList<uint32_t> recordIds;
			arrayList<Page*> pages;
			Page* current;
			uint32_t pageSize;
			tableData(uint64_t blockID, const META::TableMeta* meta,
				leveldb::Arena* arena, uint32_t _pageSize = DEFAULT_PAGE_SIZE);
			~tableData()
			{
				clean();
			}
			void clean();
			void init(uint64_t blockID, const META::TableMeta* meta,
				leveldb::Arena* arena, uint32_t _pageSize = 512 * 1024);
		};

		uint32_t* m_recordIDs;
		size_t m_size;
		size_t m_maxSize;
		std::atomic<uint32_t> m_committedRecordID;
		std::atomic<bool> m_ended;
		appendingBlockStaus m_status;
		leveldb::Arena m_arena;
		tableData m_defaultData;//for ddl,heartbeat
		std::map<uint64_t, tableData*> m_tableDatas;

		Page** m_pages;
		uint16_t m_maxPageCount;

		fileHandle m_redoFd;

		int32_t m_redoUnflushDataSize;
		int32_t m_redoFlushDataSize;
		int32_t m_redoFlushPeriod; //micro second
		uint64_t m_txnId;
		clock_t m_lastFLushTime;
		cond m_cond;
		friend class appendingBlockIterator;
		friend class appendingBlockLineIterator;
		friend class AppendingBlockIndexIterator;
	private:
		inline const tableData* getTableInfo(uint64_t tableId)
		{
			if (tableId == 0)
				return &m_defaultData;
			std::map<uint64_t, tableData*>::const_iterator iter = m_tableDatas.find(tableId);
			if (unlikely(iter == m_tableDatas.end()))
				return nullptr;
			return iter->second;
		}
		appendingIndex* getTableIndex(const tableData* tableInfo, const META::TableMeta* table, META::KEY_TYPE type, int keyId)
		{
			switch (type)
			{
			case META::KEY_TYPE::PRIMARY_KEY:
				return table->m_primaryKey != nullptr ? tableInfo->primaryKey : nullptr;
			case META::KEY_TYPE::UNIQUE_KEY:
				return table->m_uniqueKeysCount > 0 ? (table->m_uniqueKeysCount > keyId ? (tableInfo->uniqueKeys[keyId]) : nullptr) : nullptr;
			default:
				return nullptr;
			}
		}
		inline const char* getRecordByInnerId(uint32_t recordId)
		{
			uint32_t offset = m_recordIDs[recordId];
			return m_pages[pageId(offset)]->pageData + offsetInPage(offset);
		}
	public:
		AppendingBlock(uint32_t blockId, uint32_t flag,
			uint32_t bufSize, int32_t redoFlushDataSize,
			int32_t redoFlushPeriod, uint64_t startID, Database* db, META::MetaDataBaseCollection* metaDataCollection) :Block(blockId, db, metaDataCollection, flag),
			m_size(0), m_maxSize(bufSize), m_status(appendingBlockStaus::B_OK), m_defaultData(
				m_blockID, nullptr, &m_arena, 4096),
			m_redoFd(0), m_redoUnflushDataSize(0), m_redoFlushDataSize(
				redoFlushDataSize), m_redoFlushPeriod(redoFlushPeriod), m_txnId(
					0), m_lastFLushTime(0)
		{
			m_recordIDs = (uint32_t*)m_database->allocMem(
				sizeof(uint32_t) * maxRecordInBlock);
			m_maxPageCount = m_maxSize / (32 * 1204);
			m_pages = (Page**)m_database->allocMem(
				sizeof(Page*) * m_maxPageCount);
			memset(m_pages, 0, sizeof(Page*) * m_maxPageCount);
			m_minTime = m_maxTime = 0;
			m_minLogOffset = m_maxLogOffset = 0;
			m_minRecordId = startID;
			m_recordCount = 0;
			m_tableCount = 1;
			m_tableDatas.insert(std::pair<uint64_t, tableData*>(0, &m_defaultData));
		}
		~AppendingBlock()
		{
			if (m_recordCount > 0)
				assert(m_flag & BLOCK_FLAG_FINISHED);
			bufferPool::free(m_recordIDs);
			bufferPool::free(m_pages);
			if ((m_flag & BLOCK_FLAG_HAS_REDO) && !fileHandleValid(m_redoFd))
				closeFile(m_redoFd);
			for (std::map<uint64_t, tableData*>::iterator iter = m_tableDatas.begin(); iter != m_tableDatas.end(); iter++)
			{
				if (iter->second != &m_defaultData)
				{
					iter->second->clean();
					bufferPool::free(iter->second);
				}
			}
		}
		int openRedoFile(bool w);
		inline bool isLegalRecordId(uint64_t recordId)
		{
			return (recordId >= m_minRecordId && recordId < m_minRecordId + m_recordCount);
		}
		inline const char* getRecordByIdx(uint64_t recordIdx)
		{
			uint32_t offset = m_recordIDs[recordIdx];
			return m_pages[pageId(offset)]->pageData + offsetInPage(offset);
		}
		inline const char* getRecord(uint64_t recordId)
		{
			uint32_t offset = m_recordIDs[recordId - m_minRecordId];
			assert(m_pages[pageId(offset)]->pageId == pageId(offset));
			return m_pages[pageId(offset)]->pageData + offsetInPage(offset);
		}

		inline uint64_t findRecordIDByOffset(uint64_t offset)
		{
			uint32_t endID = m_recordCount - 1;
			if (m_recordCount == 0 || ((const RPC::RecordHead*)getRecord(m_minRecordId))->checkpoint.logOffset < offset
				|| ((const RPC::RecordHead*)getRecord(m_minRecordId + m_recordCount - 1))->checkpoint.logOffset>offset)
				return 0;
			int64_t s = 0, e = endID, m;
			uint64_t midOffset;
			while (s <= e)
			{
				m = (s + e) >> 1;
				midOffset = ((const RPC::RecordHead*)getRecord(m_minRecordId + m))->checkpoint.logOffset;
				if (midOffset > offset)
					e = m - 1;
				else if (midOffset < offset)
					s = m + 1;
				else
					return m_minRecordId + m;
			}
			if (s >= 0 && s<(int64_t)(endID - m_minRecordId) && ((const RPC::RecordHead*)getRecord(m_minRecordId + s))->checkpoint.logOffset > offset)
				return m_minRecordId + s;
			else
				return 0;
		}

		/*
	 * 	return value
	 * 		1: redo file not ended,and read success
	 * 			0: redo file ended,and  read success
	 * 				-1:read redo file failed
	 * 					*/
		int recoveryFromRedo();
		appendingBlockStaus writeRedo(const char* data);
		int finishRedo();
		int closeRedo();
		inline tableData* getCurrentVersion(uint64_t tableId)
		{
			std::map<uint64_t, tableData*>::iterator iter = m_tableDatas.lower_bound(META::TableMeta::genTableId((META::TableMeta::tableID(tableId) + 1), 0));
			if (iter != m_tableDatas.end())
			{
				iter--;
				if (iter == m_tableDatas.end() || META::TableMeta::tableID(iter->first) != META::TableMeta::tableID(tableId))
					return nullptr;
				else
					return iter->second;
			}
			else
			{
				std::map<uint64_t, tableData*>::reverse_iterator riter = m_tableDatas.rbegin();
				if (riter == m_tableDatas.rend() || META::TableMeta::tableID(riter->first) != META::TableMeta::tableID(tableId))
					return nullptr;
				return riter->second;
			}
		}
		inline tableData* getPrevVersion(uint64_t tableId)
		{
			std::map<uint64_t, tableData*>::iterator iter = m_tableDatas.lower_bound(tableId);
			if (iter != m_tableDatas.end())
			{
				iter--;
				if (iter == m_tableDatas.end() || META::TableMeta::tableID(iter->first) != META::TableMeta::tableID(tableId))
					return nullptr;
				else
					return iter->second;
			}
			else
				return nullptr;
		}
		inline tableData* getTableData(uint64_t tableId)
		{
			std::map<uint64_t, tableData*>::iterator iter = m_tableDatas.find(tableId);
			return iter == m_tableDatas.end() ? nullptr : iter->second;
		}
		inline tableData* getTableData(META::TableMeta* meta)
		{
			if (likely(meta != nullptr))
			{
				if (meta->userData == nullptr || m_blockID != static_cast<tableData*>(meta->userData)->blockID)
				{
					tableData* table = (tableData*)m_database->allocMem(sizeof(tableData));// new tableData(m_blockID, meta, &m_arena);
					table->init(m_blockID, meta, &m_arena);
					meta->userData = table;
					m_tableDatas.insert(std::pair<uint64_t, tableData*>(meta->m_id, table));
					m_tableCount++;
					return table;
				}
				else
					return static_cast<tableData*>(meta->userData);
			}
			else
				return &m_defaultData;
		}
		char* getRecord(const META::TableMeta* table, META::KEY_TYPE type, int keyId, const void* key)
		{
			tableData* tableInfo = getTableData(table->m_id);
			if (tableInfo == nullptr)
			{
				return nullptr;
			}
			appendingIndex* index = getTableIndex(tableInfo, table, type, keyId);
			if (index == nullptr)
			{
				return nullptr;
			}
			int32_t recordId = -1;
			switch (index->getType())
			{
			case META::COLUMN_TYPE::T_UNION:
				recordId = index->find<META::unionKey>(key);
				break;
			case META::COLUMN_TYPE::T_INT8:
				recordId = index->find<int8_t>(key);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				recordId = index->find<uint8_t>(key);
				break;
			case META::COLUMN_TYPE::T_INT16:
				recordId = index->find<int16_t>(key);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				recordId = index->find<uint16_t>(key);
				break;
			case META::COLUMN_TYPE::T_INT32:
				recordId = index->find<int32_t>(key);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				recordId = index->find<uint32_t>(key);
				break;
			case META::COLUMN_TYPE::T_INT64:
				recordId = index->find<int64_t>(key);
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				recordId = index->find<uint64_t>(key);
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				recordId = index->find<float>(key);
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				recordId = index->find<double>(key);
				break;
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_STRING:
				recordId = index->find<META::BinaryType>(key);
				break;
			default:
				abort();
			}
			if (recordId < 0)
			{
				return nullptr;
			}
			const char* r = getRecordByInnerId(recordId);
			char* newRecord = (char*)m_database->allocMem(((const RPC::MinRecordHead*)r)->size);
			memcpy(newRecord, r, ((const RPC::MinRecordHead*)r)->size);
			return newRecord;
		}
		appendingBlockStaus allocMemForRecord(tableData* t, size_t size, void*& mem);
		inline appendingBlockStaus allocMemForRecord(META::TableMeta* table, size_t size, void*& mem)
		{
			return allocMemForRecord(getTableData(table), size, mem);
		}
		inline appendingBlockStaus copyRecord(const RPC::Record*& record);
		appendingBlockStaus append(const RPC::Record* record);
		Page* createSolidIndexPage(appendingIndex* index, const META::UnionKeyMeta* ukMeta, const META::TableMeta* meta);
		SolidBlock* toSolidBlock();
		inline void commit()
		{
			if (likely(m_recordCount > 0))
			{
				m_maxTxnId = ((const RPC::RecordHead*)(getRecord(m_minRecordId + m_recordCount - 1)))->checkpoint.txnId;
				if (unlikely(m_minTxnId == 0))
					m_minTxnId = m_maxTxnId;
				m_committedRecordID.store(m_recordCount, std::memory_order_relaxed);
			}
		}
		inline void rollback()//todo
		{

		}
		BlockIndexIterator* createIndexIterator(uint32_t flag, const META::TableMeta* table, META::KEY_TYPE type, int keyId);
	};

	class appendingBlockIterator : public Iterator
	{
	protected:
		AppendingBlock* m_block;
		Filter* m_filter;
		uint32_t m_recordId;
		uint64_t m_realRecordId;
		uint32_t m_seekedId;
	public:
		appendingBlockIterator(AppendingBlock* block, Filter* filter, uint32_t flag = 0) :
			Iterator(flag, filter), m_block(block), m_filter(filter), m_recordId(
				0), m_realRecordId(0), m_seekedId(0)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
			begin();
		}
		virtual ~appendingBlockIterator()
		{
			if (m_block != nullptr)
				m_block->unuse();
		}
		void begin()
		{
			if (m_block != nullptr)
			{
				if (!(m_flag & ITER_FLAG_DESC))
				{
					m_recordId = 0;
					m_seekedId = 0;
					m_realRecordId = m_block->m_minRecordId;
				}
				else
				{
					m_recordId = m_block->m_recordCount;
					m_seekedId = m_recordId;
					m_realRecordId = m_block->m_minRecordId + m_recordId;
				}
				if (m_filter != nullptr && !m_filter->filterByRecord(m_block->getRecord(m_recordId)))
					m_status = next();
				else
					m_status = Status::OK;
			}
		}
		void resetBlock(AppendingBlock* block)
		{
			if (m_block != nullptr)
				m_block->unuse();
			m_block = block;
			begin();
		}
		inline const void* key() const
		{
			return &m_realRecordId;
		}
		inline const void* value()
		{
			return m_block->getRecordByInnerId(m_recordId);
		}
		inline Status next()
		{
			if (likely(!(m_flag & ITER_FLAG_DESC)))
				return realNext();
			else
				return realPrev();
		}
		inline Status realNext()
		{
			do
			{
				if (m_seekedId + 1 >= m_block->m_recordCount)
				{
					if (m_block->m_flag & BLOCK_FLAG_FINISHED)
						return m_status = Status::ENDED;
					if (m_flag & ITER_FLAG_SCHEDULE)
					{
						m_block->m_cond.wait();
						return  m_status = Status::BLOCKED;
					}
					if (m_flag & ITER_FLAG_WAIT)
					{
						std::this_thread::sleep_for(std::chrono::nanoseconds(10000));
						continue;
					}
					else
						return  m_status = Status::BLOCKED;
				}
				m_seekedId++;
			} while (m_filter != nullptr && !m_filter->filterByRecord(m_block->getRecord(m_seekedId)));
			m_recordId = m_seekedId;
			m_realRecordId = m_recordId + m_block->m_minRecordId;
			return  m_status = Status::OK;
		}
		inline Status realPrev()
		{
			do
			{
				if (m_seekedId == 0)
					return m_status = Status::ENDED;
				m_seekedId--;
			} while (!m_filter->filterByRecord(m_block->getRecord(m_seekedId)));
			m_recordId = m_seekedId;
			m_realRecordId = m_recordId + m_block->m_minRecordId;
			return  m_status = Status::OK;
		}
		inline bool end()
		{
			return m_status == Status::ENDED;
		}
		inline bool valid()
		{
			return m_block != nullptr && m_recordId < m_block->m_recordCount;
		}
	};
	class appendingBlockTimestampIterator :public appendingBlockIterator
	{
	private:
		bool seekIncrease(uint64_t timestamp)
		{
			m_status = Status::UNINIT;
			m_errInfo.clear();
			if (m_block == nullptr || m_block->m_maxTime < timestamp || m_block->m_minTime > timestamp)
				return false;
			m_recordId = -1;
			m_seekedId = -1;
			while (next() == Status::OK)
			{
				if (!valid())
					return false;
				const RPC::RecordHead* h = static_cast<const RPC::RecordHead*>(value());
				if (h->checkpoint.timestamp >= timestamp)
				{
					m_status = Status::OK;
					return true;
				}
			}
			if (m_status != Status::INVALID)
				m_recordId = 0xfffffffeu;
			return false;
		}
		bool seekDecrease(uint64_t timestamp)
		{
			m_status = Status::UNINIT;
			m_errInfo.clear();
			if (m_block == nullptr || m_block->m_maxTime < timestamp || m_block->m_minTime > timestamp)
				return false;
			m_recordId = m_block->m_recordCount;
			m_seekedId = m_recordId;
			while (next() == Status::OK)
			{
				if (!valid())
					return false;
				const RPC::RecordHead* h = static_cast<const RPC::RecordHead*>(value());
				if (h->checkpoint.timestamp <= timestamp)
				{
					m_status = Status::OK;
					return true;
				}
			}
			if (m_status != Status::INVALID)
				m_recordId = 0xfffffffeu;
			return false;
		}
	public:
		appendingBlockTimestampIterator(AppendingBlock* block, Filter* filter, uint32_t flag = 0) :appendingBlockIterator(block, filter, flag)
		{
			m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		bool seek(const void* key)//timestamp not increase strictly
		{
			uint64_t timestamp = *(const uint64_t*)(key);
			if (likely(!(m_flag & ITER_FLAG_DESC)))
				return seekIncrease(timestamp);
			else
				return seekDecrease(timestamp);
		}
		const void* key() const
		{
			return &((const RPC::RecordHead*)(m_block->getRecordByIdx(m_recordId)))->checkpoint.timestamp;
		}

	};
	class appendingBlockCheckpointIterator :public appendingBlockIterator
	{
	public:
		appendingBlockCheckpointIterator(AppendingBlock* block, Filter* filter, uint32_t flag = 0) :appendingBlockIterator(block, filter, flag)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		bool seek(const void* key)//timestamp not increase strictly
		{
			uint64_t logOffset = *(const uint64_t*)(key);
			m_status = Status::UNINIT;
			m_errInfo.clear();
			if (m_block == nullptr || m_block->m_maxLogOffset < logOffset || m_block->m_minLogOffset > logOffset)
				return false;
			int _e = m_block->m_recordCount - 1;
			int s = 0, e = _e, m;
			while (s <= e)
			{
				m = (s + e) >> 1;
				uint64_t offset = ((const RPC::RecordHead*)m_block->getRecord(m))->checkpoint.logOffset;
				if (logOffset > offset)
					s = m + 1;
				else if (logOffset < offset)
					e = m - 1;
				else
					goto FIND;
			}
			if (e < 0)
				m = 0;
			if (s > _e)
				m = _e;
			if (likely(!(m_flag & ITER_FLAG_DESC)))
			{
				if (((const RPC::RecordHead*)m_block->getRecord(m))->checkpoint.logOffset >= logOffset)
					goto FIND;
				if (m != _e && ((const RPC::RecordHead*)m_block->getRecord(++m))->checkpoint.logOffset >= logOffset)
					goto FIND;
			}
			else
			{
				if (((const RPC::RecordHead*)m_block->getRecord(m))->checkpoint.logOffset <= logOffset)
					goto FIND;
				if (m != 0 && ((const RPC::RecordHead*)m_block->getRecord(--m))->checkpoint.logOffset <= logOffset)
					goto FIND;
			}
			return false;
		FIND:
			if (likely(!(m_flag & ITER_FLAG_DESC)))
				m_recordId = m - 1;
			else
				m_recordId = m + 1;
			m_seekedId = m_recordId;
			if (next() != Status::OK)
			{
				m_recordId = 0xfffffffeu;
				return false;
			}
			m_realRecordId = m_recordId + m_block->m_minRecordId;
			m_status = Status::OK;
			return true;
		}
		inline const void* key() const
		{
			return &((const RPC::RecordHead*)(m_block->getRecordByIdx(m_recordId)))->checkpoint.logOffset;
		}
	};
	class appendingBlockRecordIdIterator :public appendingBlockIterator
	{
	public:
		appendingBlockRecordIdIterator(AppendingBlock* block, Filter* filter, uint32_t flag = 0) :appendingBlockIterator(block, filter, flag)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		bool seek(const void* key)//timestamp not increase strictly
		{
			uint64_t recordId = *(const uint64_t*)(key);
			m_status = Status::UNINIT;
			m_errInfo.clear();
			if (m_block == nullptr || m_block->m_minRecordId > recordId || m_block->m_minRecordId + m_block->m_recordCount < recordId)
				return false;
			m_recordId = recordId - m_block->m_minRecordId - 1;
			m_realRecordId = recordId;
			m_seekedId = m_recordId;
			if (next() != Status::OK)
			{
				m_recordId = 0xfffffffeu;
				return false;
			}
			m_status = Status::OK;
			return true;
		}
		inline const void* key() const
		{
			return &((const RPC::RecordHead*)(m_block->getRecordByIdx(m_recordId)))->recordId;
		}
	};
	class AppendingBlockIndexIterator :public BlockIndexIterator
	{
	private:
		appendingIndex* m_index;
		AppendingBlock* m_block;
		AppendingBlock::tableData* m_table;
		IndexIterator<appendingIndex>* indexIter;
	public:
		AppendingBlockIndexIterator(uint32_t flag, AppendingBlock* block, appendingIndex* index);
		virtual ~AppendingBlockIndexIterator()
		{
			if (indexIter != nullptr)
				delete indexIter;
			m_block->unuse();
		}
		bool seek(const void* key)
		{
			return indexIter->seek(key);
		}
		virtual bool valid()
		{
			return indexIter->valid();//todo
		};
		virtual Status next()
		{
			if (m_flag & ITER_FLAG_DESC)
				return indexIter->prevKey() ? Status::OK : Status::ENDED;
			else
				return indexIter->nextKey() ? Status::OK : Status::ENDED;
		}
		virtual const void* key() const
		{
			return indexIter->key();
		}
		virtual const void* value()
		{
			uint32_t recordId = indexIter->value();
			return m_block->getRecord(recordId);
		}
		virtual bool end()//todo
		{
			return false;
		}
	};
#pragma pack()
}


