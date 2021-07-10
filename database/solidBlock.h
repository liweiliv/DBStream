/*
 * solidBlock.h
 *
 *  Created on: 2019年1月7日
 *	  Author: liwei
 */
#ifndef SOLIDBLOCK_H_
#define SOLIDBLOCK_H_
#include <mutex>
#include <stdint.h>
#include "iterator.h"
#include "util/file.h"
#include "util/crcBySSE.h"
#include "glog/logging.h"
#include "lz4/lib/lz4.h"
#include "meta/metaData.h"
#include "meta/metaDataBaseCollection.h"
#include "thread/barrier.h"
#include "database.h"
#include "block.h"
#include "thread/threadLocal.h"
#include "solidIndex.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
#define SOLID_ITER_PAGE_CACHE_SIZE 16
#define SOLID_ITER_PAGE_CACHE_MASK 0xf

#define SOLID_INDEX_ITER_PAGE_CACHE_SIZE 4
#define SOLID_INDEX_ITER_PAGE_CACHE_MASK 0xf

namespace DATABASE
{
	class SolidBlockIterator;
	class AppendingBlock;
	template<class T>
	class SolidBlockIndexIterator;
	static threadLocal<char> compressBuffer;
	static threadLocal<uint32_t> compressBufferSize;
#pragma pack(1)
	class SolidBlock :public Block {
		fileHandle m_fd;
		TableDataInfo* m_tableInfo;
		RecordGeneralInfo* m_recordInfos;
		uint32_t* m_recordIdOrderyTable;
		TableFullData* m_tables;
		uint64_t* pageOffsets;
		char* pageInfo;
		Page** pages;
		Page* firstPage;
		std::mutex m_fileLock;
		friend class AppendingBlock;
		friend class SolidBlockIterator;
		friend class SolidBlockTimestampIterator;
		friend class SolidBlockCheckpointIterator;
		template <class T>
		friend class SolidBlockIndexIterator;
	public:
		SolidBlock(uint32_t blockId, Database* db, META::MetaDataBaseCollection* metaDataCollection, uint32_t flag) :Block(blockId, db, metaDataCollection, flag), m_fd(INVALID_HANDLE_VALUE), m_tableInfo(nullptr), m_recordInfos(nullptr),
			m_recordIdOrderyTable(nullptr), m_tables(nullptr), pageOffsets(nullptr), pageInfo(nullptr), pages(nullptr), firstPage(nullptr)
		{
		}
		SolidBlock(Block* b) :Block(b), m_fd(INVALID_HANDLE_VALUE), m_tableInfo(nullptr), m_recordInfos(nullptr),
			m_recordIdOrderyTable(nullptr), m_tables(nullptr), pageOffsets(nullptr), pageInfo(nullptr), pages(nullptr), firstPage(nullptr)
		{

		}
		~SolidBlock();
	public:
		int load(int id = 0);
	private:
		int loadFirstPage();
		int loadPage(Page* p, size_t size, size_t offset);
		inline char* getCompressbuffer(uint32_t size);
		int64_t writepage(Page* p, uint64_t offset, bool compress);
		const TableDataInfo* getTableInfo(uint64_t tableId);
		inline Page* getPage(int pageId)
		{
			Page* p = pages[pageId];
			p->use();
		RETRY:
			if (p->pageData == nullptr)
			{
				if (!p->_ref.own())
				{
					p->_ref.waitForShare();
					goto RETRY;
				}
				if (unlikely(0 != loadPage(p, pageOffsets[pageId + 1] - ALIGN(pageOffsets[pageId], 512), ALIGN(pageOffsets[pageId], 512))))
				{
					p->_ref.share();
					p->unuse();
					LOG(ERROR) << "load page " << pageId << " from block " << m_blockID << " at offset " << ALIGN(pageOffsets[pageId], 512) << " failed";
					return nullptr;
				}
				else
					p->_ref.share();
			}
			return p;
		}
		inline uint32_t getPageId(uint32_t recordId)
		{
			return pageId(m_recordInfos[recordId].offset);
		}
		Page* getIndex(const META::TableMeta* table, META::KEY_TYPE type, int keyId);
		char* getRecordByInnerId(uint32_t recordId);
		inline const char* getRecordFromPage(Page* p, uint32_t recordId)
		{
			return p->pageData + offsetInPage(m_recordInfos[recordId].offset);
		}
	public:
		inline const char* getRecord(uint64_t recordId)
		{
			return getRecordByInnerId(recordId - m_minRecordId - 1);
		}
		int writeToFile();
		int loadFromFile();
		int getTableIndexPageId(const TableDataInfo* tableInfo, const META::TableMeta* table, META::KEY_TYPE type, int keyId);
		void clear();
		BlockIndexIterator* createIndexIterator(uint32_t flag, const META::TableMeta* table, META::KEY_TYPE type, int keyId);
		char* getRecord(const META::TableMeta* table, META::KEY_TYPE type, int keyId, const void* key);
	};
#pragma pack()
	class SolidBlockIterator :public Iterator {
	protected:
		uint32_t m_recordId;
		uint32_t m_seekedId;
		uint64_t m_realRecordId;
		SolidBlock* m_block;
		Page* m_pageCache[16];
	public:
		SolidBlockIterator(SolidBlock* block, int flag, Filter* filter) :Iterator(flag, filter), m_recordId(0), m_seekedId(0), m_realRecordId(0), m_block(block)
		{
			memset(m_pageCache, 0, sizeof(m_pageCache));
			m_keyType = META::COLUMN_TYPE::T_UINT64;
			begin();
		}
		~SolidBlockIterator()
		{
			for (int i = 0; i < SOLID_ITER_PAGE_CACHE_SIZE; i++)
			{
				if (m_pageCache[i] != nullptr)
				{
					m_pageCache[i]->unuse();
					m_pageCache[i] = nullptr;
				}
			}
			if (m_block != nullptr)
			{
				m_block->unuse();
			}
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
				if (filterCurrentSeekRecord() != 0)
				{
					if (m_status == Status::INVALID)
						m_status = next();
				}
				else
					m_status = Status::OK;
			}
		}
		void resetBlock(SolidBlock* block);
		bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter);
		bool seekByRecordId(uint64_t recordId);
		inline bool valid()
		{
			return m_block != nullptr && m_recordId < m_block->m_recordCount;
		}
		int filterCurrentSeekRecord();
		Status next();
		inline const void* key()const
		{
			return &m_realRecordId;
		}
	private:
		inline const void* value(uint32_t rid)
		{
			uint32_t pageId = m_block->getPageId(rid), cacheId = pageId & SOLID_ITER_PAGE_CACHE_MASK;
			if (likely(m_pageCache[cacheId] != nullptr))
			{
				if (m_pageCache[cacheId]->pageId != pageId)
				{
					m_pageCache[cacheId]->unuse();
					if (nullptr == (m_pageCache[cacheId] = m_block->getPage(pageId)))
						return nullptr;
					m_pageCache[cacheId]->use();
				}
			}
			else
			{
				if (nullptr == (m_pageCache[cacheId] = m_block->getPage(pageId)))
					return nullptr;
				m_pageCache[cacheId]->use();
			}
			return m_block->getRecordFromPage(m_pageCache[cacheId], rid);
		}
	public:
		inline const void* value()
		{
			return value(m_recordId);
		}
		inline bool end()
		{
			return m_status == Status::ENDED;
		}
	};
	class SolidBlockTimestampIterator :public SolidBlockIterator
	{
	private:
		bool seekIncrease(const void* key);
		bool seekDecrease(const void* key);
	public:
		SolidBlockTimestampIterator(SolidBlock* block, int flag, Filter* filter) :SolidBlockIterator(block, flag, filter)
		{
			m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		bool seek(const void* key)
		{
			if (m_flag & ITER_FLAG_DESC)
				return seekDecrease(key);
			else
				return seekIncrease(key);
		}
		inline const void* key()const
		{
			return &m_block->m_recordInfos[m_recordId].timestamp;
		}
	};
	class SolidBlockCheckpointIterator :public SolidBlockIterator
	{
	public:
		SolidBlockCheckpointIterator(SolidBlock* block, int flag, Filter* filter) :SolidBlockIterator(block, flag, filter)
		{
			this->m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		bool seek(const void* key);
		inline const void* key()const
		{
			return &m_block->m_recordInfos[m_recordId].timestamp;
		}
	};
	class solidBlockRecordIdIterator :public SolidBlockIterator
	{
	public:
		solidBlockRecordIdIterator(SolidBlock* block, int flag, Filter* filter) :SolidBlockIterator(block, flag, filter)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
		}
		bool seek(const void* key);
		inline const void* key()const
		{
			return &m_realRecordId;
		}
	};
	template<class INDEX_TYPE>
	class SolidBlockIndexIterator :public BlockIndexIterator
	{
	private:
		SolidBlock* m_block;
		INDEX_TYPE m_index;
		IndexIterator<INDEX_TYPE>* indexIter;
		Page* m_currentPage;

	public:
		SolidBlockIndexIterator(uint32_t flag, SolidBlock* block, INDEX_TYPE& index) :BlockIndexIterator(flag, nullptr, block->m_blockID), m_block(block), m_index(index), m_currentPage(nullptr)
		{
			switch (index.getType())
			{
			case META::COLUMN_TYPE::T_UNION:
			{
				indexIter = new solidIndexIterator<META::unionKey, INDEX_TYPE>(flag, &m_index);
				break;
			}
			case META::COLUMN_TYPE::T_INT8:
				indexIter = new solidIndexIterator<int8_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				indexIter = new solidIndexIterator<uint8_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_INT16:
				indexIter = new solidIndexIterator<int16_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				indexIter = new solidIndexIterator<uint16_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_INT32:
				indexIter = new solidIndexIterator<int32_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				indexIter = new solidIndexIterator<uint32_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_INT64:
				indexIter = new solidIndexIterator<int64_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				indexIter = new solidIndexIterator<uint64_t, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				indexIter = new solidIndexIterator<float, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				indexIter = new solidIndexIterator<double, INDEX_TYPE>(flag, &m_index);
				break;
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_STRING:
				indexIter = new solidIndexIterator<META::BinaryType, INDEX_TYPE>(flag, &m_index);
				break;
			default:
				abort();
			}
		}
		virtual ~SolidBlockIndexIterator()
		{
			if (indexIter != nullptr)
				delete indexIter;
			if (m_block != nullptr)
			{
				if (m_currentPage != 0)
					m_currentPage->unuse();
				m_block->unuse();
			}
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
			if (increase())
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
			uint32_t pageId = m_block->getPageId(recordId);
			if (m_currentPage != nullptr)
			{
				if (m_currentPage->pageId != pageId)
				{
					m_currentPage->unuse();
					m_currentPage = m_block->getPage(pageId);

				}
			}
			else
				m_currentPage = m_block->getPage(pageId);
			if (m_currentPage == nullptr)
				return nullptr;
			return m_block->getRecordFromPage(m_currentPage, recordId);
		}
		virtual bool end()
		{
			return false;
		}
	};
}

#endif
