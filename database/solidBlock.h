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
#include "util/barrier.h"
#include "database.h"
#include "block.h"
#include "thread/threadLocal.h"
#include "solidIndex.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
namespace DATABASE
{
	class solidBlockIterator;
	class appendingBlock;
	template<class T>
	class solidBlockIndexIterator;
	static threadLocal<char> compressBuffer;
	static threadLocal<uint32_t> compressBufferSize;
#pragma pack(1)
	class solidBlock :public block {
		fileHandle m_fd;
		tableDataInfo* m_tableInfo;
		recordGeneralInfo* m_recordInfos;
		uint32_t* m_recordIdOrderyTable;
		tableFullData* m_tables;
		uint64_t* pageOffsets;
		page** pages;
		page* firstPage;
		std::mutex m_fileLock;
		friend class appendingBlock;
		friend class solidBlockIterator;
		friend class solidBlockTimestampIterator;
		friend class solidBlockCheckpointIterator;
		template <class T>
		friend class solidBlockIndexIterator;
	public:
		solidBlock(uint32_t blockId,database* db, META::metaDataBaseCollection* metaDataCollection,uint32_t flag) :block(blockId,db, metaDataCollection,flag), m_fd(INVALID_HANDLE_VALUE), m_tableInfo(nullptr), m_recordInfos(nullptr),
			m_recordIdOrderyTable(nullptr), m_tables(nullptr), pageOffsets(nullptr), pages(nullptr), firstPage(nullptr)
		{
		}
		solidBlock(block * b) :block(b), m_fd(INVALID_HANDLE_VALUE), m_tableInfo(nullptr), m_recordInfos(nullptr),
			m_recordIdOrderyTable(nullptr), m_tables(nullptr), pageOffsets(nullptr), pages(nullptr), firstPage(nullptr)
		{

		}
		~solidBlock()
		{
			assert(m_flag & BLOCK_FLAG_FLUSHED);
			if(pages!=nullptr)
			{
				for (uint32_t i = 0; i < m_pageCount; i++)
				{
					if (pages[i] != nullptr)
						m_database->freePage(pages[i]);
				}
				basicBufferPool::free(pages);
			}
			m_database->freePage(firstPage);
			if (fileHandleValid(m_fd))
				closeFile(m_fd);
		}
	public:
		int load(int id = 0);
	private:
		int loadFirstPage();
		int loadPage(page* p, size_t size, size_t offset);
		inline char* getCompressbuffer(uint32_t size);
		int64_t writepage(page* p, uint64_t offset, bool compress);
		const tableDataInfo* getTableInfo(uint64_t tableId);
		inline page* getPage(int pageId)
		{
			page* p = pages[pageId];
			if (p == nullptr)
			{
				if (unlikely(0 != loadPage(pages[pageId], ALIGN(pageOffsets[pageId], 512), pageOffsets[pageId + 1] - ALIGN(pageOffsets[pageId], 512))))
					return nullptr;
			}
			return p;
		}
		inline uint32_t getPageId(uint32_t recordId)
		{
			return pageId(m_recordInfos[recordId].offset);
		}
		page* getIndex(const META::tableMeta* table, META::KEY_TYPE type, int keyId);
		inline const char* getRecordByInnerId(uint32_t recordId)
		{
			if (recordId >= m_recordCount)
				return nullptr;
			uint16_t pId = pageId(m_recordInfos[recordId].offset);
			page* p = getPage(pId);
			if (unlikely(p == nullptr))
				return nullptr;
			return p->pageData + offsetInPage(m_recordInfos[recordId].offset);
		}
public:
		inline const char* getRecord(uint64_t recordId)
		{
			return getRecordByInnerId(recordId - m_minRecordId - 1);
		}
		int writeToFile();
		int loadFromFile();
		int getTableIndexPageId(const tableDataInfo* tableInfo,const META::tableMeta* table, META::KEY_TYPE type, int keyId);
		int gc();
		blockIndexIterator* createIndexIterator(uint32_t flag,const META::tableMeta* table, META::KEY_TYPE type, int keyId);
		char* getRecord(const META::tableMeta* table, META::KEY_TYPE type, int keyId, const void* key);
	};
#pragma pack()
	class solidBlockIterator :public iterator {
	protected:
		uint32_t m_recordId;
		uint32_t m_seekedId;
		uint64_t m_realRecordId;
		solidBlock* m_block;
		uint32_t currentPageId;
	public:
		solidBlockIterator(solidBlock* block, int flag, filter* filter) :iterator(flag, filter), m_recordId(0), m_seekedId(0), m_realRecordId(0), m_block(block), currentPageId(0xfffffffful)
		{
			m_keyType = META::COLUMN_TYPE::T_UINT64;
			begin();
		}
		~solidBlockIterator()
		{
			if (m_block != nullptr)
			{
				if (currentPageId != 0xfffffffful)
					m_block->getPage(currentPageId)->unuse();
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
					if (m_status == status::INVALID)
						m_status = next();
				}
				else
					m_status = status::OK;
			}
		}
		void resetBlock(solidBlock* block);
		bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter);
		bool seekByRecordId(uint64_t recordId);
		inline bool valid()
		{
			return m_block != nullptr && m_recordId < m_block->m_recordCount;
		}
		int filterCurrentSeekRecord();
		status next();
		inline const void* key()const
		{
			return &m_realRecordId;
		}
		inline const void* value()
		{
			uint32_t pageId = m_block->getPageId(m_recordId);
			if (currentPageId != pageId)
			{
				if (currentPageId != 0xfffffffful)
					m_block->getPage(currentPageId)->unuse();
				m_block->getPage(pageId)->use();
				currentPageId = pageId;
			}
			return m_block->getRecordByInnerId(m_recordId);
		}
		inline bool end()
		{
			return m_status == status::ENDED;
		}
	};
	class solidBlockTimestampIterator :public solidBlockIterator
	{
	private:
		bool seekIncrease(const void* key);
		bool seekDecrease(const void* key);
	public:
		solidBlockTimestampIterator(solidBlock* block, int flag, filter* filter) :solidBlockIterator(block, flag, filter)
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
	class solidBlockCheckpointIterator :public solidBlockIterator
	{
	public:
		solidBlockCheckpointIterator(solidBlock* block, int flag, filter* filter) :solidBlockIterator(block, flag, filter)
		{
			this->m_keyType = META::COLUMN_TYPE::T_TIMESTAMP;
		}
		bool seek(const void* key);
		inline const void* key()const
		{
			return &m_block->m_recordInfos[m_recordId].timestamp;
		}
	};
	class solidBlockRecordIdIterator :public solidBlockIterator
	{
	public:
		solidBlockRecordIdIterator(solidBlock* block, int flag, filter* filter) :solidBlockIterator(block, flag, filter)
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
	class solidBlockIndexIterator :public blockIndexIterator
	{
	private:
		solidBlock* m_block;
		INDEX_TYPE m_index;
		indexIterator<INDEX_TYPE>* indexIter;
		uint32_t currentPageId;
	public:
		solidBlockIndexIterator(uint32_t flag,solidBlock* block, INDEX_TYPE &index):blockIndexIterator(flag, nullptr, block->m_blockID),m_block(block), m_index(index), currentPageId(0)
		{
			switch (index.getType())
			{
			case META::COLUMN_TYPE::T_UNION:
			{
				indexIter = new solidIndexIterator<META::unionKey, INDEX_TYPE>(flag,&m_index);
				break;
			}
			case META::COLUMN_TYPE::T_INT8:
				indexIter = new solidIndexIterator<int8_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				indexIter = new solidIndexIterator<uint8_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_INT16:
				indexIter = new solidIndexIterator<int16_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				indexIter = new solidIndexIterator<uint16_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_INT32:
				indexIter = new solidIndexIterator<int32_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				indexIter = new solidIndexIterator<uint32_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_INT64:
				indexIter = new solidIndexIterator<int64_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				indexIter = new solidIndexIterator<uint64_t, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				indexIter = new solidIndexIterator<float, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				indexIter = new solidIndexIterator<double, INDEX_TYPE>(flag,&m_index);
				break;
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_STRING:
				indexIter = new solidIndexIterator<META::binaryType, INDEX_TYPE>(flag,&m_index);
				break;
			default:
				abort();
			}
		}
		virtual ~solidBlockIndexIterator()
		{
			if (indexIter != nullptr)
				delete indexIter;
			if (m_block != nullptr)
			{
				if (currentPageId != 0)
					m_block->getPage(currentPageId)->unuse();
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
		virtual status next()
		{
			if(increase())
				return indexIter->prevKey() ? status::OK : status::ENDED;
			else
				return indexIter->nextKey() ? status::OK : status::ENDED;
		}
		virtual const void* key() const
		{
			return indexIter->key();
		}
		virtual const void* value() 
		{
			uint32_t recordId = indexIter->value();
			uint32_t pageId = m_block->getPageId(recordId);
			if (currentPageId != pageId)
			{
				if (currentPageId != 0)
					m_block->getPage(currentPageId)->unuse();
				m_block->getPage(pageId)->use();
				currentPageId = pageId;
			}
			return m_block->getRecordByInnerId(recordId);
		}
		virtual bool end()
		{
			return false;
		}
	};
}

#endif
