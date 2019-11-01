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
		//friend class solidBlockIndexIterator<fixedSolidIndex>;
		//friend class solidBlockIndexIterator<varSolidIndex>;
	public:
		solidBlock(database* db, META::metaDataBaseCollection* metaDataCollection,uint32_t flag) :block(db, metaDataCollection,flag), m_fd(INVALID_HANDLE_VALUE), m_tableInfo(nullptr), m_recordInfos(nullptr),
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
			for (uint32_t i = 0; i < m_pageCount; i++)
			{
				if (pages[i] != nullptr)
					m_database->freePage(pages[i]);
			}
			m_database->freePage(firstPage);
			if (fileHandleValid(m_fd))
				closeFile(m_fd);
		}
	public:
		int load(int id);
	private:
		int loadFirstPage();
		int loadPage(page* p, size_t size, size_t offset);
		inline char* getCompressbuffer(uint32_t size);
		int64_t writepage(page* p, uint64_t offset, bool compress);
public:
		inline const char* getRecord(uint32_t recordId)
		{
			if (recordId >= m_recordCount)
				return nullptr;
			uint16_t pId = pageId(m_recordInfos[recordId].offset);
			page* p = pages[pId];
			if (unlikely(0 != loadPage(p, ALIGN(pageOffsets[pId], 512), pageOffsets[pId + 1] - ALIGN(pageOffsets[pId], 512))))
				return nullptr;
			return p->pageData + offsetInPage(m_recordInfos[recordId].offset);
		}
		int writeToFile();
		int loadFromFile();
	};
#pragma pack()
	class solidBlockIterator :public iterator {
		uint32_t m_recordId;
		solidBlock* m_block;
	public:
		solidBlockIterator(solidBlock* block, int flag, filter* filter) :iterator(flag, filter), m_recordId(0xfffffffeu), m_block(block)
		{
			seekByRecordId(m_block->m_minRecordId);
		}
		~solidBlockIterator()
		{
			if (m_block != nullptr)
			{
				m_block->unuse();
			}
		}
		void resetBlock(solidBlock* block, uint32_t id = 0);
		/*
		find record by timestamp
		[IN]timestamp ,start time by micro second
		[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
		[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
		*/
		bool seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter);//timestamp not increase strictly
		bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter);
		bool seekByRecordId(uint64_t recordId);
		inline bool valid()
		{
			return m_block != nullptr && m_recordId < m_block->m_recordCount;
		}
		status next();
		inline const void* value() const
		{
			return m_block->getRecord(m_recordId);
		}
		inline bool end()
		{
			return m_status == status::ENDED;
		}
	};
	template<typename INDEX_TYPE>
	class solidBlockIndexIterator :public iterator
	{
	private:
		solidBlock* m_block;
		INDEX_TYPE* m_index;
		indexIterator<INDEX_TYPE>* indexIter;
	public:
		solidBlockIndexIterator(solidBlock* block, INDEX_TYPE* index):m_block(block), m_index(index)
		{
			switch (index->getType())
			{
			case META::COLUMN_TYPE::T_UNION:
				indexIter = new solidIndexIterator<unionKey, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_INT8:
				indexIter = new solidIndexIterator<int8_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_UINT8:
				indexIter = new solidIndexIterator<uint8_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_INT16:
				indexIter = new solidIndexIterator<int16_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_UINT16:
				indexIter = new solidIndexIterator<uint16_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_INT32:
				indexIter = new solidIndexIterator<int32_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_UINT32:
				indexIter = new solidIndexIterator<uint32_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_INT64:
				indexIter = new solidIndexIterator<int64_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				indexIter = new solidIndexIterator<uint64_t, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_FLOAT:
				indexIter = new solidIndexIterator<float, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_DOUBLE:
				indexIter = new solidIndexIterator<double, INDEX_TYPE>(index);
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_STRING:
				indexIter = new solidIndexIterator<binaryType, INDEX_TYPE>(index);
			default:
				abort();
			}
		}
		virtual ~solidBlockIndexIterator()
		{
			if (indexIter != nullptr)
				delete indexIter;
		}
		bool seek(const void* key, bool equalOrGreater)
		{
			return indexIter->seek(key, equalOrGreater);
		}
		virtual bool valid()
		{
			return indexIter->valid();
		};
		virtual status next()
		{
			return indexIter->next() ? status::OK : status::ENDED;
		}
		virtual const void* key() const
		{
			return indexIter->key();
		}
		virtual const void* value() const
		{
			uint32_t recordId = indexIter->value();
			return m_block->getRecord(recordId);
		}
		virtual bool end()
		{
			return false;
		}
	};
}

#endif
