/*
 * block.h
 *
 *  Created on: 2019年1月18日
 *      Author: liwei
 */

#ifndef BLOCK_H_
#define BLOCK_H_
#include <stdint.h>
#include "util/ref.h"
#include "page.h"
#include "util/file.h"
#include "glog/logging.h"
#include "util/crcBySSE.h"
#include "iterator.h"
namespace META {
	class metaDataBaseCollection;
	class tableMeta;
}
namespace DATABASE {
#define BLOCK_FLAG_APPENDING    0x01
#define BLOCK_FLAG_SOLID        0x02
#define BLOCK_FLAG_FINISHED     0x04
#define BLOCK_FLAG_FLUSHED      0x08
#define BLOCK_FLAG_HAS_REDO     0x10
#define BLOCK_FLAG_COMPRESS     0x20
#define BLOCK_FLAG_MULTI_TABLE  0x40
	static constexpr auto DEFAULT_PAGE_SIZE = 512 * 1024;
	enum BLOCK_LOAD_STATUS {
		BLOCK_UNLOAD,
		BLOCK_LOADING_HEAD,
		BLOCK_LOADED_HEAD,
		BLOCK_LOADING_FIRST_PAGE,
		BLOCK_LOADED,
		BLOCK_LOAD_FAILED
	};
#pragma pack(1)
	struct recordID {
		uint32_t id;
		uint32_t pos;
	};
	struct dataPartHead {
		uint64_t dataPartSize;
		union {
			uint64_t rawSizeBeforeCompress;
			struct {
				uint32_t resever;
				uint32_t crc;
			};
		};
	};
	struct  tableDataInfo
	{
		uint64_t tableId;
		uint32_t recordCount;
		uint32_t recordIdsOffset;
		uint16_t pageCount;
		uint16_t firstPageId;
	};
#define setRecordPosition(p,pageId,offsetInPage) (p) = (pageId);(p) <<= 20;(p)+= (offsetInPage);
#define pageId(p) ((p)>>20)
#define offsetInPage(p) ((p)&0x000fffff)
	struct recordGeneralInfo
	{
		uint8_t recordType;
		uint32_t tableIndex;
		uint32_t offset;
		uint64_t timestamp;
		uint64_t logOffset;
	};
	struct tableFullData {
		uint16_t recordPageCount;
		uint16_t firstRecordPageId;
		uint16_t indexCount;
		uint16_t firstIndexPageId;
		META::tableMeta* meta;
	};
	class database;
	class block {
	public:
		std::atomic_uchar m_loading;
		::ref m_ref;
		database* m_database;
		std::atomic<block*> next;
		std::atomic<block*> prev;
		META::metaDataBaseCollection* m_metaDataCollection;
		uint32_t m_version;
		uint32_t m_flag;
		uint32_t m_prevBlockID;
		uint32_t m_blockID;
		uint64_t m_tableID;// if !(flag&BLOCK_FLAG_MULTI_TABLE)
		uint64_t m_minTime;
		uint64_t m_maxTime;
		uint64_t m_minLogOffset;
		uint64_t m_maxLogOffset;
		uint64_t m_minRecordId;
		uint32_t m_recordCount;
		uint32_t m_minTxnId;
		uint32_t m_maxTxnId;
		uint32_t m_tableCount;
		uint16_t m_pageCount;
		uint32_t m_solidBlockHeadPageRawSize;
		uint32_t m_solidBlockHeadPageSize;
		uint32_t m_crc;
		block(database* db, META::metaDataBaseCollection* metaDataCollection,uint32_t flag) :m_database(db),next(nullptr),prev(nullptr), m_metaDataCollection(metaDataCollection),m_version(1),m_flag(flag)
		{
			m_loading.store(BLOCK_UNLOAD, std::memory_order_relaxed);
			memset(&m_blockID, 0, sizeof(block) - offsetof(block, m_tableID));
		}
		block(block* b)
		{
			memcpy(&m_database, &b->m_database, sizeof(block) - offsetof(block, m_database));
			m_loading.store(b->m_loading.load(std::memory_order_relaxed), std::memory_order_relaxed);
		}
		virtual ~block() {}
		inline bool use()
		{
			return m_ref.use();
		}
		inline void unuse()
		{
			if (!m_ref.unuse())//no user now,and blockManager wants to delete it
				delete this;
		}
		//call it after loading 
		inline uint64_t lastRecordId()
		{
			return m_minRecordId + m_recordCount - 1;
		}
		int loadBlockInfo(fileHandle h,uint32_t id);
		static block* loadFromFile(uint32_t id, database* db, META::metaDataBaseCollection* metaDataCollection=nullptr);
	};
#pragma pack()
	class blockIndexIterator :public iterator {
	public:
		blockIndexIterator(uint32_t flag, filter* filter,uint32_t blockId) :iterator(flag, filter),m_blockId(blockId) {}
		virtual ~blockIndexIterator() {}
		uint32_t getBlockId()const { return m_blockId; }
	protected:
		uint32_t m_blockId;
	};
}



#endif /* BLOCK_H_ */
