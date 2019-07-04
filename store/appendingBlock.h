/*
 * appendingBlock.h
 *
 *  Created on: 2019年1月7日
 *      Author: liwei
 */
#include <atomic>
#include <time.h>
#include "../util/file.h"
#include "../util/skiplist.h"
#include "../meta/metaDataCollection.h"
#include "../util/unorderMapUtil.h"
#include "../message/record.h"
#include "filter.h"
#include "iterator.h"
#include "schedule.h"
#include "block.h"
#include <string.h>
#include <map>
#include "../glog/logging.h"
#include "../meta/metaData.h"
#include "appendingIndex.h"
#include "../util/arrayList.h"
#include "../util/barrier.h"
#include "../lz4/lib/lz4.h"
#include "solidBlock.h"
#include "cond.h"
namespace STORE
{
class appendingBlockIterator;
class appendingBlockLineIterator;
static constexpr int maxRecordInBlock = 1024 * 128;
#pragma pack(1)
class appendingBlock: public block
{
public:
	enum appendingBlockStaus
	{
		B_OK, B_FULL, B_ILLEGAL, B_FAULT
	};
private:
	struct tableData
	{
		uint64_t blockID;
		const META::tableMeta * meta;
		appendingIndex * primaryKey;
		appendingIndex ** uniqueKeys;
		arrayList<uint32_t> recordIds;
		arrayList<page*> pages;
		page * current;
		uint32_t pageSize;
		tableData(uint64_t blockID, const META::tableMeta * meta,
				leveldb::Arena *arena, uint32_t _pageSize = 512 * 1024) :
				blockID(blockID), meta(meta), primaryKey(nullptr), uniqueKeys(
						nullptr), recordIds(arena), pages(arena), current(
						nullptr), pageSize(_pageSize)
		{
			if (meta != nullptr)
			{
				if (meta->m_primaryKey.count > 0)
					primaryKey = new appendingIndex(
							meta->m_primaryKey.keyIndexs,
							meta->m_primaryKey.count, meta, arena);
				if (meta->m_uniqueKeysCount > 0)
				{
					uniqueKeys = new appendingIndex*[meta->m_uniqueKeysCount];
					for (uint16_t i = 0; i < meta->m_uniqueKeysCount; i++)
						uniqueKeys[i] = new appendingIndex(
								meta->m_uniqueKeys[i].keyIndexs,
								meta->m_uniqueKeys[i].count, meta, arena);
				}
			}
		}
		~tableData()
		{
			clean();
		}
		void clean()
		{
			if (primaryKey != nullptr)
			{
				delete primaryKey;
				primaryKey = nullptr;
			}
			if (uniqueKeys != nullptr)
			{
				for (uint16_t idx = 0; idx < meta->m_uniqueKeysCount; idx++)
				{
					if (uniqueKeys[idx] != nullptr)
						delete uniqueKeys[idx];
				}
				delete uniqueKeys;
				uniqueKeys = nullptr;
			}
		}
		void init(uint64_t blockID, const META::tableMeta * meta,
				leveldb::Arena *arena, uint32_t _pageSize = 512 * 1024)
		{
			this->blockID = blockID;
			this->meta = meta;
			if (meta != nullptr)
			{
				if (meta->m_primaryKey.count > 0)
					primaryKey = new appendingIndex(
							meta->m_primaryKey.keyIndexs,
							meta->m_primaryKey.count, meta, arena);
				else
					primaryKey = nullptr;
				if (meta->m_uniqueKeysCount > 0)
				{
					uniqueKeys = new appendingIndex*[meta->m_uniqueKeysCount];
					for (uint16_t i = 0; i < meta->m_uniqueKeysCount; i++)
						uniqueKeys[i] = new appendingIndex(
								meta->m_uniqueKeys[i].keyIndexs,
								meta->m_uniqueKeys[i].count, meta, arena);
				}
				else
					uniqueKeys = nullptr;
			}
			else
			{
				primaryKey = nullptr;
				uniqueKeys = nullptr;
			}
			recordIds.init(arena);
			pages.init(arena);
			current = nullptr;
			pageSize = _pageSize; //todo
		}
	};

	uint32_t * m_recordIDs;
	size_t m_size;
	size_t m_maxSize;
	std::atomic<uint32_t> m_committedRecordID;
	std::atomic<bool> m_ended;
	appendingBlockStaus m_status;
	leveldb::Arena m_arena;
	tableData m_defaultData;//for ddl,heartbeat
	std::map<uint64_t, tableData*> m_tableDatas;

	page ** m_pages;
	uint16_t m_pageNum;
	uint16_t m_maxPageNum;

	fileHandle m_redoFd;

	int32_t m_redoUnflushDataSize;
	int32_t m_redoFlushDataSize;
	int32_t m_redoFlushPeriod; //micro second
	uint64_t m_txnId;
	clock_t m_lastFLushTime;
	cond m_cond;
	friend class appendingBlockIterator;
	friend class appendingBlockLineIterator;
public:
	appendingBlock(uint32_t flag,
			uint32_t bufSize, int32_t redoFlushDataSize,
			int32_t redoFlushPeriod, uint64_t startID, blockManager * manager,META::metaDataCollection *metaDataCollection) :block(manager, metaDataCollection),
			m_size(0), m_maxSize(bufSize), m_status(B_OK), m_defaultData(
					m_blockID, nullptr, &m_arena, 4096),m_pageNum(
					0), m_redoFd(0), m_redoUnflushDataSize(0), m_redoFlushDataSize(
					redoFlushDataSize), m_redoFlushPeriod(redoFlushPeriod), m_txnId(
					0), m_lastFLushTime(0)
	{
		m_recordIDs = (uint32_t*) m_blockManager->allocMem(
				sizeof(uint32_t) * maxRecordInBlock);
		m_maxPageNum = m_maxSize / (32 * 1204);
		m_pages = (page**) m_blockManager->allocMem(
				sizeof(page*) * m_maxPageNum);
		m_minTime = m_maxTime = 0;
		m_minLogOffset = m_maxLogOffset = 0;
		m_minRecordId = startID;
		m_recordCount = 0;
		m_tableCount = 0;
		m_tableDatas.insert(std::pair<uint64_t,tableData*>(0,&m_defaultData));
	}
	~appendingBlock()
	{
		assert(m_flag&BLOCK_FLAG_FINISHED);
		bufferPool::free(m_recordIDs);
		bufferPool::free(m_pages);
		if (m_flag&BLOCK_FLAG_HAS_REDO && !fileHandleValid(m_redoFd))
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
	int openRedoFile(bool w)
	{
		char fileName[512];
		m_blockManager->genBlockFileName(m_blockID,fileName);
		strcat(fileName,".redo");
		m_redoFd = openFile(fileName ,true, true,
			true);
		if (!fileHandleValid(m_redoFd))
		{
			LOG(ERROR)<<"open redo file :"<<fileName<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
			return -1;
		}
		if (w)
		{
			int64_t size = seekFile(m_redoFd, 0, SEEK_END);
			if (size < 0)
			{
				LOG(ERROR) << "open redo file :" << fileName<< "failed for errno:" << errno << ",error info:" << strerror(errno);
				closeFile(m_redoFd);
				m_redoFd = INVALID_HANDLE_VALUE;
				return -1;
			}
			else if (size == 0)
			{
				if (sizeof(m_minLogOffset) != writeFile(m_redoFd, (char*)& m_minLogOffset, sizeof(m_minLogOffset)))
				{
					LOG(ERROR) << "open redo file :" << fileName << "failed for errno:" << errno << ",error info:" << strerror(errno);
					closeFile(m_redoFd);
					m_redoFd = INVALID_HANDLE_VALUE;
					return -1;
				}
			}
		}
		return 0;
	}
	inline bool isLegalRecordId(uint64_t recordId)
	{
		return (recordId >= m_minRecordId && recordId < m_minRecordId + m_recordCount);
	}
	inline const char * getRecord(uint64_t recordId)
	{
		uint32_t offset = m_recordIDs[recordId - m_minRecordId];
		return m_pages[pageId(offset)]->pageData + offsetInPage(offset);
	}
	inline uint64_t findRecordIDByOffset(uint64_t offset)
	{
		uint32_t endID = m_recordCount-1;
		if(m_recordCount ==0|| ((const DATABASE_INCREASE::recordHead*)getRecord(m_minRecordId))->logOffset<offset|| ((const DATABASE_INCREASE::recordHead*)getRecord(m_minRecordId+m_recordCount-1))->logOffset>offset)
		return 0;
		int64_t s = 0, e = endID ,m;
		uint64_t midOffset;
		while (s <= e)
		{
			m = (s + e) >> 1;
			midOffset = ((const DATABASE_INCREASE::recordHead*)getRecord(m_minRecordId + m))->logOffset;
			if (midOffset > offset)
			e = m - 1;
			else if (midOffset < offset)
			s = m + 1;
			else
			return m_minRecordId + m;
		}
		if (s >= 0 && s<(int64_t)(endID - m_minRecordId) && ((const DATABASE_INCREASE::recordHead*)getRecord(m_minRecordId + s))->logOffset > offset)
		return m_minRecordId + s;
		else
		return 0;
	}

	/*
	return value
	1: redo file not ended,and read success
	0: redo file ended,and  read success
	-1:read redo file failed
	*/
	int recoveryFromRedo()
	{
		int ret = 1;
		if (m_redoFd >0)
			closeFile(m_redoFd);
		if(0>openRedoFile(false))
		{
			LOG(ERROR)<<"open redo file of block"<<m_blockID<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
			return -1;
		}
		int size = seekFile(m_redoFd,0,SEEK_END); //get fileSize
		if(size<0)
		{
			LOG(ERROR)<<"get size of  redo file of block :"<<m_blockID<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
			return -1;
		}
		if(size <(int)sizeof(uint64_t)) //empty file
			return 0;
		char * buf = (char*)malloc(size);
		if(buf == NULL)
		{
			LOG(ERROR)<<"alloc "<<size<<" byte memory failed";
			return -1;
		}
		if(0!= seekFile(m_redoFd,0,SEEK_SET)) //seek to begin of file
		{
			free(buf);
			LOG(ERROR)<<"leeek to begin of redo file :"<<m_blockID<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
			return -1;
		}
		if(size!=readFile(m_redoFd,buf,size)) //read all data one time
		{
			free(buf);
			LOG(ERROR)<<"read redo file of block :"<<m_blockID<<".redo failed for errno:"<<errno<<",error info:"<<strerror(errno);
			return -1;
		}
		m_flag &= (~BLOCK_FLAG_HAS_REDO); //unset BLOCK_FLAG_HAS_REDO,so call [append] will not write redo file
		m_minRecordId = *(uint64_t*)buf;
		DATABASE_INCREASE::recordHead* head = (DATABASE_INCREASE::recordHead*) (buf+sizeof(uint64_t));
		while((char*)head<=buf+size)
		{
			/*redo end normally*/
			if (unlikely(((char*)head - buf) + sizeof(DATABASE_INCREASE::recordHead::size) == (uint32_t)size) && head->size == ENDOF_REDO_NUM)
			{
				ret = 0;
				break;
			}

			/*redo truncated*/
			if(((char*)head)+sizeof(DATABASE_INCREASE::recordHead)>buf+size||((char*)head)+head->size>buf+size) //unfinished write ,truncate
			{
				LOG(WARNING)<<"get an incomplete redo data in redo file of block:"<<m_blockID<<",offset is "<<((char*)head)-buf<<",truncate it";
				
				if (((char*)head) - buf != seekFile(m_redoFd, ((char*)head) - buf, SEEK_SET)|| truncateFile(m_redoFd, ((char*)head) - buf)!=0)
				{
					LOG(WARNING) << "truncate redo file of block:" << m_blockID << "failed for:errno" << errno << "," << strerror(errno);
					free(buf);
					return -1;
				}
				break;
			}
			if(append(DATABASE_INCREASE::createRecord((const char*)head, m_metaDataCollection))!=B_OK)
			{
				LOG(ERROR)<<"recoveryFromRedo from redo file of block:"<<m_blockID<<" failed for append data failed";
				free(buf);
				m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
				return -1;
			}
			head = (DATABASE_INCREASE::recordHead*)(((char*)head)+ head->size);
		}
		m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
		LOG(INFO)<<"recoveryFromRedo from redo file :"<<m_blockID<<" success";
		free(buf);
		return ret;
	}
	appendingBlockStaus writeRedo(const char * data)
	{
		if (!fileHandleValid(m_redoFd)&&0!=openRedoFile(true))
			return B_FAULT;
		DATABASE_INCREASE::recordHead* head = (DATABASE_INCREASE::recordHead*) data;
		int64_t writeSize;
		if(head->size!=(uint64_t)(writeSize=writeFile(m_redoFd,data,head->size)))
		{
			if(errno==EBADF) //maybe out time or other cause,reopen it
			{
				LOG(ERROR)<<"write redo file of block:"<<m_blockID<<" failed for "<<strerror(errno)<<" reopen it";
					return B_FAULT;
			}
			else
			{
				LOG(ERROR)<<"write redo file of block :"<<m_blockID<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
				return B_FAULT;
			}
		}
		clock_t now;
		if(m_redoFlushDataSize==0|| //m_redoFlushDataSize == 0 means flush immediately
				m_redoFlushPeriod==0||//m_redoFlushPeriod == 0 also means flush immediately
				(m_redoFlushDataSize>0&&(m_redoUnflushDataSize+=head->size)>=m_redoFlushDataSize)||//check if unflushed data big enough
				(m_redoFlushPeriod>0&&//check if time from last flush is long enough
						(now = clock(),
								now<m_lastFLushTime||now - m_lastFLushTime > m_redoFlushPeriod*CLOCKS_PER_SEC/1000)))
		{
			if(0!=fsync(m_redoFd))
			{
				LOG(ERROR)<<"fsync redo file of block:"<<m_blockID<<" failed for errno:"<<errno<<",error info:"<<strerror(errno);
				return B_FAULT;
			}
			m_redoUnflushDataSize = 0;
			m_lastFLushTime = clock();
			return B_OK;
		}
		return B_OK;
	}
	int finishRedo()
	{
		if (!fileHandleValid(m_redoFd) && 0 != openRedoFile(true))
			return -1;
		uint64_t v = ENDOF_REDO_NUM;
		if (sizeof(v) != writeFile(m_redoFd, (char*)& v, sizeof(v)))
		{
			LOG(ERROR) << "finish redo file of block:" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
			closeFile(m_redoFd);
			m_redoFd = INVALID_HANDLE_VALUE;
			return -1;
		}
		return closeRedo();
	}
	int closeRedo() 
	{
		if (m_flag & BLOCK_FLAG_HAS_REDO && fileHandleValid(m_redoFd))
		{
			if (0 != fsync(m_redoFd))
			{
				LOG(ERROR) << "fsync redo file of block:" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
				return -1;
			}
			closeFile(m_redoFd);
			m_redoFd = INVALID_HANDLE_VALUE;
		}
		return 0;
	}
	inline tableData *getTableData(META::tableMeta * meta)
	{
		if (likely(meta != nullptr))
		{
			if (meta->userData == nullptr || m_blockID != static_cast<tableData*>(meta->userData)->blockID)
			{
				tableData * table = (tableData*)m_blockManager->allocMem(sizeof(tableData));// new tableData(m_blockID, meta, &m_arena);
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
	inline appendingBlockStaus allocMemForRecord(tableData * t, size_t size, void *&mem)
	{
		if (m_recordCount >= maxRecordInBlock)
		{
			m_flag |= BLOCK_FLAG_FINISHED;
			m_cond.wakeUp();
			return B_FULL;
		}
		if (unlikely(t->current == nullptr || t->current->pageUsedSize + size > t->current->pageSize))
		{
			size_t psize = size > t->pageSize ? size : t->pageSize;
			if(t->current == nullptr)
			{
				if (m_pageNum + 1 + t->meta->m_primaryKey.count > 0 ? 1 : 0 + t->meta->m_uniqueKeysCount >= m_maxPageNum || m_size + psize >= m_maxSize)
				{
					m_flag |= BLOCK_FLAG_FINISHED;
					m_cond.wakeUp();
					return B_FULL;
				}
				m_pageNum+=t->meta->m_primaryKey.count>0?1:0+t->meta->m_uniqueKeysCount;//every index look as a page
			}
			else
			{
				if (m_pageNum + 1 >= m_maxPageNum || m_size + psize >= m_maxSize)
				{
					m_flag |= BLOCK_FLAG_FINISHED;
					m_cond.wakeUp();
					return B_FULL;
				}
			}

			t->current = m_blockManager->allocPage(psize);
			t->current->pageId = (t->meta->m_primaryKey.count==0?0:1)+t->meta->m_uniqueKeysCount+t->pages.size();
			m_pages[m_pageNum++] = t->current;
			t->pages.append(t->current);
		}
		mem = t->current->pageData + t->current->pageUsedSize;
		return B_OK;
	}
	inline appendingBlockStaus allocMemForRecord(META::tableMeta * table, size_t size,void *&mem)
	{
		return allocMemForRecord(getTableData(table),size,mem);
	}
	inline appendingBlockStaus copyRecord(const DATABASE_INCREASE::record * record)
	{
		tableData *t = getTableData(likely(record->head->type <= DATABASE_INCREASE::R_REPLACE) ? (META::tableMeta*)((DATABASE_INCREASE::DMLRecord*)record)->meta : nullptr);
		page * current = t->current;
		if (unlikely(current == nullptr||current->pageData + current->pageUsedSize != record->data))
		{
			appendingBlockStaus s;
			void *mem;
			if (B_OK != (s = allocMemForRecord(t, record->head->size, mem)))
				return s;
			memcpy(mem, record->data, record->head->size);
			current = t->current;
		}
		((DATABASE_INCREASE::recordHead*)(current->pageData+ current->pageUsedSize))->recordId = m_minRecordId + m_recordCount;
		setRecordPosition(m_recordIDs[m_recordCount], current->pageId, current->pageUsedSize);
		current->pageUsedSize += record->head->size;
		return B_OK;
	}
	appendingBlockStaus append(const DATABASE_INCREASE::record * record)
	{
		if (m_maxLogOffset > record->head->logOffset)
		{
			LOG(ERROR) << "can not append record to block for record log offset "<<record->head->logOffset<<"is less than max log offset:"<< m_maxLogOffset
			<< "record type:"<<record->head->type;
			return B_ILLEGAL;
		}
		appendingBlockStaus s;
		if ((s = copyRecord(record))!=B_OK)
			return s;
		if (m_flag&BLOCK_FLAG_HAS_REDO)
		{
			appendingBlockStaus rtv;
			if (B_OK != (rtv = writeRedo(record->data)))
			{
				LOG(ERROR) << "write redo log failed";
				return rtv;
			}
		}
		if (record->head->type <= DATABASE_INCREASE::R_REPLACE) //build index
		{
			const META::tableMeta * meta = ((const DATABASE_INCREASE::DMLRecord*)record)->meta;
			tableData * table = static_cast<tableData*>(meta->userData);
			table->recordIds.append(m_recordCount);
			if (table->primaryKey)
				table->primaryKey->append((const DATABASE_INCREASE::DMLRecord*)record, m_recordCount);
			for (int i = 0; i < meta->m_uniqueKeysCount; i++)
				table->uniqueKeys[i]->append((const DATABASE_INCREASE::DMLRecord*)record, m_recordCount);
		}
		barrier;
		m_maxLogOffset = record->head->logOffset;
		if(m_minLogOffset ==0)
			m_minLogOffset = record->head->logOffset;
		if (m_minTime == 0)
			m_minTime = record->head->timestamp;
		if (m_maxTime < record->head->timestamp)
			m_maxTime = record->head->timestamp;
		m_recordCount++;
		if (record->head->txnId > m_txnId || record->head->type == DATABASE_INCREASE::R_DDL)
			m_committedRecordID.store(m_recordCount, std::memory_order_release);
		m_txnId = record->head->txnId;
		m_cond.wakeUp();
		return B_OK;
	}
	page* createSolidIndexPage(appendingIndex *index,uint16_t *columnIdxs,uint16_t columnCount,const META::tableMeta * meta)
	{
		page * p = m_blockManager->allocPage(index->toSolidIndexSize());
		if(columnCount==1)
		{
	        switch(meta->m_columns[columnIdxs[0]].m_columnType)
	        {
	        case T_INT8:
	        	index->toString<int8_t>(p->pageData);
				break;
			case T_UINT8:
	        	index->toString<uint8_t>(p->pageData);
				break;
	        case T_INT16:
	        	index->toString<int16_t>(p->pageData);
				break;
			case T_UINT16:
	        	index->toString<uint16_t>(p->pageData);
				break;
			case T_INT32:
	        	index->toString<int32_t>(p->pageData);
				break;
			case T_UINT32:
	        	index->toString<uint32_t>(p->pageData);
				break;
	      case T_INT64:
	        	index->toString<int64_t>(p->pageData);
				break;
			case T_TIMESTAMP:
			case T_UINT64:
	        	index->toString<uint64_t>(p->pageData);
	         break;
			case T_FLOAT:
	        	index->toString<float>(p->pageData);
				break;
			case T_DOUBLE:
				index->toString<double>(p->pageData);
				break;
			case T_BLOB:
			case T_STRING:
				index->toString<binaryType>(p->pageData);
				break;
	       default:
				abort();
	        }
		}
		else
		{
			index->toString<unionKey>(p->pageData);
		}
		p->pageUsedSize =p->pageSize;
		return p;
	}
	solidBlock * toSolidBlock()
	{
		if (!use())
			return nullptr;
		solidBlock * block = new solidBlock(m_blockManager, m_metaDataCollection);
		uint32_t firstPageSize = sizeof(tableDataInfo)*m_tableCount+(sizeof(recordGeneralInfo)+sizeof(uint32_t))*m_recordCount+sizeof(uint64_t)*(m_pageCount+1)+m_pageCount*offsetof(page, _ref);
		block->firstPage = m_blockManager->allocPage(firstPageSize);
		block->pages = (page**)m_blockManager->allocMem(sizeof(page*)*m_pageCount);
		memcpy(&block->m_version,&m_version,sizeof(block)-offsetof(STORE::block,m_version));
		block->m_flag &= (~BLOCK_FLAG_APPENDING);
		block->m_flag |= BLOCK_FLAG_FINISHED;
		char * pos = block->firstPage->pageData;
		block->m_tableInfo = (tableDataInfo*)pos;
		pos += sizeof(tableDataInfo)*m_tableCount;
		block->m_recordInfos = (recordGeneralInfo*)pos;
		pos += sizeof(recordGeneralInfo)*m_recordCount;
		block->m_recordIdOrderyTable = (uint32_t *)pos;
		pos+=sizeof(uint32_t)*m_recordCount;
		block->pageOffsets = (uint64_t*)pos;
		pos += sizeof(uint64_t)*(m_pageCount+1);
		block->pages = (page**)m_blockManager->allocMem(sizeof(page*)*m_pageCount);
		uint16_t pageId = 0;
		uint16_t tableIdx = 0;
		uint32_t recordIdsOffset = 0;
		for(std::map<uint64_t,tableData*>::iterator iter = m_tableDatas.begin();iter!=m_tableDatas.end();iter++)
		{
			block->m_tableInfo[tableIdx].firstPageId = pageId;
			tableData* t = iter->second;
			uint16_t keyPageCount= 0;
			if(t->meta!=nullptr)
			{
				keyPageCount = t->meta->m_primaryKey.count>0?1:0+t->meta->m_uniqueKeysCount;
				if(t->primaryKey!=nullptr)
				{
					block->pages[pageId] = createSolidIndexPage(t->primaryKey,t->meta->m_primaryKey.keyIndexs,t->meta->m_primaryKey.count,t->meta);
					block->pages[pageId]->pageId = pageId;
					pageId++;
				}
				if(t->uniqueKeys!=nullptr)
				{
					for(uint16_t idx=0;idx<t->meta->m_uniqueKeysCount;idx++)
					{
						block->pages[pageId] = createSolidIndexPage(t->uniqueKeys[idx],t->meta->m_uniqueKeys[idx].keyIndexs,t->meta->m_uniqueKeys[idx].count,t->meta);
						block->pages[pageId]->pageId = pageId;
						pageId++;
					}
				}
				block->m_tableInfo[tableIdx].tableId = t->meta->m_id;
			}
			else
				block->m_tableInfo[tableIdx].tableId = 0;

			arrayList<page*>::iterator piter ;
			t->pages.begin(piter);
			do{
				block->pages[pageId] = piter.value();
				block->pages[pageId]->pageId = pageId;
				pageId++;
			}while(piter.next());
			arrayList<uint32_t>::iterator riter ;
			t->recordIds.begin(riter);
			block->m_tableInfo[tableIdx].recordCount = t->recordIds.size();
			block->m_tableInfo[tableIdx].recordIdsOffset =recordIdsOffset;
			do{
				uint32_t rid = riter.value();
				uint32_t &currentOffset = m_recordIDs[rid],&newOffset =block->m_recordInfos[rid].offset;
				setRecordPosition(newOffset, pageId(currentOffset)+keyPageCount,offsetInPage(currentOffset));
				block->m_recordIdOrderyTable[recordIdsOffset++] = rid;
				const DATABASE_INCREASE::recordHead *head = (const DATABASE_INCREASE::recordHead *)getRecord(rid);
				block->m_recordInfos[rid].tableIndex = tableIdx;
				block->m_recordInfos[rid].recordType = head->type;
				block->m_recordInfos[rid].timestamp = head->timestamp;
				block->m_recordInfos[rid].logOffset = head->logOffset;
			}while(riter.next());
			tableIdx++;
		}
		unuse();
		return block;
	}
	inline void commit()
	{
		if (likely(m_recordCount > 0))
		{
			m_maxTxnId = ((const DATABASE_INCREASE::recordHead*)(getRecord(m_minRecordId + m_recordCount - 1)))->txnId;
			if (unlikely(m_minTxnId == 0))
				m_minTxnId = m_maxTxnId;
			m_committedRecordID.store(m_recordCount, std::memory_order_relaxed);
		}
	}
	inline void rollback()//todo
	{

	}
};

class appendingBlockIterator: public iterator
{
private:
	appendingBlock * m_block;
	filter * m_filter;
	uint32_t m_recordId;
	uint32_t m_seekedId;
public:
	appendingBlockIterator(appendingBlock * block, filter * filter,
			uint32_t recordId = 0, uint32_t flag = 0) :
			iterator(flag, filter), m_block(block), m_filter(filter), m_recordId(
				recordId), m_seekedId(recordId)
	{
		seekByRecordId(block->m_minRecordId);
	}
	~appendingBlockIterator()
	{
		if (m_block != nullptr)
			m_block->unuse();
	}
	/*
find record by timestamp
[IN]timestamp ,start time by micro second
[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
*/
	bool seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter)//timestamp not increase strictly
	{
		m_status = UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxTime > timestamp || m_block->m_minTime < timestamp)
			return false;
		m_recordId = -1;
		while (next())
		{
			if (!valid())
				return false;
			const DATABASE_INCREASE::recordHead* h = static_cast<const DATABASE_INCREASE::recordHead*>(value());
			if (h->timestamp >= timestamp)
			{
				if (!equalOrAfter)
				{
					m_status = OK;
					return true;
				}
				else if (h->timestamp <= timestamp + interval)
				{
					m_status = OK;
					return true;
				}
				else
				{
					m_recordId = 0xfffffffeu;
					return false;
				}
			}
		}
		if (m_status != INVALID)
			m_recordId = 0xfffffffeu;
		return false;
	}
	bool seekByLogOffset(uint64_t logOffset, bool equalOrAfter)
	{
		m_status = UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxLogOffset > logOffset || m_block->m_minLogOffset < logOffset)
			return false;
		int s = 0, e = m_block->m_recordCount - 1, m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			uint64_t offset = ((const DATABASE_INCREASE::recordHead*)m_block->getRecord(m))->logOffset;
			if (logOffset > offset)
				s = m + 1;
			else if (logOffset < offset)
				e = m - 1;
			else
				goto FIND;
		}
		if (equalOrAfter)
			return false;
		if (e < 0)
			m = 0;
	FIND:
		m_recordId = m - 1;
		if (!next())
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		m_status = OK;
		return true;
	}
	bool seekByRecordId(uint64_t recordId)
	{
		m_status = UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_minRecordId > recordId || m_block->m_minRecordId + m_block->m_recordCount < recordId)
			return false;
		m_recordId = recordId - m_block->m_minRecordId - 1;
		if (!next())
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		m_status = OK;
		return true;
	}
	void resetBlock(appendingBlock* block,uint32_t id = 0)
	{
		if (m_block != nullptr)
			m_block->unuse();
		m_recordId = id;
		if (m_recordId == 0 && block->m_recordCount == 0)
		{
			m_status = BLOCKED;
			return;
		}
		if (!valid())
		{
			m_status = INVALID;
			return;
		}
		if (!m_filter->filterByRecord(m_block->getRecord(id)))
			next();
		else
			m_status = OK;
	}
	inline const void* value() const
	{
		return m_block->getRecord(m_recordId);
	}
	inline status next()
	{
		do
		{
			if (m_seekedId +1>=m_block->m_recordCount)
			{
				if (m_block->m_flag & BLOCK_FLAG_FINISHED)
					return m_status = ENDED;
				if (m_flag & ITER_FLAG_SCHEDULE)
				{
					m_block->m_cond.wait();
					return  m_status = BLOCKED;
				}
				if (m_flag & ITER_FLAG_WAIT)
				{
					std::this_thread::sleep_for(std::chrono::nanoseconds(10000));
					continue;
				}
			}
			m_seekedId++;
		} while (!m_filter->filterByRecord(m_block->getRecord(m_seekedId)));
		m_recordId = m_seekedId;
		return  m_status = OK;
	}
	inline bool end()
	{
		if (m_seekedId + 1 >= m_block->m_recordCount&& m_block->m_flag & BLOCK_FLAG_FINISHED)
			return true;
		return false;
	}
	inline bool valid()
	{
		return m_block != nullptr && m_recordId < m_block->m_recordCount;
	}
};
#pragma pack()
}

