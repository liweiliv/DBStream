#include "appendingBlock.h"
namespace DATABASE {
	static RPC::DMLRecord dml;
	static RPC::DDLRecord ddl;
	AppendingBlock::tableData::tableData(uint64_t blockID, const META::TableMeta* meta,
		leveldb::Arena* arena, uint32_t _pageSize) :
		blockID(blockID), meta(meta), primaryKey(nullptr), uniqueKeys(
			nullptr), recordIds(arena), pages(arena), current(
				nullptr), pageSize(_pageSize)
	{
		if (meta != nullptr)
		{
			if (meta->m_primaryKey != nullptr)
				primaryKey = new appendingIndex(
					meta->m_primaryKey, meta, arena);
			if (meta->m_uniqueKeysCount > 0)
			{
				uniqueKeys = new appendingIndex * [meta->m_uniqueKeysCount];
				for (uint16_t i = 0; i < meta->m_uniqueKeysCount; i++)
					uniqueKeys[i] = new appendingIndex(
						meta->m_uniqueKeys[i], meta, arena);
			}
		}
	}
	void AppendingBlock::tableData::clean()
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
			delete[]uniqueKeys;
			uniqueKeys = nullptr;
		}
	}
	void AppendingBlock::tableData::init(uint64_t blockID, const META::TableMeta* meta,
		leveldb::Arena* arena, uint32_t _pageSize)
	{
		this->blockID = blockID;
		this->meta = meta;
		if (meta != nullptr)
		{
			if (meta->m_primaryKey != nullptr)
				primaryKey = new appendingIndex(
					meta->m_primaryKey, meta, arena);
			else
				primaryKey = nullptr;
			if (meta->m_uniqueKeysCount > 0)
			{
				uniqueKeys = new appendingIndex * [meta->m_uniqueKeysCount];
				for (uint16_t i = 0; i < meta->m_uniqueKeysCount; i++)
					uniqueKeys[i] = new appendingIndex(
						meta->m_uniqueKeys[i], meta, arena);
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
	int AppendingBlock::openRedoFile(bool w)
	{
		char fileName[524];
		m_database->genBlockFileName(m_blockID, fileName);
		strcat(fileName, ".redo");
		m_redoFd = openFile(fileName, true, true,
			true);
		if (!fileHandleValid(m_redoFd))
		{
			LOG(ERROR) << "open redo file :" << fileName << " failed for errno:" << errno << ",error info:" << strerror(errno);
			return -1;
		}
		if (w)
		{
			int64_t size = seekFile(m_redoFd, 0, SEEK_END);
			if (size < 0)
			{
				LOG(ERROR) << "open redo file :" << fileName << "failed for errno:" << errno << ",error info:" << strerror(errno);
				closeFile(m_redoFd);
				m_redoFd = INVALID_HANDLE_VALUE;
				return -1;
			}
			else if (size == 0)
			{
				if (sizeof(m_minLogOffset) != writeFile(m_redoFd, (char*)&m_minLogOffset, sizeof(m_minLogOffset)))
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
	/*
* 	return value
* 		1: redo file not ended,and read success
* 			0: redo file ended,and  read success
* 				-1:read redo file failed
* 					*/
	int AppendingBlock::recoveryFromRedo()
	{
		int ret = 1;
		if (m_redoFd > 0)
			closeFile(m_redoFd);
		if (0 > openRedoFile(false))
		{
			LOG(ERROR) << "open redo file of block" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
			return -1;
		}
		int size = seekFile(m_redoFd, 0, SEEK_END); //get fileSize
		if (size < 0)
		{
			LOG(ERROR) << "get size of  redo file of block :" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
			return -1;
		}
		if (size < (int)sizeof(uint64_t)) //empty file
			return 0;
		char* buf = (char*)malloc(size);
		if (buf == NULL)
		{
			LOG(ERROR) << "alloc " << size << " byte memory failed";
			return -1;
		}
		if (0 != seekFile(m_redoFd, 0, SEEK_SET)) //seek to begin of file
		{
			free(buf);
			LOG(ERROR) << "leeek to begin of redo file :" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
			return -1;
		}
		if (size != readFile(m_redoFd, buf, size)) //read all data one time
		{
			free(buf);
			LOG(ERROR) << "read redo file of block :" << m_blockID << ".redo failed for errno:" << errno << ",error info:" << strerror(errno);
			return -1;
		}
		m_flag &= (~BLOCK_FLAG_HAS_REDO); //unset BLOCK_FLAG_HAS_REDO,so call [append] will not write redo file
		m_minRecordId = *(uint64_t*)buf;
		RPC::RecordHead* head = (RPC::RecordHead*)(buf + sizeof(uint64_t));
		while ((char*)head <= buf + size)
		{
			/*redo end normally*/
			if (unlikely(((char*)head - buf) + sizeof(RPC::MinRecordHead::size) == (uint32_t)size) && head->minHead.size == ENDOF_REDO_NUM)
			{
				ret = 0;
				break;
			}

			/*redo truncated*/
			if (((char*)head) + sizeof(RPC::RecordHead) > buf + size || ((char*)head) + head->minHead.size > buf + size) //unfinished write ,truncate
			{
				LOG(WARNING) << "get an incomplete redo data in redo file of block:" << m_blockID << ",offset is " << ((char*)head) - buf << ",truncate it";

				if (((char*)head) - buf != seekFile(m_redoFd, ((char*)head) - buf, SEEK_SET) || truncateFile(m_redoFd, ((char*)head) - buf) != 0)
				{
					LOG(WARNING) << "truncate redo file of block:" << m_blockID << "failed for:errno" << errno << "," << strerror(errno);
					free(buf);
					return -1;
				}
				break;
			}
			if (append(RPC::createRecord((const char*)head, m_metaDataCollection)) != appendingBlockStaus::B_OK)
			{
				LOG(ERROR) << "recoveryFromRedo from redo file of block:" << m_blockID << " failed for append data failed";
				free(buf);
				m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
				return -1;
			}
			head = (RPC::RecordHead*)(((char*)head) + head->minHead.size);
		}
		m_flag |= BLOCK_FLAG_HAS_REDO; //reset BLOCK_FLAG_HAS_REDO
		LOG(INFO) << "recoveryFromRedo from redo file :" << m_blockID << " success";
		free(buf);
		return ret;
	}
	AppendingBlock::appendingBlockStaus AppendingBlock::writeRedo(const char* data)
	{
		if (!fileHandleValid(m_redoFd) && 0 != openRedoFile(true))
			return appendingBlockStaus::B_FAULT;
		RPC::RecordHead* head = (RPC::RecordHead*)data;
		int64_t writeSize;
		if (head->minHead.size != (uint64_t)(writeSize = writeFile(m_redoFd, data, head->minHead.size)))
		{
			if (errno == EBADF) //maybe out time or other cause,reopen it
			{
				LOG(ERROR) << "write redo file of block:" << m_blockID << " failed for " << strerror(errno) << " reopen it";
				return appendingBlockStaus::B_FAULT;
			}
			else
			{
				LOG(ERROR) << "write redo file of block :" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
				return appendingBlockStaus::B_FAULT;
			}
		}
		clock_t now;
		if (m_redoFlushDataSize == 0 || //m_redoFlushDataSize == 0 means flush immediately
			m_redoFlushPeriod == 0 ||//m_redoFlushPeriod == 0 also means flush immediately
			(m_redoFlushDataSize > 0 && (m_redoUnflushDataSize += head->minHead.size) >= m_redoFlushDataSize) ||//check if unflushed data big enough
			(m_redoFlushPeriod > 0 &&//check if time from last flush is long enough
				(now = clock(),
					now<m_lastFLushTime || now - m_lastFLushTime > m_redoFlushPeriod * CLOCKS_PER_SEC / 1000)))
		{
			if (0 != fsync(m_redoFd))
			{
				LOG(ERROR) << "fsync redo file of block:" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
				return appendingBlockStaus::B_FAULT;
			}
			m_redoUnflushDataSize = 0;
			m_lastFLushTime = clock();
			return appendingBlockStaus::B_OK;
		}
		return appendingBlockStaus::B_OK;
	}
	int AppendingBlock::finishRedo()
	{
		if (!fileHandleValid(m_redoFd) && 0 != openRedoFile(true))
			return -1;
		uint64_t v = ENDOF_REDO_NUM;
		if (sizeof(v) != writeFile(m_redoFd, (char*)&v, sizeof(v)))
		{
			LOG(ERROR) << "finish redo file of block:" << m_blockID << " failed for errno:" << errno << ",error info:" << strerror(errno);
			closeFile(m_redoFd);
			m_redoFd = INVALID_HANDLE_VALUE;
			return -1;
		}
		return closeRedo();
	}
	int AppendingBlock::closeRedo()
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
	AppendingBlock::appendingBlockStaus AppendingBlock::allocMemForRecord(tableData* t, size_t size, void*& mem)
	{
		if (m_recordCount >= maxRecordInBlock)
		{
			m_flag |= BLOCK_FLAG_FINISHED;
			m_cond.wakeUp();
			return appendingBlockStaus::B_FULL;
		}
		if (unlikely(t->current == nullptr || t->current->pageUsedSize + size > t->current->pageSize))
		{
			size_t psize = size > t->pageSize ? size : t->pageSize;
			if (t->current == nullptr)
			{
				if ((m_pageCount + 1 + (t->meta == nullptr ? 0 : ((t->meta->m_primaryKey != nullptr ? 1 : 0) + t->meta->m_uniqueKeysCount))) >= m_maxPageCount || m_size + psize >= m_maxSize)
				{
					m_flag |= BLOCK_FLAG_FINISHED;
					m_cond.wakeUp();
					return appendingBlockStaus::B_FULL;
				}
				m_pageCount += 1 + (t->meta == nullptr ? 0 : ((t->meta->m_primaryKey != nullptr ? 1 : 0) + t->meta->m_uniqueKeysCount));//every index look as a page
			}
			else
			{
				if (m_pageCount + 1 >= m_maxPageCount || m_size + psize >= m_maxSize)
				{
					m_flag |= BLOCK_FLAG_FINISHED;
					m_cond.wakeUp();
					return appendingBlockStaus::B_FULL;
				}
				m_pageCount++;
			}
			t->current = m_database->allocPage(psize);
			t->current->pageId = m_pageCount - 1;
			m_pages[m_pageCount - 1] = t->current;
			m_size += t->current->pageSize;
			t->pages.append(t->current);
		}
		mem = t->current->pageData + t->current->pageUsedSize;
		return appendingBlockStaus::B_OK;
	}
	inline AppendingBlock::appendingBlockStaus AppendingBlock::copyRecord(const RPC::Record*& record)
	{
		tableData* t = getTableData(likely(record->head->minHead.type <= static_cast<uint8_t>(RPC::RecordType::R_REPLACE)) ? (META::TableMeta*)((RPC::DMLRecord*)record)->meta : nullptr);
		Page* current = t->current;
		if (unlikely(current == nullptr || current->pageData + current->pageUsedSize != record->data))
		{
			appendingBlockStaus s;
			void* mem;
			if (appendingBlockStaus::B_OK != (s = allocMemForRecord(t, record->head->minHead.size, mem)))
				return s;
			memcpy(mem, record->data, record->head->minHead.size);
			current = t->current;
			if (likely(record->head->minHead.type <= static_cast<uint8_t>(RPC::RecordType::R_REPLACE)))
			{
				dml.load((char*)mem, ((const RPC::DMLRecord*)record)->meta);
				record = &dml;
			}
			else if (record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL))
			{
				ddl.load((char*)mem);
				record = &ddl;
			}
			else
				abort();
		}
		((RPC::RecordHead*)(current->pageData + current->pageUsedSize))->recordId = m_minRecordId + m_recordCount;
		setRecordPosition(m_recordIDs[m_recordCount], current->pageId, current->pageUsedSize);
		current->pageUsedSize += record->head->minHead.size;
		return appendingBlockStaus::B_OK;
	}
	AppendingBlock::appendingBlockStaus AppendingBlock::append(const RPC::Record* record)
	{
		if (unlikely(m_maxLogOffset > record->head->checkpoint.logOffset))
		{
			LOG(ERROR) << "can not append record to block for record log offset " << record->head->checkpoint.logOffset << "is less than max log offset:" << m_maxLogOffset
				<< "record type:" << record->head->minHead.type;
			return appendingBlockStaus::B_ILLEGAL;
		}
		appendingBlockStaus s;
		if (record->head->minHead.type >= 4)
			vSave(record, record->head->minHead.size + sizeof(RPC::DMLRecord));
		else
			vSave(record, record->head->minHead.size + sizeof(RPC::DDLRecord));
		if ((s = copyRecord(record)) != appendingBlockStaus::B_OK)
			return s;
		if (m_flag & BLOCK_FLAG_HAS_REDO)
		{
			appendingBlockStaus rtv;
			if (appendingBlockStaus::B_OK != (rtv = writeRedo(record->data)))
			{
				LOG(ERROR) << "write redo log failed";
				return rtv;
			}
		}
		if (record->head->minHead.type <= static_cast<uint8_t>(RPC::RecordType::R_REPLACE)) //build index
		{
			const META::TableMeta* meta = ((const RPC::DMLRecord*)record)->meta;
			tableData* table = static_cast<tableData*>(meta->userData);
			table->recordIds.append(m_recordCount);
			if (table->primaryKey)
				table->primaryKey->append((const RPC::DMLRecord*)record, m_recordCount);
			for (int i = 0; i < meta->m_uniqueKeysCount; i++)
				table->uniqueKeys[i]->append((const RPC::DMLRecord*)record, m_recordCount);
		}
		m_maxLogOffset = record->head->checkpoint.logOffset;
		if (m_minLogOffset == 0)
			m_minLogOffset = record->head->checkpoint.logOffset;
		if (m_minTime == 0)
			m_minTime = record->head->checkpoint.timestamp;
		if (m_maxTime < record->head->checkpoint.timestamp)
			m_maxTime = record->head->checkpoint.timestamp;
		m_recordCount++;
		if (record->head->checkpoint.txnId > m_txnId || record->head->minHead.type == static_cast<uint8_t>(RPC::RecordType::R_DDL))
			m_committedRecordID.store(m_recordCount, std::memory_order_relaxed);
		m_txnId = record->head->checkpoint.txnId;
		m_cond.wakeUp();
		return appendingBlockStaus::B_OK;
	}
	Page* AppendingBlock::createSolidIndexPage(appendingIndex* index, const META::UnionKeyMeta* ukMeta, const META::TableMeta* meta)
	{
		Page* p = m_database->allocPage(index->toSolidIndexSize());
		memset(p->pageData, 0, p->pageSize);
		if (ukMeta->columnCount == 1)
		{
			switch (static_cast<META::COLUMN_TYPE>(ukMeta->columnInfo[0].type))
			{
			case META::COLUMN_TYPE::T_INT8:
				index->toString<int8_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_UINT8:
				index->toString<uint8_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_INT16:
				index->toString<int16_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_UINT16:
				index->toString<uint16_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_INT32:
				index->toString<int32_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_UINT32:
				index->toString<uint32_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_INT64:
				index->toString<int64_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			case META::COLUMN_TYPE::T_UINT64:
				index->toString<uint64_t>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				index->toString<float>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				index->toString<double>(p->pageData);
				break;
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_STRING:
				index->toString<META::BinaryType>(p->pageData);
				break;
			default:
				abort();
			}
		}
		else
		{
			index->toString<META::unionKey>(p->pageData);
		}
		p->pageUsedSize = p->pageSize = ((solidIndexHead*)(p->pageData))->size;
		return p;
	}
	SolidBlock* AppendingBlock::toSolidBlock()
	{
		if (!use())
			return nullptr;
		uint32_t memSize = 0;
		uint32_t* pageMap = (uint32_t*)m_arena.Allocate(sizeof(uint32_t) * m_pageCount);
		SolidBlock* block = new SolidBlock(m_blockID, m_database, m_metaDataCollection, (m_flag & (~BLOCK_FLAG_APPENDING)) | BLOCK_FLAG_FINISHED | BLOCK_FLAG_SOLID);
		uint32_t firstPageSize = sizeof(TableDataInfo) * m_tableCount + (sizeof(RecordGeneralInfo) + sizeof(uint32_t)) * m_recordCount + sizeof(uint64_t) * (m_pageCount + 1) + m_pageCount * offsetof(Page, _ref);
		block->firstPage = m_database->allocPage(firstPageSize);
		memset(block->firstPage->pageData, 0, block->firstPage->pageSize);
		block->pages = (Page**)m_database->allocMem(sizeof(Page*) * m_pageCount);
		block->m_loading.store(static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED), std::memory_order_relaxed);
		memcpy(&block->m_blockID, &m_blockID, offsetof(SolidBlock, m_fd) - offsetof(SolidBlock, m_blockID));
		char* pos = block->firstPage->pageData;
		block->m_tableInfo = (TableDataInfo*)pos;
		pos += sizeof(TableDataInfo) * m_tableCount;
		block->m_recordInfos = (RecordGeneralInfo*)pos;
		pos += sizeof(RecordGeneralInfo) * m_recordCount;
		block->m_recordIdOrderyTable = (uint32_t*)pos;
		pos += sizeof(uint32_t) * m_recordCount;
		block->pageOffsets = (uint64_t*)pos;
		pos += sizeof(uint64_t) * (m_pageCount + 1);
		block->pageInfo = pos;
		block->firstPage->pageUsedSize = firstPageSize;
		uint16_t pageId = 0;
		uint16_t tableIdx = 0;
		uint32_t recordIdsOffset = 0;
		assert(m_tableCount == m_tableDatas.size());
		for (std::map<uint64_t, tableData*>::iterator iter = m_tableDatas.begin(); iter != m_tableDatas.end(); iter++)
		{
			block->m_tableInfo[tableIdx].firstPageId = pageId;
			tableData* t = iter->second;
			if (t->meta != nullptr)
			{
				if (t->primaryKey != nullptr)
				{
					block->pages[pageId] = createSolidIndexPage(t->primaryKey, t->meta->m_primaryKey, t->meta);
					block->pages[pageId]->pageId = pageId;
					memSize += block->pages[pageId]->pageUsedSize;

					pageId++;
				}
				if (t->uniqueKeys != nullptr)
				{
					for (uint16_t idx = 0; idx < t->meta->m_uniqueKeysCount; idx++)
					{
						block->pages[pageId] = createSolidIndexPage(t->uniqueKeys[idx], t->meta->m_uniqueKeys[idx], t->meta);
						block->pages[pageId]->pageId = pageId;
						memSize += block->pages[pageId]->pageUsedSize;

						pageId++;
					}
				}
				block->m_tableInfo[tableIdx].tableId = t->meta->m_id;
			}
			else
				block->m_tableInfo[tableIdx].tableId = 0;
			if (!t->pages.empty())
			{
				arrayList<Page*>::iterator piter;
				t->pages.begin(piter);
				do {
					pageMap[piter.value()->pageId] = pageId;
					block->pages[pageId] = piter.value();
					block->pages[pageId]->pageId = pageId;
					memSize += block->pages[pageId]->pageUsedSize;
					pageId++;
				} while (piter.next());
			}
			if (!t->recordIds.empty())
			{
				arrayList<uint32_t>::iterator riter;
				t->recordIds.begin(riter);
				block->m_tableInfo[tableIdx].recordCount = t->recordIds.size();
				block->m_tableInfo[tableIdx].recordIdsOffset = recordIdsOffset;
				do {
					uint32_t rid = riter.value();
					uint32_t& currentOffset = m_recordIDs[rid], & newOffset = block->m_recordInfos[rid].offset;
					setRecordPosition(newOffset, pageMap[pageId(currentOffset)], offsetInPage(currentOffset));
					block->m_recordIdOrderyTable[recordIdsOffset++] = rid;
					const RPC::RecordHead* head = (const RPC::RecordHead*)getRecordByIdx(rid);
					block->m_recordInfos[rid].tableIndex = tableIdx;
					block->m_recordInfos[rid].recordType = head->minHead.type;
					block->m_recordInfos[rid].timestamp = head->checkpoint.timestamp;
					block->m_recordInfos[rid].logOffset = head->checkpoint.logOffset;
				} while (riter.next());
			}
			tableIdx++;
		}
		assert(pageId == m_pageCount);
		char* pageInfoPos = block->pageInfo;
		for (int i = 0; i < pageId; i++)
		{
			memcpy(pageInfoPos, (void*)block->pages[i], offsetof(Page, _ref));
			pageInfoPos += offsetof(Page, _ref);
		}
		unuse();
		LOG(INFO) << "block:" << m_blockID << " trans to solid block success,size:" << memSize << ",record count:" << m_recordCount << ",table count:" << m_tableCount;
		return block;
	}

	BlockIndexIterator* AppendingBlock::createIndexIterator(uint32_t flag, const META::TableMeta* table, META::KEY_TYPE type, int keyId)
	{
		if (!use())
			return nullptr;
		tableData* tableInfo = getTableData(table->m_id);
		if (tableInfo == nullptr)
		{
			unuse();
			return nullptr;
		}
		appendingIndex* index = getTableIndex(tableInfo, table, type, keyId);
		if (index == nullptr)
		{
			unuse();
			return nullptr;
		}
		return new AppendingBlockIndexIterator(flag, this, index);
	}

	AppendingBlockIndexIterator::AppendingBlockIndexIterator(uint32_t flag, AppendingBlock* block, appendingIndex* index) :BlockIndexIterator(0, nullptr, block->m_blockID), m_index(index), m_block(block), indexIter(nullptr)
	{
		m_table = m_block->getTableData(index->getMeta()->m_id);
		switch (index->getType())
		{
		case META::COLUMN_TYPE::T_UNION:
			indexIter = new appendingIndex::iterator<META::unionKey>(flag, index);
			break;
		case META::COLUMN_TYPE::T_INT8:
			indexIter = new appendingIndex::iterator<int8_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_UINT8:
			indexIter = new appendingIndex::iterator<uint8_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_INT16:
			indexIter = new appendingIndex::iterator<int16_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_UINT16:
			indexIter = new appendingIndex::iterator<uint16_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_INT32:
			indexIter = new appendingIndex::iterator<int32_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_UINT32:
			indexIter = new appendingIndex::iterator<uint32_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_INT64:
			indexIter = new appendingIndex::iterator<int64_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_TIMESTAMP:
		case META::COLUMN_TYPE::T_UINT64:
			indexIter = new appendingIndex::iterator<uint64_t>(flag, index);
			break;
		case META::COLUMN_TYPE::T_FLOAT:
			indexIter = new appendingIndex::iterator<float>(flag, index);
			break;
		case META::COLUMN_TYPE::T_DOUBLE:
			indexIter = new appendingIndex::iterator<double>(flag, index);
			break;
		case META::COLUMN_TYPE::T_BLOB:
		case META::COLUMN_TYPE::T_STRING:
			indexIter = new appendingIndex::iterator<META::BinaryType>(flag, index);
			break;
		default:
			abort();
		}
	}
}
