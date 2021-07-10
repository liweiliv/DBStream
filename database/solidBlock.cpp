#include "solidBlock.h"
#include "solidIndex.h"
#include "database.h"
namespace DATABASE
{
	SolidBlock::~SolidBlock()
	{
		assert(m_flag & BLOCK_FLAG_FLUSHED);
		if (pages != nullptr)
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
	char* SolidBlock::getRecordByInnerId(uint32_t recordId)
	{
		if (recordId >= m_recordCount)
			return nullptr;
		uint16_t pId = pageId(m_recordInfos[recordId].offset);
		Page* p = getPage(pId);
		if (unlikely(p == nullptr))
			return nullptr;
		const char* v = getRecordFromPage(p, recordId);
		char* newRecord = (char*)m_database->allocMem(((const RPC::MinRecordHead*)v)->size);
		memcpy(newRecord, v, ((const RPC::MinRecordHead*)v)->size);
		p->unuse();
		return newRecord;
	}
	int SolidBlock::load(int id)
	{
		uint8_t loadStatus = m_loading.load(std::memory_order_relaxed);
		if (loadStatus >= static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED))
			return 0;
		m_fileLock.lock();
		if (m_loading.load(std::memory_order_relaxed) >= static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED))
		{
			m_fileLock.unlock();
			return 0;
		}
		if (!fileHandleValid(m_fd))
		{
			char fileName[524];
			m_database->genBlockFileName(m_blockID, fileName);
			m_fd = openFile(fileName, true, false, false);
			if (!fileHandleValid(m_fd))
			{
				m_fileLock.unlock();
				m_loading.store(static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_UNLOAD), std::memory_order_relaxed);
				LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
				return -1;
			}
		}
		m_fileLock.unlock();
		if (m_blockID == 0)
		{
			if (0 != loadBlockInfo(m_fd, id))
			{
				LOG(ERROR) << "load block file:" << id << " failed ";
				return -1;
			}
		}
		if (0 != loadFirstPage())
		{
			LOG(ERROR) << "load block file:" << id << " failed ";
			return -1;
		}
		return 0;
	}
	int SolidBlock::loadFirstPage()
	{
		uint8_t loadStatus = m_loading.load(std::memory_order_relaxed);
		do {
			if (loadStatus == static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED_HEAD))
			{
				if (m_loading.compare_exchange_weak(loadStatus, static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADING_FIRST_PAGE), std::memory_order_relaxed,
					std::memory_order_relaxed))
					break;
			}
			else if (loadStatus == static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADING_FIRST_PAGE))
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				loadStatus = m_loading.load(std::memory_order_relaxed);
			}
			else if (loadStatus < static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED_HEAD))
			{
				LOG(ERROR) << "loadFirstPage of block file failed for it`s status is less then BLOCK_LOADED_HEAD";
				return -1;
			}
			else if (loadStatus == static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED))
				return 0;
			else
				return -1;
		} while (1);

		firstPage = (Page*)m_database->allocMem(sizeof(Page));
		firstPage->pageSize = firstPage->pageUsedSize = m_solidBlockHeadPageRawSize;
		firstPage->crc = 0;
		firstPage->pageData = nullptr;
		firstPage->pageId = 0;
		if (0 != loadPage(firstPage, m_solidBlockHeadPageSize, offsetof(SolidBlock, m_fd) - offsetof(SolidBlock, m_version)))
		{
			m_loading.store(static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOAD_FAILED), std::memory_order_relaxed);
			return -1;
		}
		char* pos = firstPage->pageData;
		m_tableInfo = (TableDataInfo*)pos;

		pos += sizeof(TableDataInfo) * m_tableCount;
		m_recordInfos = (RecordGeneralInfo*)pos;
		pos += sizeof(RecordGeneralInfo) * m_recordCount;
		if (pos > firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		m_recordIdOrderyTable = (uint32_t*)pos;
		pos += sizeof(uint32_t) * m_recordCount;
		if (pos > firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pageOffsets = (uint64_t*)pos;
		pos += sizeof(uint64_t) * (m_pageCount + 1);
		if (pos > firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pages = (Page**)m_database->allocMem(sizeof(Page*) * m_pageCount);
		if (pos + m_pageCount * offsetof(Page, _ref) > firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;
		for (uint16_t idx = 0; idx < m_pageCount; idx++)
		{
			pages[idx] = (Page*)m_database->allocMem(sizeof(Page));
			pages[idx]->_ref.m_ref.store(0, std::memory_order_relaxed);
			memcpy((void*)pages[idx], pos, offsetof(Page, _ref));
			pages[idx]->pageData = nullptr;
			pos += offsetof(Page, _ref);
		}
		m_flag |= BLOCK_FLAG_FLUSHED;
		m_loading.store(static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOADED), std::memory_order_relaxed);
		return 0;

	FIRST_PAGE_SIZE_FAULT:
		m_loading.store(static_cast<uint8_t>(BLOCK_LOAD_STATUS::BLOCK_LOAD_FAILED), std::memory_order_relaxed);
		LOG(ERROR) << "load block file:" << m_blockID << " failed for first page is illegal";
		m_database->freePage(firstPage);
		firstPage = nullptr;
		return -1;
	}
	int SolidBlock::loadPage(Page* p, size_t size, size_t offset)
	{
		if (unlikely(p->pageData != nullptr))
			return 0;
		m_fileLock.lock();
		if (p->pageData != nullptr)
		{
			m_fileLock.unlock();
			return 0;
		}
		if (!fileHandleValid(m_fd))
		{
			char fileName[524];
			m_database->genBlockFileName(m_blockID, fileName);
			m_fd = openFile(fileName, true, false, false);
			if (!fileHandleValid(m_fd))
			{
				m_fileLock.unlock();
				LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
				return -1;
			}
		}
		if (offset != (size_t)seekFile(m_fd, offset, SEEK_SET))
		{
			m_fileLock.unlock();
			LOG(ERROR) << "seek to position:" << offset << " of block " << m_blockID << " failed for" << errno << "," << strerror(errno);
			return -1;
		}
		char* data = (char*)m_database->allocMem(size);
		if (size != (size_t)readFile(m_fd, data, size))
		{
			m_fileLock.unlock();
			LOG(ERROR) << "read page in position:" << offset << " of block " << m_blockID << " failed for" << errno << "," << strerror(errno);
			return -1;
		}
		m_fileLock.unlock();
		if (p->crc != 0)//not compressed data
		{
			uint32_t crc = hwCrc32c(0, data, size);
			if (crc != p->crc)
			{
				bufferPool::free(data);
				LOG(ERROR) << "read page in position:" << offset << " of block " << m_blockID << " failed for crc check failed,expect is " <<
					p->crc << ",actually is " << crc;
				return -1;
			}
			p->pageData = data;
		}
		else
		{
			char* uncompressedData = (char*)m_database->allocMem(p->pageUsedSize);
			if (p->pageUsedSize > LZ4_MAX_INPUT_SIZE)
			{
				uint32_t* sizeArray = (uint32_t*)data;
				uint32_t arraySize = p->pageUsedSize / LZ4_MAX_INPUT_SIZE + ((p->pageUsedSize % LZ4_MAX_INPUT_SIZE) ? 1 : 0);
				char* realData = data + arraySize * sizeof(uint32_t);
				uint64_t tmpSize = 0;
				for (uint32_t idx = 0; idx < arraySize; idx++)
				{
					int32_t decompressSize;
					if ((decompressSize = LZ4_decompress_safe(realData, uncompressedData + tmpSize, sizeArray[idx], p->pageUsedSize - tmpSize)) < 0)
					{
						LOG(ERROR) << "deCompress page data in position:" << offset << " of block " << m_blockID << " failed ";
						bufferPool::free(uncompressedData);
						bufferPool::free(data);
						return -1;
					}
					tmpSize += decompressSize;
				}
			}
			else
			{
				if (LZ4_decompress_safe(data, uncompressedData, size, p->pageUsedSize) < 0)
				{
					LOG(ERROR) << "deCompress page data in position:" << offset << " of block " << m_blockID << " failed,compressed size:" << size << ",raw size:" << p->pageUsedSize;
					bufferPool::free(uncompressedData);
					bufferPool::free(data);
					return -1;
				}
			}
			p->pageData = uncompressedData;
		}
		return 0;
	}
	char* SolidBlock::getCompressbuffer(uint32_t size)
	{
		char* cbuf = compressBuffer.get();
		uint32_t* csize;
		if (cbuf == nullptr)
		{
			cbuf = new char[size];
			csize = new uint32_t;
			*csize = size;
			compressBuffer.set(cbuf);
			compressBufferSize.set(csize);
		}
		else
		{
			csize = compressBufferSize.get();
			if (*csize < size)
			{
				cbuf = new char[size];
				compressBuffer.set(cbuf);
				*csize = size;
			}
			else if (*csize > size&& size < DEFAULT_PAGE_SIZE)
			{
				cbuf = new char[DEFAULT_PAGE_SIZE];
				compressBuffer.set(cbuf);
				*csize = DEFAULT_PAGE_SIZE;
			}
		}
		return cbuf;
	}
	int64_t SolidBlock::writepage(Page* p, uint64_t offset, bool compress)
	{
		errno = 0;
		if (seekFile(m_fd, 0, SEEK_CUR) != (int64_t)offset)
		{
			if (seekFile(m_fd, offset, SEEK_SET) != (int64_t)offset)
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
				return -1;
			}
		}
		if (compress)
		{
			p->crc = 0;
			((Page*)(pageInfo + (p->pageId * offsetof(Page, _ref))))->crc = 0;
			char* compressBuf;
			uint32_t compressedSize = 0;
			if (unlikely(p->pageUsedSize > LZ4_MAX_INPUT_SIZE))
			{
				uint32_t fullCount = p->pageUsedSize / LZ4_MAX_INPUT_SIZE;
				uint64_t notFullTailSize = p->pageUsedSize % LZ4_MAX_INPUT_SIZE;
				uint64_t compressBufSize = sizeof(uint32_t) * (fullCount + 1 + notFullTailSize ? 1 : 0) + fullCount * LZ4_COMPRESSBOUND(LZ4_MAX_INPUT_SIZE) + notFullTailSize ? LZ4_COMPRESSBOUND(p->pageUsedSize % LZ4_MAX_INPUT_SIZE) : 0;
				compressBuf = getCompressbuffer(compressBufSize);
				char* compressPos = compressBuf + sizeof(uint32_t) * (fullCount + 1 + notFullTailSize ? 1 : 0), * dataPos = p->pageData;
				uint32_t idx;
				for (idx = 0; idx < fullCount; idx++)
				{
					((uint32_t*)compressBuf)[idx] = compressPos - compressBuf;
					compressPos += LZ4_compress_default(dataPos, compressPos, LZ4_MAX_INPUT_SIZE, LZ4_COMPRESSBOUND(LZ4_MAX_INPUT_SIZE));
					dataPos += LZ4_MAX_INPUT_SIZE;
				}
				if (notFullTailSize)
				{
					((uint32_t*)compressBuf)[idx++] = compressPos - compressBuf;
					compressPos += LZ4_compress_default(dataPos, compressPos, notFullTailSize, LZ4_COMPRESSBOUND(notFullTailSize));
				}
				((uint32_t*)compressBuf)[idx] = compressPos - compressBuf;
				compressedSize = compressPos - compressBuf;
			}
			else
			{
				uint32_t compressBufSize = LZ4_COMPRESSBOUND(p->pageUsedSize);
				compressBuf = getCompressbuffer(compressBufSize);
				compressedSize = LZ4_compress_default(p->pageData, compressBuf, p->pageUsedSize, compressBufSize);
				if (compressedSize != writeFile(m_fd, compressBuf, compressedSize))
				{
					LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
					return -1;
				}
			}
			return compressedSize;
		}
		else
		{
			p->crc = hwCrc32c(0, p->pageData, p->pageUsedSize);
			((Page*)(pageInfo + (p->pageId * offsetof(Page, _ref))))->crc = p->crc;
			if (p->pageUsedSize != (uint32_t)writeFile(m_fd, p->pageData, p->pageUsedSize))
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
				return -1;
			}
			return p->pageUsedSize;
		}
	}
	int SolidBlock::writeToFile()
	{
		if (m_flag & BLOCK_FLAG_FLUSHED)
			return 0;
		if (fileHandleValid(m_fd))
			closeFile(m_fd);
		char fileName[524], tmpFile[524];
		m_database->genBlockFileName(m_blockID, fileName);
		strcpy(tmpFile, fileName);
		strcat(tmpFile, ".tmp");
		remove(tmpFile);
		m_fd = openFile(tmpFile, true, true, true);
		if (!fileHandleValid(m_fd))
		{
			LOG(ERROR) << "write block file " << tmpFile << " failed for errno:" << errno << " ," << strerror(errno);
			return -1;
		}
		int writeSize = 0;
		int headSize = offsetof(SolidBlock, m_fd) - offsetof(SolidBlock, m_version);
		m_rawSize = headSize;
		uint64_t pagePos = ALIGN(headSize + LZ4_COMPRESSBOUND(firstPage->pageUsedSize), 512);
		for (uint16_t pageIdx = 0; pageIdx < m_pageCount; pageIdx++)
		{
			m_rawSize += pages[pageIdx]->pageUsedSize;
			pageOffsets[pageIdx] = pagePos;
			assert(pageIdx == pages[pageIdx]->pageId);
			pagePos = ALIGN(pagePos, 512);
			int64_t writedSize = writepage(pages[pageIdx], pagePos, m_flag & BLOCK_FLAG_COMPRESS);
			if (writedSize < 0)
				return -1;
			pagePos += writedSize;
		}
		pageOffsets[m_pageCount] = pagePos;
		m_rawSize += firstPage->pageUsedSize;

		m_solidBlockHeadPageSize = writepage(firstPage, headSize, m_flag & BLOCK_FLAG_COMPRESS);
		m_solidBlockHeadPageRawSize = firstPage->pageUsedSize;
		m_fileSize = pagePos;
		if (seekFile(m_fd, 0, SEEK_SET) != 0)
		{
			LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
			return -1;
		}
		m_crc = hwCrc32c(0, (char*)&m_version, headSize - sizeof(m_crc));
		if (headSize != (writeSize = writeFile(m_fd, (char*)&m_version, headSize)))
		{
			LOG(ERROR) << "write head info to block file " << tmpFile << " failed for errno:" << errno << " ," << strerror(errno);
			return -1;
		}

		if (0 != fsync(m_fd))
		{
			LOG(ERROR) << "flush block file " << tmpFile << " failed for errno:" << errno << " ," << strerror(errno);
			return -1;
		}
		closeFile(m_fd);
		m_fd = 0;
		if (0 != rename(tmpFile, fileName))
		{
			LOG(ERROR) << "rename tmp block file " << tmpFile << " to " << fileName << " failed for errno:" << errno << " ," << strerror(errno);
			return -1;
		}
		m_flag |= BLOCK_FLAG_FLUSHED;
		return 0;
	}
	int SolidBlock::loadFromFile()
	{
		char fileName[524];
		m_database->genBlockFileName(m_blockID, fileName);
		m_fd = openFile(fileName, true, false, false);
		if (!fileHandleValid(m_fd))
		{
			LOG(ERROR) << "open block file:" << fileName << " failed for error:" << errno << "," << strerror(errno);
			return -1;
		}
		if (0 != loadBlockInfo(m_fd, m_blockID))
		{
			closeFile(m_fd);
			m_fd = INVALID_HANDLE_VALUE;
			return -1;
		}
		return 0;
	}
	const TableDataInfo* SolidBlock::getTableInfo(uint64_t tableId)
	{
		int s = 0, e = m_tableCount - 1, m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			if (m_tableInfo[m].tableId < tableId)
				s = m + 1;
			else if (m_tableInfo[m].tableId > tableId)
				e = m - 1;
			else
				return &m_tableInfo[m];
		}
		return nullptr;
	}
	int SolidBlock::getTableIndexPageId(const TableDataInfo* tableInfo, const META::TableMeta* table, META::KEY_TYPE type, int keyId)
	{
		switch (type)
		{
		case META::KEY_TYPE::PRIMARY_KEY:
			return table->m_primaryKey != nullptr ? tableInfo->firstPageId : -1;
		case META::KEY_TYPE::UNIQUE_KEY:
		{
			if (table->m_uniqueKeysCount <= keyId)
				return -1;
			if (table->m_primaryKey != nullptr)
				return tableInfo->firstPageId + 1 + keyId;
			else
				return tableInfo->firstPageId + keyId;
		}
		case META::KEY_TYPE::INDEX:
			return -1;
		default:
			return -1;
		}
	}
	void SolidBlock::clear()
	{
		for (int i = 0; i < m_pageCount; i++)
		{
			if (pages[i] != nullptr)
			{
				pages[i]->freeWhenNoUser();
			}
		}
	}
	Page* SolidBlock::getIndex(const META::TableMeta* table, META::KEY_TYPE type, int keyId)
	{
		if (!use())
			return nullptr;
		const TableDataInfo* tableInfo = getTableInfo(table->m_id);
		if (tableInfo == nullptr)
		{
			unuse();
			return nullptr;
		}
		if (type == META::KEY_TYPE::PRIMARY_KEY)
		{
			if (table->m_primaryKey == nullptr)
			{
				unuse();
				return nullptr;
			}
		}
		else if (type == META::KEY_TYPE::UNIQUE_KEY)
		{
			if (table->m_uniqueKeysCount <= keyId)
			{
				unuse();
				return nullptr;
			}
		}
		else
		{
			unuse();
			return nullptr;
		}

		int indexPageId = getTableIndexPageId(tableInfo, table, type, keyId);
		if (indexPageId < 0)
		{
			unuse();
			return nullptr;
		}
		Page* p = getPage(indexPageId);
		if (p == nullptr)
		{
			unuse();
			return nullptr;
		}
		return p;
	}

	BlockIndexIterator* SolidBlock::createIndexIterator(uint32_t flag, const META::TableMeta* table, META::KEY_TYPE type, int keyId)
	{
		Page* p = getIndex(table, type, keyId);
		if (p == nullptr)
		{
			unuse();
			return nullptr;
		}
		const solidIndexHead* head = (const solidIndexHead*)(p->pageData);
		if (head->flag & SOLID_INDEX_FLAG_FIXED)
		{
			fixedSolidIndex sidx(p);
			return new SolidBlockIndexIterator<fixedSolidIndex>(flag, this, sidx);
		}
		else
		{
			varSolidIndex vidx(p);
			return new SolidBlockIndexIterator<varSolidIndex>(flag, this, vidx);
		}
	}
	template<class I, class T>
	static inline bool getRecordIdsOfKey(Page* p, const void* key, const uint32_t*& recordIds, uint32_t& count)
	{
		I i(p);
		int32_t offset = i.find(*static_cast<const T*>(key), true);
		if (offset < 0)
			return false;
		i.getRecordIdByIndex(offset, recordIds, count);
		return true;
	}
	char* SolidBlock::getRecord(const META::TableMeta* table, META::KEY_TYPE type, int keyId, const void* key)
	{
		Page* p = getIndex(table, type, keyId);
		if (p == nullptr)
		{
			unuse();
			return nullptr;
		}
		const solidIndexHead* head = (const solidIndexHead*)(p->pageData);
		const uint32_t* recordIds;
		uint32_t rcount;
		bool success;
		switch (static_cast<META::COLUMN_TYPE>(head->type))
		{
		case META::COLUMN_TYPE::T_UNION:
		{
			if (head->flag & SOLID_INDEX_FLAG_FIXED)
				success = getRecordIdsOfKey<fixedSolidIndex, META::unionKey>(p, key, recordIds, rcount);
			else
				success = getRecordIdsOfKey<varSolidIndex, META::unionKey>(p, key, recordIds, rcount);
		}
		break;
		case META::COLUMN_TYPE::T_INT8:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, int8_t>(p, key, recordIds, rcount);
			break;
		}
		case META::COLUMN_TYPE::T_UINT8:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, uint8_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_INT16:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, int16_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_UINT16:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, uint16_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_INT32:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, int32_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_UINT32:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, uint32_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_INT64:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, int64_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_TIMESTAMP:
		case META::COLUMN_TYPE::T_UINT64:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, uint64_t>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_FLOAT:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, float>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_DOUBLE:
		{
			success = getRecordIdsOfKey<fixedSolidIndex, double>(p, key, recordIds, rcount);
			break;
		};
		case META::COLUMN_TYPE::T_BLOB:
		case META::COLUMN_TYPE::T_STRING:
		{
			success = getRecordIdsOfKey<varSolidIndex, META::BinaryType>(p, key, recordIds, rcount);
			break;
		};
		default:
			return nullptr;//todo
		}

		if (!success)
		{
			unuse();
			return nullptr;
		}
		char* r = getRecordByInnerId(recordIds[rcount - 1]);
		unuse();
		return r;
	}
	void SolidBlockIterator::resetBlock(SolidBlock* block)
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
			m_block->unuse();
		m_block = block;
		begin();
	}
	bool SolidBlockTimestampIterator::seekIncrease(const void* key)//timestamp not increase strictly
	{
		uint64_t timestamp = *(uint64_t*)key;
		m_status = Status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxTime < timestamp || m_block->m_minTime > timestamp)
			return false;
		m_recordId = -1;
		m_seekedId = -1;
		while (next() == Status::OK)
		{
			RecordGeneralInfo* recordInfo = &m_block->m_recordInfos[m_recordId];
			if (recordInfo->timestamp >= timestamp)
			{
				m_status = Status::OK;
				return true;
			}
		}
		return false;
	}
	bool SolidBlockTimestampIterator::seekDecrease(const void* key)//timestamp not increase strictly
	{
		uint64_t timestamp = *(uint64_t*)key;
		m_status = Status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxTime < timestamp || m_block->m_minTime > timestamp)
			return false;
		m_recordId = m_block->m_recordCount;
		m_seekedId = m_recordId;
		while (next() == Status::OK)
		{
			RecordGeneralInfo* recordInfo = &m_block->m_recordInfos[m_recordId];
			if (recordInfo->timestamp <= timestamp)
			{
				m_status = Status::OK;
				return true;
			}
		}
		return false;
	}
	bool SolidBlockCheckpointIterator::seek(const void* key)
	{
		uint64_t logOffset = *(uint64_t*)key;
		m_status = Status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxLogOffset < logOffset || m_block->m_minLogOffset > logOffset)
			return false;
		int s = 0, e = m_block->m_recordCount - 1, m;
		while (s <= e)
		{
			m = (s + e) >> 1;
			uint64_t offset = m_block->m_recordInfos[m].logOffset;
			if (logOffset > offset)
				s = m + 1;
			else if (logOffset < offset)
				e = m - 1;
			else
				goto FIND;
		}
		if (e < 0)
			m = 0;
		if (s > (int)m_block->m_recordCount - 1)
			m = m_block->m_recordCount - 1;
		if (increase())
		{
			if (m_block->m_recordInfos[m].logOffset < logOffset)
			{
				if (m == (int)m_block->m_recordCount - 1)
					return false;
				m++;
			}
		}
		else
		{
			if (m_block->m_recordInfos[m].logOffset > logOffset)
			{
				if (m == 0)
					return false;
				m--;
			}
		}
	FIND:
		m_recordId = m_seekedId = m;
		m_realRecordId = m_recordId + m_block->m_minRecordId;
		if (filterCurrentSeekRecord() != 0)
		{
			if (m_status == Status::INVALID)
				return false;
			if (next() != Status::OK)
			{
				m_recordId = 0xfffffffeu;
				return false;
			}
		}
		m_status = Status::OK;
		return true;
	}

	bool solidBlockRecordIdIterator::seek(const void* key)
	{
		uint64_t recordId = *(uint64_t*)key;
		m_status = Status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_minRecordId > recordId || m_block->m_minRecordId + m_block->m_recordCount < recordId)
			return false;
		m_recordId = recordId - m_block->m_minRecordId;
		m_seekedId = m_recordId;
		m_realRecordId = recordId;
		if (filterCurrentSeekRecord() != 0)
		{
			if (m_status == Status::INVALID)
				return false;
			if (next() != Status::OK)
			{
				m_recordId = 0xfffffffeu;
				return false;
			}
		}
		m_status = Status::OK;
		return true;
	}
	int SolidBlockIterator::filterCurrentSeekRecord()
	{
		if (m_filter)
		{
			const char* tmpRecord = nullptr;
			RecordGeneralInfo* recordInfo = &m_block->m_recordInfos[m_seekedId];
			if (!m_filter->filterByGeneralInfo(m_block->m_tableInfo[recordInfo->tableIndex].tableId, recordInfo->recordType, recordInfo->logOffset, recordInfo->timestamp))
				return 1;
			if (!m_filter->onlyNeedGeneralInfo())
			{
				if ((tmpRecord = (const char*)value(m_seekedId)) == nullptr)
				{
					m_status = Status::INVALID;
					LOG(ERROR) << "read next record from block " << m_block->m_blockID << " failed,record id:" << m_seekedId;
					return -1;
				}
				if (!m_filter->filterByRecord(tmpRecord))
				{
					tmpRecord = nullptr;
					return 1;
				}
			}
			return 0;
		}
		else
			return 0;
	}

	Iterator::Status SolidBlockIterator::next()
	{
		for (;;)
		{
			if (likely(increase()))
			{
				if (++m_seekedId >= m_block->m_recordCount)
					return m_status = Iterator::Status::ENDED;
			}
			else
			{
				if (m_seekedId == 0)
					return m_status = Iterator::Status::ENDED;
				m_seekedId--;
			}
			if (filterCurrentSeekRecord() != 0)
			{
				if (unlikely(m_status == Status::INVALID))
					return Status::INVALID;
				else
					continue;
			}
			m_recordId = m_seekedId;
			m_realRecordId = m_recordId + m_block->m_minRecordId;
			return Status::OK;
		}
		return m_status = Status::ENDED;
	}
}
