#include "solidBlock.h"
namespace DATABASE
{
	int solidBlock::load(int id)
	{
		if (!fileHandleValid(m_fd))
		{
			char fileName[512];
			m_database->genBlockFileName(m_blockID, fileName);
			m_fd = openFile(fileName, true, false, false);
			if (!fileHandleValid(m_fd))
			{
				m_loading.store(BLOCK_UNLOAD, std::memory_order_relaxed);
				LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
				return -1;
			}
		}
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
	int solidBlock::loadFirstPage()
	{
		uint8_t loadStatus = m_loading.load(std::memory_order_relaxed);
		do {
			if (loadStatus == BLOCK_LOADED_HEAD)
			{
				if (m_loading.compare_exchange_weak(loadStatus, BLOCK_LOADING_FIRST_PAGE, std::memory_order_relaxed,
					std::memory_order_relaxed))
					break;
			}
			else if (loadStatus == BLOCK_LOADING_FIRST_PAGE)
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				loadStatus = m_loading.load(std::memory_order_relaxed);
			}
			else if (loadStatus < BLOCK_LOADED_HEAD)
			{
				LOG(ERROR) << "loadFirstPage of block file failed for it`s status is less then BLOCK_LOADED_HEAD";
				return -1;
			}
			else if (loadStatus == BLOCK_LOADED)
				return 0;
			else
				return -1;
		} while (1);

		firstPage = (page*)m_database->allocMem(sizeof(page));
		firstPage->pageSize = firstPage->pageUsedSize = m_solidBlockHeadPageRawSize;
		firstPage->crc = 0;
		if (0 != loadPage(firstPage, m_solidBlockHeadPageSize, offsetof(solidBlock, m_solidBlockHeadPageSize) - offsetof(solidBlock, m_version)))
		{
			m_loading.store(BLOCK_LOAD_FAILED, std::memory_order_relaxed);
			return -1;
		}
		char* pos = firstPage->pageData;
		m_tableInfo = (tableDataInfo*)pos;

		pos += sizeof(tableDataInfo) * m_tableCount;
		goto FIRST_PAGE_SIZE_FAULT;
		m_recordInfos = (recordGeneralInfo*)pos;
		pos += sizeof(recordGeneralInfo) * m_recordCount;
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		m_recordIdOrderyTable = (uint32_t*)pos;
		pos += sizeof(uint32_t) * m_recordCount;
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pageOffsets = (uint64_t*)pos;
		pos += sizeof(uint64_t) * (m_pageCount + 1);
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pages = (page**)m_database->allocMem(sizeof(page*) * m_pageCount);
		if (pos + m_pageCount * offsetof(page, _ref) >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;
		for (uint16_t idx = 0; idx < m_pageCount; idx++)
		{
			pages[idx] = (page*)m_database->allocMem(sizeof(page));
			memcpy(pages[idx], pos, offsetof(page, _ref));
			pos += offsetof(page, _ref);
		}
		m_flag |= BLOCK_FLAG_FLUSHED;
		m_loading.store(BLOCK_LOADED, std::memory_order_relaxed);
		return 0;

	FIRST_PAGE_SIZE_FAULT:
		m_loading.store(BLOCK_LOAD_FAILED, std::memory_order_relaxed);
		LOG(ERROR) << "load block file:" << m_blockID << " failed for first page is illegal";
		m_database->freePage(firstPage);
		firstPage = nullptr;
		return -1;
	}
	int solidBlock::loadPage(page* p, size_t size, size_t offset)
	{
		if (p->pageData != nullptr)
			return 0;
		m_fileLock.lock();
		if (!fileHandleValid(m_fd))
		{
			char fileName[512];
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
					LOG(ERROR) << "deCompress page data in position:" << offset << " of block " << m_blockID << " failed ";
					bufferPool::free(uncompressedData);
					bufferPool::free(data);
					return -1;
				}
			}
			p->pageData = uncompressedData;
		}
		return 0;
	}
	char* solidBlock::getCompressbuffer(uint32_t size)
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
	int64_t solidBlock::writepage(page* p, uint64_t offset, bool compress)
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
			if (compressedSize != writeFile(m_fd, compressBuf, compressedSize))
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
				return -1;
			}
			return compressedSize;
		}
		else
		{
			p->crc = hwCrc32c(0, p->pageData, p->pageUsedSize);
			if (p->pageUsedSize != (uint32_t)writeFile(m_fd, p->pageData, p->pageUsedSize))
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << "," << strerror(errno);
				return -1;
			}
			return p->pageUsedSize;
		}
	}
	int solidBlock::writeToFile()
	{
		if (m_flag & BLOCK_FLAG_FLUSHED)
			return 0;
		if (fileHandleValid(m_fd))
			closeFile(m_fd);
		char fileName[512], tmpFile[512];
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
		int headSize = offsetof(solidBlock, m_fd) - offsetof(solidBlock, m_version);

		uint64_t pagePos = ALIGN(headSize + LZ4_COMPRESSBOUND(firstPage->pageUsedSize), 512);
		for (uint16_t pageIdx = 0; pageIdx < m_pageCount; pageIdx++)
		{
			pageOffsets[pageIdx] = pagePos;
			int64_t writedSize = writepage(pages[pageIdx], pagePos, m_flag & BLOCK_FLAG_COMPRESS);
			if (writedSize < 0)
				return -1;
			pagePos = ALIGN(pagePos + writedSize, 512);
		}
		m_solidBlockHeadPageSize = writepage(firstPage, headSize, m_flag & BLOCK_FLAG_COMPRESS);
		m_solidBlockHeadPageRawSize = firstPage->pageUsedSize;
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
	int solidBlock::loadFromFile()
	{
		char fileName[512];
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
	void solidBlockIterator::resetBlock(solidBlock* block, uint32_t id)
	{
		if (m_block != nullptr)
			m_block->unuse();
		m_recordId = id - 1;
		if (next() != status::OK)
		{
			m_status = status::INVALID;
			return;
		}
		else
			m_status = status::OK;
	}
	/*
	find record by timestamp
	[IN]timestamp ,start time by micro second
	[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
	[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
	*/
	bool solidBlockIterator::seekByTimestamp(uint64_t timestamp, uint32_t interval, bool equalOrAfter)//timestamp not increase strictly
	{
		m_status = status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxTime > timestamp || m_block->m_minTime < timestamp)
			return false;
		m_recordId = -1;
		while (next() == status::OK)
		{
			recordGeneralInfo* recordInfo = &m_block->m_recordInfos[m_recordId];
			if (recordInfo->timestamp >= timestamp)
			{
				if (!equalOrAfter)
				{
					m_status = status::OK;
					return true;
				}
				else if (recordInfo->timestamp <= timestamp + interval)
				{
					m_status = status::OK;
					return true;
				}
				else
				{
					m_recordId = 0xfffffffeu;
					return false;
				}
			}
		}
		if (m_status != status::INVALID)
			m_recordId = 0xfffffffeu;
		return false;
	}
	bool solidBlockIterator::seekByLogOffset(uint64_t logOffset, bool equalOrAfter)
	{
		m_status = status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_maxLogOffset > logOffset || m_block->m_minLogOffset < logOffset)
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
		if (equalOrAfter)
			return false;
		if (e < 0)
			m = 0;
	FIND:
		m_recordId = m - 1;
		if (next() != status::OK)
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		m_status = status::OK;
		return true;
	}
	bool solidBlockIterator::seekByRecordId(uint64_t recordId)
	{
		m_status = status::UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_minRecordId > recordId || m_block->m_minRecordId + m_block->m_recordCount < recordId)
			return false;
		m_recordId = recordId - m_block->m_minRecordId - 1;
		if (next() != status::OK)
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		m_status = status::OK;
		return true;
	}
	iterator::status solidBlockIterator::next()
	{
		uint32_t _recordId = m_recordId;
		while (++_recordId < m_block->m_recordCount)
		{
			const char* tmpRecord = nullptr;
			if (m_filter)
			{
				recordGeneralInfo* recordInfo = &m_block->m_recordInfos[_recordId];
				if (!m_filter->filterByGeneralInfo(m_block->m_tableInfo[recordInfo->tableIndex].tableId, recordInfo->recordType, recordInfo->logOffset, recordInfo->timestamp))
					continue;
				if (!m_filter->onlyNeedGeneralInfo())
				{
					if ((tmpRecord = m_block->getRecord(_recordId)) == nullptr)
					{
						m_status = status::INVALID;
						LOG(ERROR) << "read next record from block " << m_block->m_blockID << " failed,record id:" << _recordId;
						return status::INVALID;
					}
					if (!m_filter->filterByRecord(tmpRecord))
					{
						tmpRecord = nullptr;
						continue;
					}
				}
			}
			m_recordId = _recordId;
			return status::OK;
		}
		m_status = status::ENDED;
		return status::ENDED;
	}
}
