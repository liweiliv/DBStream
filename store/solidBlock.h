/*
 * solidBlock.h
 *
 *  Created on: 2019年1月7日
 *      Author: liwei
 */
#ifndef SOLIDBLOCK_H_
#define SOLIDBLOCK_H_
#include <mutex>
#include <stdint.h>
#include "iterator.h"
#include "../util/file.h"
#include "../util/crcBySSE.h"
#include "../glog/logging.h"
#include "../lz4/lz4.h"
#include "../meta/metaData.h"
#include "../meta/metaDataCollection.h"
#include "../util/barrier.h"
#include "blockManager.h"
#include "block.h"
#ifndef ALIGN
#define ALIGN(x, a)   (((x)+(a)-1)&~(a - 1))
#endif
namespace STORE
{
class solidBlockIterator;
class appendingBlock;
#pragma pack(1)
class solidBlock :public STORE::block{
	uint32_t solidBlockHeadPageRawSize;
	uint32_t solidBlockHeadPageSize;
	fileHandle m_fd;
	tableDataInfo * m_tableInfo;
	recordGeneralInfo * m_recordInfos;
	uint32_t *m_recordIdOrderyTable;
	tableFullData * m_tables;
	uint64_t *pageOffsets;
	page ** pages;
	page *firstPage;
	std::mutex m_fileLock;
	META::metaDataCollection *m_metaDataCollection;
	friend class appendingBlock;
	friend class solidBlockIterator;
public:
	solidBlock( blockManager *blockManager, META::metaDataCollection *metaDataCollection):block(blockManager, metaDataCollection), solidBlockHeadPageRawSize(0), solidBlockHeadPageSize(0), m_fd(0), m_tableInfo(nullptr), m_recordInfos(nullptr),
	m_recordIdOrderyTable(nullptr),m_tables(nullptr), pageOffsets(nullptr),pages(nullptr), firstPage(nullptr)
	{
	}
	~solidBlock()
	{
		assert(m_flag&BLOCK_FLAG_FLUSHED);
		for (uint32_t i = 0; i < m_pageCount; i++)
		{
			if (pages[i] != nullptr)
				m_blockManager->freePage(pages[i]);
		}
		m_blockManager->freePage(firstPage);
		if (fileHandleValid(m_fd))
			closeFile(m_fd);
	}
	int load(uint64_t id)
	{
		errno = 0;

		std::string fileName = m_blockManager->getBlockFile(id);
		m_fd = openFile(fileName.c_str(), true, false, false);
		if (!fileHandleValid(m_fd))
		{
			LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
			return -1;
		}

		int readSize = 0;
		int headSize = offsetof(solidBlock, solidBlockHeadPageSize) - offsetof(solidBlock, m_version);
		if (headSize != (readSize=readFile(m_fd, (char*)&m_version, headSize)))
		{
			LOG(ERROR) << "load block file:" << fileName << " failed for read head failed ";
			if (errno != 0)
				LOG(ERROR) << "errno:" << errno << " ," << strerror(errno);
			else
				LOG(ERROR) << "expect read " << headSize << " byte ,but only can read " << readSize << " byte";
			closeFile(m_fd);
			m_fd = 0;
			return -1;
		}
		if (m_blockID != id)
		{
			LOG(ERROR) << "load block file:" << fileName << " failed for block id is not match,expect is "<<id<<" but actually is "<<m_blockID;
			return -1;
		}
		firstPage = (page*)m_blockManager->allocMem(sizeof(page));
		firstPage->pageSize = firstPage->pageUsedSize = solidBlockHeadPageRawSize;
		firstPage->crc = 0;
		if (0 != loadPage(firstPage, solidBlockHeadPageSize, headSize))
			return -1;
		char * pos = firstPage->pageData;
		m_tableInfo = (tableDataInfo*)pos;

		pos += sizeof(tableDataInfo)*m_tableCount;
		goto FIRST_PAGE_SIZE_FAULT;
		m_recordInfos = (recordGeneralInfo*)pos;
		pos += sizeof(recordGeneralInfo)*m_recordCount;
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		m_recordIdOrderyTable = (uint32_t *)pos;
		pos+=sizeof(uint32_t)*m_recordCount;
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pageOffsets = (uint64_t*)pos;
		pos += sizeof(uint64_t)*(m_pageCount+1);
		if (pos >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;

		pages = (page**)m_blockManager->allocMem(sizeof(page*)*m_pageCount);
		if (pos+ m_pageCount*offsetof(page, ref) >= firstPage->pageData + firstPage->pageUsedSize)
			goto FIRST_PAGE_SIZE_FAULT;
		for (uint16_t idx = 0; idx < m_pageCount; idx++)
		{
			pages[idx] = (page*)m_blockManager->allocMem(sizeof(page));
			memcpy(pages[idx], pos, offsetof(page, ref));
			pos += offsetof(page, ref);
		}
		m_flag |= BLOCK_FLAG_FLUSHED;
		return 0;
FIRST_PAGE_SIZE_FAULT:
		LOG(ERROR) << "load block file:" << fileName << " failed for first page is illegal";
		m_blockManager->freePage(firstPage);
		firstPage = nullptr;
		return -1;
	}
	int loadPage(page *p,size_t size,size_t offset)
	{
		if (p->pageData!=nullptr)
			return 0;
		m_fileLock.lock();
		if (!fileHandleValid(m_fd))
		{
			std::string fileName = m_blockManager->getBlockFile(m_blockID);
			m_fd = openFile(fileName.c_str(), true, false, false);
			if (!fileHandleValid(m_fd))
			{
				LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
				return -1;
			}
		}
		if (offset != seekFile(m_fd, offset, SEEK_SET))
		{
			m_fileLock.unlock();
			LOG(ERROR) << "seek to position:" << offset << " of block " << m_blockID << " failed for" << errno << "," << strerror(errno);
			return -1;
		}
		char * data = (char*)m_blockManager->allocMem(size);
		if (size != readFile(m_fd, data, size))
		{
			m_fileLock.unlock();
			LOG(ERROR) << "read page in position:" << offset << " of block " << m_blockID << " failed for" << errno << "," << strerror(errno);
			return -1;
		}
		m_fileLock.unlock();
		if (p->crc!=0)//not compressed data
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
			char * uncompressedData = (char*)m_blockManager->allocMem(p->pageUsedSize);
			if (p->pageUsedSize > LZ4_MAX_INPUT_SIZE)
			{
				uint32_t * sizeArray = (uint32_t*)data;
				uint32_t arraySize = p->pageUsedSize / LZ4_MAX_INPUT_SIZE + ((p->pageUsedSize%LZ4_MAX_INPUT_SIZE) ? 1 : 0);
				char * realData = data + arraySize*sizeof(uint32_t);
				uint64_t tmpSize = 0;
				for (uint32_t idx = 0; idx < arraySize; idx++)
				{
					int32_t decompressSize;
					if ((decompressSize=LZ4_decompress_safe(realData, uncompressedData+ tmpSize, sizeArray[idx], p->pageUsedSize-tmpSize)) < 0)
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
	int64_t writepage(page* p,uint64_t offset,bool compress)
	{
		if(seekFile(m_fd,0,SEEK_CUR)!=offset)
		{
			if(seekFile(m_fd,offset,SEEK_SET)!=offset)
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << ","<<strerror(errno);
				return -1;
			}
		}
		if(compress)
		{
			p->crc = 0;
			int64_t compressedSize = 0;
			char * compressBuf = nullptr;
			if(unlikely(p->pageUsedSize>LZ4_MAX_INPUT_SIZE))
			{
				uint32_t fullCount = p->pageUsedSize/LZ4_MAX_INPUT_SIZE;
				uint32_t notFullTailSize = p->pageUsedSize%LZ4_MAX_INPUT_SIZE;
				uint64_t compressBufSize = sizeof(uint32_t)*(fullCount+1+notFullTailSize?1:0)+fullCount*LZ4_COMPRESSBOUND(LZ4_MAX_INPUT_SIZE)+ notFullTailSize?LZ4_COMPRESSBOUND(p->pageUsedSize%LZ4_MAX_INPUT_SIZE):0;
				compressBuf = (char*)m_blockManager->allocMem(compressBufSize);
				char * compressPos = compressBuf+sizeof(uint32_t)*(fullCount+1+notFullTailSize?1:0),*dataPos = p->pageData;
				uint32_t idx ;
				for(idx=0;idx<fullCount;idx++)
				{
					((uint32_t*)compressBuf)[idx]=compressPos - compressBuf;
					compressPos+=LZ4_compress_default(dataPos,compressPos,LZ4_MAX_INPUT_SIZE,LZ4_COMPRESSBOUND(LZ4_MAX_INPUT_SIZE));
					dataPos +=LZ4_MAX_INPUT_SIZE;
				}
				if(notFullTailSize)
				{
					((uint32_t*)compressBuf)[idx++]=compressPos - compressBuf;
					compressPos+=LZ4_compress_default(dataPos,compressPos,notFullTailSize,LZ4_COMPRESSBOUND(notFullTailSize));
				}
				((uint32_t*)compressBuf)[idx]=compressPos - compressBuf;
				compressedSize = compressPos - compressBuf;
			}
			else
			{
				uint32_t compressBufSize = LZ4_COMPRESSBOUND(p->pageUsedSize);
				compressBuf = (char*)m_blockManager->allocMem(compressBufSize);
				compressedSize = LZ4_compress_default(p->pageData,compressBuf,p->pageUsedSize,compressBufSize);
			}
			if(compressedSize!=writeFile(m_fd,compressBuf,compressedSize))
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << ","<<strerror(errno);
				bufferPool::free(compressBuf);
				return -1;
			}
			bufferPool::free(compressBuf);
			return compressedSize;

		}
		else
		{
			p->crc = hwCrc32c(0, p->pageData, p->pageUsedSize);
			if(p->pageUsedSize!=writeFile(m_fd,p->pageData,p->pageUsedSize))
			{
				LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << ","<<strerror(errno);
				return -1;
			}
			return p->pageUsedSize;
		}
	}
	inline const char * getRecord(uint32_t recordId)
	{
		if (recordId >= m_recordCount)
			return nullptr;
		uint16_t pId = pageId( m_recordInfos[recordId].offset);
		page *p = pages[pId];
		if (unlikely(0 != loadPage(p,ALIGN(pageOffsets[pId],512),pageOffsets[pId+1]-ALIGN(pageOffsets[pId],512))))
			return nullptr;
		return p->pageData + offsetInPage(m_recordInfos[recordId].offset);
	}

	int writeToFile()
	{
		if (m_flag&BLOCK_FLAG_FLUSHED)
			return 0;
		if (fileHandleValid(m_fd))
			closeFile(m_fd);
		std::string file = m_blockManager->getBlockFile(m_blockID);
		std::string tmpFile = file.append(".tmp");
		remove(tmpFile.c_str());
		m_fd = openFile(tmpFile.c_str(), true, true, true);
		if (!fileHandleValid(m_fd))
		{
			LOG(ERROR) << "write block file "<< tmpFile<< " failed for errno:"<<errno<<" ,"<<strerror(errno);
			return -1;
		}
		int writeSize = 0;
		int headSize = offsetof(solidBlock, m_fd) - offsetof(solidBlock, m_version);

		uint64_t pagePos = ALIGN(headSize+LZ4_COMPRESSBOUND(firstPage->pageUsedSize),512);
		for(uint16_t pageIdx =0;pageIdx<m_pageCount;pageIdx++)
		{
			pageOffsets[pageIdx] = pagePos;
			int64_t writedSize = writepage(pages[pageIdx],pagePos,m_flag&BLOCK_FLAG_COMPRESS);
			if(writedSize<0)
				return -1;
			pagePos = ALIGN(pagePos+writedSize,512);
		}

		solidBlockHeadPageSize = writepage(firstPage,headSize, m_flag&BLOCK_FLAG_COMPRESS);
		solidBlockHeadPageRawSize = firstPage->pageUsedSize;
		if(seekFile(m_fd,0,SEEK_SET)!=0)
		{
			LOG(ERROR) << "write page to block " << m_blockID << " failed ,error:" << errno << ","<<strerror(errno);
			return -1;
		}
		if (headSize != (writeSize = writeFile(m_fd, (char*)&m_version, headSize)))
		{
			LOG(ERROR) << "write head info to block file " << tmpFile << " failed for errno:" << errno << " ," << strerror(errno);
			return -1;
		}

		if(0!=fsync(m_fd))
		{
			LOG(ERROR) << "flush block file "<< tmpFile<< " failed for errno:"<<errno<<" ,"<<strerror(errno);
			return -1;
		}
		closeFile(m_fd);
		m_fd = 0;
		if(0!=rename(tmpFile.c_str(),file.c_str()))
		{
			LOG(ERROR) << "rename tmp block file "<< tmpFile<< " to "<<file<<" failed for errno:"<<errno<<" ,"<<strerror(errno);
			return -1;
		}
		m_flag |= BLOCK_FLAG_FLUSHED;
		return 0;
	}
};
class solidBlockIterator :public iterator{
	uint32_t m_recordId;
	DATABASE_INCREASE::record * m_record;
	solidBlock * m_block;
	solidBlockIterator(solidBlock * block,int flag,filter * filter) :iterator(flag,filter),m_recordId(0xfffffffeu), m_record(nullptr),m_block(block)
	{
		if (m_block != nullptr)
		{
			if (!m_block->use())
				m_block = nullptr;
		}
	}
	~solidBlockIterator()
	{
		if (m_block != nullptr)
		{
			m_block->unuse();
		}
	}
	/*
	find record by timestamp
	[IN]timestamp ,start time by micro second
	[IN]interval, micro second,effect when equalOrAfter is true,find in a range [timestamp,timestamp+interval]
	[IN]equalOrAfter,if true ,find in a range [timestamp,timestamp+interval],if has no data,return false,if false ,get first data equal or after [timestamp]
	*/
	bool seekByTimestamp(uint64_t timestamp, uint32_t interval,bool equalOrAfter)//timestamp not increase strictly
	{
		m_status = UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr||m_block->m_maxTime> timestamp|| m_block->m_minTime< timestamp)
			return false;
		m_recordId = -1;
		while (next())
		{
			recordGeneralInfo * recordInfo = &m_block->m_recordInfos[m_recordId];
			if (recordInfo->timestamp >= timestamp)
			{
				if (!equalOrAfter)
				{
					m_status = OK;
					return true;
				}
				else if (recordInfo->timestamp <= timestamp + interval)
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
		if (m_block == nullptr|| m_block->m_maxLogOffset> logOffset|| m_block->m_minLogOffset < logOffset)
			return false;
		int s = 0, e = m_block->m_recordCount - 1,m;
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
		if (!next())
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		recordGeneralInfo * recordInfo = &m_block->m_recordInfos[m_recordId];
		if (recordInfo->logOffset != logOffset && equalOrAfter)
			return false;
		m_status = OK;
		return true;
	}
	bool seekByRecordId(uint64_t recordId, bool equalOrAfter)
	{
		m_status = UNINIT;
		m_errInfo.clear();
		if (m_block == nullptr || m_block->m_minRecordId > recordId || m_block->m_minRecordId+m_block->m_recordCount > recordId)
			return false;
		m_recordId = recordId - m_block->m_minRecordId-1;
		if (!next())
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		if (m_recordId != recordId && equalOrAfter)
		{
			m_recordId = 0xfffffffeu;
			return false;
		}
		m_status = OK;
		return true;
	}
	bool valid()
	{
		return m_block != nullptr&&m_recordId < 0xfffffffeu&& m_record!=nullptr;
	}
	bool next()
	{
		uint32_t _recordId = m_recordId;
		while (++_recordId < m_block->m_recordCount)
		{
			DATABASE_INCREASE::record * tmpRecord = nullptr;
			if (m_filter)
			{
				recordGeneralInfo * recordInfo = &m_block->m_recordInfos[_recordId];
				if (!m_filter->filterByGeneralInfo(m_block->m_tableInfo[recordInfo->tableIndex].tableId, recordInfo->recordType, recordInfo->logOffset, recordInfo->timestamp))
					continue;
				if (!m_filter->onlyNeedGeneralInfo())
				{
					const char * _r = m_block->getRecord(_recordId);
					if (_r == nullptr || nullptr == (tmpRecord = DATABASE_INCREASE::createRecord(_r, m_block->m_metaDataCollection)))
					{
						m_status = INVALID;
						LOG(ERROR) << "read next record from block " << m_block->m_blockID << " failed,record id:" << _recordId;
						return false;
					}
					if (!m_filter->filterByRecord(tmpRecord))
					{
						tmpRecord = nullptr;
						continue;
					}
				}
			}
			if (tmpRecord == nullptr)
			{
				const char * _r = m_block->getRecord(_recordId);
				if (_r == nullptr || nullptr == (tmpRecord = DATABASE_INCREASE::createRecord(_r, m_block->m_metaDataCollection)))
				{
					m_status = INVALID;
					LOG(ERROR) << "read next record from block " << m_block->m_blockID << " failed,record id:" << _recordId;
					return false;
				}
			}
			m_record = tmpRecord;
			m_recordId = _recordId;
			return true;
		}
		m_status = ENDED;
		return false;
	}
	inline void* value() const
	{
		return m_record;
	}
	inline bool end()
	{
		return m_status == ENDED;
	}
};
}

#endif
