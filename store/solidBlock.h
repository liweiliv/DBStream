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
#include "block.h"
#define OS_WIN //todo
#include "../util/file.h"
#include "../util/crcBySSE.h"
#include <glog/logging.h>
#include "../lz4/lz4.h"
#include "../meta/metaData.h"
#include "../meta/metaDataCollection.h"
#include "../util/barrier.h"
namespace STORE
{
#pragma pack(1)
class solidBlock :public block{
	struct dataPartHead {
		uint64_t dataPartSize;
		union{
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
		uint32_t recordDataOffset;
		uint32_t recordDataSize;
		uint32_t indexsOffset;
	};
	struct recordOffsetInfo {
		uint16_t tableIndex;
		uint16_t pageId;
		uint32_t offsetInPage;
	};
	struct recordGeneralInfo
	{
		uint8_t recordType;
		recordOffsetInfo offset;
		uint64_t timestamp;
		uint64_t logOffset;
	};
	struct tableFullData {
		uint32_t tablePageCount;
		uint32_t * tablePageOffsets;
		const char** tablePages;
		META::tableMeta *meta;
	};
	uint32_t m_tableCount;
	uint32_t m_recordCount;
	fileHandle m_fd;
	tableDataInfo * m_tableInfo;
	recordGeneralInfo * m_recordInfos;
	tableFullData * m_tables;
	uint32_t * m_pageInfo;
	std::mutex m_fileLock;
	META::metaDataCollection *m_metaDataCollection;
	int load(const char * fileName)
	{
		errno = 0;
		m_fd = openFile(fileName, true, false, false);
		if (m_fd < 0)
		{
			LOG(ERROR) << "load block file:" << fileName << " failed for open file failed, errno:" << errno << " ," << strerror(errno);
			return -1;
		}
		int readSize = 0;
		int headSize = offsetof(solidBlock, m_fd) - offsetof(solidBlock, m_version);
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
		if (nullptr == (m_tableInfo = (tableDataInfo*)loadDataPart()))
		{
			LOG(ERROR) << "load tableInfo from block file:" << fileName << " failed";
			closeFile(m_fd);
			m_fd = 0;
			return -1;
		}
		if (nullptr == (m_recordInfos = (recordGeneralInfo*)loadDataPart()))
		{
			LOG(ERROR) << "load recordGeneralInfo from block file:" << fileName << " failed";
			free(m_tableInfo);
			closeFile(m_fd);
			m_fd = 0;
			return -1;
		}
		if (nullptr == (m_pageInfo = (uint32_t*)loadDataPart()))
		{
			LOG(ERROR) << "load pageInfo from block file:" << fileName << " failed";
			free(m_tableInfo);
			free(m_recordInfos);
			closeFile(m_fd);
			m_fd = 0;
			return -1;
		}
		m_tables = (tableFullData*)malloc(sizeof(tableFullData)*m_tableCount);
		int pageInfoPos = 0;
		for (int i = 0; i < m_tableCount; i++)
		{
			m_tables[i].meta = nullptr;
			m_tables[i].tablePageCount = *(uint32_t*)m_pageInfo[pageInfoPos];
			m_tables[i].tablePageOffsets = &m_pageInfo[pageInfoPos + 1];
			pageInfoPos += m_tables[i].tablePageCount + 1;
			m_tables[i].tablePages = nullptr;
		}
		return 0;
	}
	int loadPage(uint32_t tableIdx, uint32_t pageId)
	{
		tableFullData * table = &m_tables[tableIdx];
		if (table->tablePages == nullptr)
		{
			META::tableMeta * meta = m_metaDataCollection->get(m_tableInfo[tableIdx].tableId);
			if (meta = nullptr)
			{
				LOG(ERROR) << "can not find table id " << m_tableInfo[tableIdx].tableId << "in metaDataCollection";
				return -1;
			}
			const char ** tmp = (const char**)malloc(sizeof(const char*)*table->tablePageCount);
			memset(tmp, 0, sizeof(const char*)*table->tablePageCount);
			m_fileLock.lock();
			if (table->tablePages == nullptr)
			{
				table->meta = meta;
				barrier;
				table->tablePages = tmp;
			}
			else
				free(tmp);
		}
		else
		{
			if (table->tablePages[pageId] != nullptr)
				return 0;
			m_fileLock.lock();
		}

		if (table->tablePages[pageId] != nullptr)
		{
			m_fileLock.unlock();
			return 0;
		}
		const char * page = loadDataPart(table->tablePageOffsets[pageId]);
		if (page == nullptr)
		{
			m_fileLock.unlock();
			LOG(ERROR) << "load page from block failed,table:" << table->meta->m_dbName<<"."<<table->meta->m_tableName;
			return -1;
		}
		table->tablePages[pageId] = page;
		m_fileLock.unlock();
		return 0;
	}
	const char * getRecord(uint32_t recordId)
	{
		if (recordId >= m_maxDataIdx - m_minDataIdx)
			return nullptr;
		uint16_t tableIndex = m_recordInfos[recordId].offset.tableIndex;
		tableFullData * table = &m_tables[tableIndex];
		if (0 != loadPage(tableIndex, m_recordInfos[recordId].offset.pageId))
			return nullptr;
		return table->tablePages[m_recordInfos[recordId].offset.pageId] + m_recordInfos[recordId].offset.offsetInPage;
	}
	char* loadDataPart(uint64_t offset = 0)
	{
		dataPartHead p;
		int readSize = 0;
		errno = 0;
		if (offset > 0)
		{
			if (offset != seekFile(m_fd, offset, SEEK_SET))
			{
				LOG(ERROR) << "seek pos " + offset << " failed for " << errno << " ," << strerror(errno);
				return nullptr;
			}
		}
		if (sizeof(p) != (readSize = readFile(m_fd, (char*)&p, sizeof(p))))
		{
			LOG(ERROR) << "load block file failed for read dataPartHead failed ";
			if (errno != 0)
				LOG(ERROR) << "errno:" << errno << " ," << strerror(errno);
			else
				LOG(ERROR) << "expect read " << sizeof(p) << " byte ,but only can read " << readSize << " byte";
			return nullptr;
		}
		char * dataPart = (char*)malloc(p.dataPartSize);
		if (p.dataPartSize != (readSize = readFile(m_fd, dataPart, p.dataPartSize)))
		{
			LOG(ERROR) << "load block file failed for read data failed ";
			if (errno != 0)
				LOG(ERROR) << "errno:" << errno << " ," << strerror(errno);
			else
				LOG(ERROR) << "expect read " << p.dataPartSize << " byte ,but only can read " << readSize << " byte";
			free(dataPart);
			return nullptr;
		}
		if (p.rawSizeBeforeCompress&0x8000000000000000ul == 0)
		{
			if (p.rawSizeBeforeCompress < (MAXINT32))
			{
				char * raw = (char*)malloc(p.rawSizeBeforeCompress);
				if (p.rawSizeBeforeCompress != LZ4_decompress_safe(dataPart, raw, p.dataPartSize, p.rawSizeBeforeCompress))
				{
					LOG(ERROR) << "load block file failed decompress failed ";
					free(raw);
					free(dataPart);
					return nullptr;
				}
				free(dataPart);
				return  raw;
			}
			else
			{
				//todo
				abort();
			}
		}
		else
		{
			if (hwCrc32c(0, dataPart, p.dataPartSize) != p.crc)
			{
				LOG(ERROR) << "load block file failed for crc check failed ";
				free(dataPart);
				return nullptr;
			}
			return  dataPart;
		}
	}
};
class solidBlockIterator :public iterator{
	uint32_t recordId;
	solidBlock * block;
	solidBlockIterator(solidBlock * block) :recordId(0xfffffffeu),block(block) {}
	bool valid()
	{
		return block != nullptr&&recordId < 0xfffffffeu;
	}
	bool next()
	{
		uint32_t _recordId = recordId;
		while (++_recordId < block->m_minDataIdx - block->m_minDataIdx)
		{
			if (m_filter)
			{

			}
				return false;
		}

		recordId++;

	}
};
}

#endif
