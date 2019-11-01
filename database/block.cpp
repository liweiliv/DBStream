#include "block.h"
#include "blockManager.h"
#include "solidBlock.h"
namespace DATABASE {
	int block::loadBlockInfo(fileHandle h, uint32_t id)
	{
		uint8_t loadStatus = m_loading.load(std::memory_order_relaxed);
		do {
			if (loadStatus == BLOCK_UNLOAD)
			{
				if (m_loading.compare_exchange_weak(loadStatus, BLOCK_LOADING_HEAD, std::memory_order_relaxed,
					std::memory_order_relaxed))
					break;
			}
			else if (loadStatus == BLOCK_LOADING_HEAD)
			{
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				loadStatus = m_loading.load(std::memory_order_relaxed);
			}
			else if (loadStatus != BLOCK_LOAD_FAILED)
				return 0;
			else
				return -1;
		} while (1);
		int readSize = 0;
		int headSize = sizeof(m_crc) + offsetof(solidBlock, m_crc) - offsetof(solidBlock, m_version);
		if (headSize != (readSize = readFile(h, (char*)& m_version, headSize)))
		{
			LOG(ERROR) << "load block file " << id << " failed for read head failed ";
			if (errno != 0)
				LOG(ERROR) << "errno:" << errno << " ," << strerror(errno);
			else
				LOG(ERROR) << "expect read " << headSize << " byte ,but only can read " << readSize << " byte";
			return -1;
		}
		if (m_blockID != id)
		{
			LOG(ERROR) << "load block file " << id << " failed for id check failed,id in block is:" << m_blockID;
			return -1;
		}
		uint32_t crc = hwCrc32c(0, (const char*)& m_version, headSize - sizeof(m_crc));
		if (crc != m_crc)
		{
			LOG(ERROR) << "load block file " << id << " failed for crc check failed";
			return -1;
		}
		return 0;
	}
	block* block::loadFromFile(uint32_t id, blockManager* blockManager, META::metaDataBaseCollection* metaDataCollection)
	{
		char fileName[512];
		blockManager->genBlockFileName(id, fileName);
		fileHandle h = openFile(fileName, true, false, false);
		if (!fileHandleValid(h))
		{
			LOG(ERROR) << "open block file:" << fileName << " failed for error:" << errno << "," << strerror(errno);
			return nullptr;
		}
		block* b = new block(blockManager, metaDataCollection,0);
		if (0 != b->loadBlockInfo(h, id))
		{
			closeFile(h);
			delete b;
			return nullptr;
		}
		closeFile(h);
		return b;
	}
}
