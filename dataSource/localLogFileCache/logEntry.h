#pragma once
#include "message/record.h"
namespace DATA_SOURCE
{
	struct LogEntry
	{
		uint32_t size;
		char data[1];
		inline RPC::Checkpoint* getCheckpoint()const
		{
			return (RPC::Checkpoint*)&data[0];
		}

		inline const char* getRealData() const
		{
			return &data[0] + getCheckpoint()->checkpointSize();
		}

		inline void copyData(const char* data, uint32_t dataSize)
		{
			memcpy(&this->data[0] + getCheckpoint()->checkpointSize(), data, size);
		}

		inline uint32_t dataSize() const
		{
			return size - getCheckpoint()->checkpointSize();
		}

		bool isHeartbeat() const
		{
			return getCheckpoint()->checkpointSize() == size;
		}

		inline static uint32_t allocSize(uint16_t ckpExternSize, uint32_t dataSize)
		{
			return sizeof(size) + sizeof(RPC::Checkpoint) + ckpExternSize + dataSize - 1;
		}
	};
}