#pragma once
namespace DATA_SOURCE
{
	static inline uint64_t createMysqlRecordOffset(uint32_t logId, uint64_t logOffset)//max support 
	{
		assert(logOffset < (1UL << 44));
		return (static_cast<uint64_t>(logId) << 44) | (logOffset);
	}
	static inline uint64_t offsetInFile(uint64_t recordOffset)
	{
		return recordOffset & ((1UL << 44) - 1);
	}
	static inline uint32_t fileId(uint64_t recordOffset)
	{
		return recordOffset >> 44;
	}
}

