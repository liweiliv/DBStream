#pragma once
#include <stdint.h>
#include <string>
#include <glog/logging.h>
#include "util/status.h"
#include "dataSource/localLogFileCache/logEntry.h"
namespace DATA_SOURCE
{
	static constexpr uint8_t LP_OFFSET = 44;
	static constexpr uint64_t MAX_LOG_ID = 1ULL << LP_OFFSET;
	static inline uint64_t createMysqlRecordOffset(uint32_t logId, uint64_t logOffset)//max support 
	{

		assert(logOffset < MAX_LOG_ID);
		return (static_cast<uint64_t>(logId) << LP_OFFSET) | (logOffset);
	}
	static inline uint64_t offsetInFile(uint64_t recordOffset)
	{
		return recordOffset & (MAX_LOG_ID - 1);
	}
	static inline uint32_t fileId(uint64_t recordOffset)
	{
		return recordOffset >> LP_OFFSET;
	}
}

