#pragma once
#include <stdint.h>
#include <atomic>
#include <string>
#include "message/record.h"
#include "block.h"
#include "solidBlock.h"
#include "globalInfo/global.h"
#include <glog/logging.h>
namespace DATABASE
{
	struct statistic
	{
		std::atomic<uint32_t> blockCount;
		std::atomic<uint32_t> startBlockId;
		std::atomic<uint32_t> endBlockId;
		uint64_t startCheckpoint;
		uint64_t endCheckPoint;
		uint64_t startTimestamp;  //earlieast time in database 
		uint64_t endTimestamp; //last time in database 
		uint64_t recordCount;  
		uint64_t diskUsedSize;
		uint64_t rawDataSize;

		uint64_t rps;
		uint64_t tps;
		uint64_t iops;

		uint64_t rpsNow;
		uint64_t tpsNow;
		uint64_t iopsNow;

		uint64_t prevTid;
		Timer::Timestamp now;
		std::string name;
		statistic(const char * name) :startCheckpoint(0), endCheckPoint(0), startTimestamp(0), endTimestamp(0), recordCount(0), diskUsedSize(0), rawDataSize(0),
			rps(0), tps(0), iops(0), rpsNow(0), tpsNow(0), iopsNow(0), prevTid(0),name(name)
		{
			now.time = GLOBAL::currentTime.time;
		}
		inline void newRecord(const RPC::Record* record)
		{
			if (unlikely(GLOBAL::currentTime.seconds > now.seconds)) {
				now.time = GLOBAL::currentTime.time;
				rps = rpsNow;
				tps = tpsNow;
				iops = iopsNow;
				rpsNow = 0;
				tpsNow = 0;
				iopsNow = 0;
			}
			recordCount++;
			rpsNow ++;
			iopsNow += record->head->minHead.size;
			if (prevTid != record->head->checkpoint.txnId)
			{
				tpsNow++;
				prevTid = record->head->checkpoint.txnId;
			}
			if (unlikely(startCheckpoint == 0))
			{
				startCheckpoint = record->head->checkpoint.logOffset;
				startTimestamp = record->head->checkpoint.timestamp;
			}

			if (endTimestamp < record->head->checkpoint.timestamp)
				endTimestamp = record->head->checkpoint.timestamp;
			endCheckPoint = record->head->checkpoint.logOffset;
		}
		inline void newBlock(const Block* b)
		{
			blockCount++;
		}
		inline void newSolidBlockFile(const Block* b)
		{
			diskUsedSize += b->m_fileSize;
		}
		inline void loadFile(const Block* b)
		{
			blockCount++;
			diskUsedSize += b->m_fileSize;
			rawDataSize += b->m_rawSize;
			recordCount += b->m_recordCount;
		}
		inline void purgeSolidBlock(const Block* b)
		{
			blockCount--;
			diskUsedSize -= b->m_fileSize;
			rawDataSize -= b->m_rawSize;
			recordCount -= b->m_recordCount;
		}
		void logStatistic() const
		{
			LOG(INFO) << name << " statistic info:" << "rps:" << rps << ",tps:" << tps << ",iops:" << iops << ",rawDataSize:" << rawDataSize << ",diskUsedSize:" << diskUsedSize << ",blockCount:" << blockCount << ",recordCount:" << recordCount;
		}
	};
}
