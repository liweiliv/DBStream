/*
 * filter.h
 *
 *  Created on: 2019年1月8日
 *      Author: liwei
 */

#ifndef FILTER_H_
#define FILTER_H_
#include "meta/metaDataCollection.h"
#include "message/record.h"
namespace DATABASE {
#define 	FL_CHECKPOINT		0x01
#define 	FL_TABLE_ID			0x02
#define		FL_CONSTRANT		0x04
#define		FL_COMMON_COLUMN	0x08
#define		FL_RECORD_TYPE		0x10
#define		FL_MULTI_TABLE		0x20
	struct filter {
		uint32_t m_filterType;
		uint64_t m_startCheckpoint;
		uint64_t m_endCheckpoint;
		uint64_t* m_tableIdWhiteList;
		uint32_t m_tableIdWhiteListSize;
		uint64_t* m_tableIdBlackList;
		uint32_t m_tableIdBlackListSize;
		uint8_t m_typeBitmap[(static_cast<uint8_t>(DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE) >> 3) + (static_cast<uint8_t>(DATABASE_INCREASE::RecordType::MAX_RECORD_TYPE) & 0x7) ? 1 : 0];
		filter(uint32_t filterType) :m_filterType(filterType) {

		}
		int init(const char* filters)
		{
			return 0;//todo

		}
		inline bool onlyNeedGeneralInfo()
		{
			return m_filterType & (~(FL_CHECKPOINT | FL_TABLE_ID | FL_RECORD_TYPE));
		}
		inline bool filterByTableID(uint64_t tableID)//todo
		{
			return true;
		}
		inline bool filterByGeneralInfo(uint64_t tableId, uint8_t recordType, uint64_t LogOffset, uint64_t timestamp)
		{
			if (!TEST_BITMAP(m_typeBitmap, recordType))
				return false;
			if ((m_filterType & FL_CHECKPOINT) && (m_startCheckpoint > LogOffset || m_endCheckpoint < LogOffset))
				return false;
			if (m_filterType & FL_TABLE_ID && !filterByTableID(tableId))
				return false;
			return true;
		}
		inline bool filterByRecord(const char* record)
		{
			DATABASE_INCREASE::recordHead* h = (DATABASE_INCREASE::recordHead*)record;
			if (!TEST_BITMAP(m_typeBitmap, h->minHead.type))
				return false;
			if ((m_filterType & FL_CHECKPOINT) && (m_startCheckpoint > h->logOffset || m_endCheckpoint < h->logOffset))
				return false;
			if (m_filterType & FL_TABLE_ID && !filterByTableID(DATABASE_INCREASE::DMLRecord::tableId(record)))
				return false;
			return true;
		}
	};
}
#endif /* FILTER_H_ */
