#pragma once
#include <stdint.h>
#include "database/iterator.h"
#include "database/filter.h"
#include "job.h"
#include "database/database.h"
namespace DATABASE_INCREASE {
	struct  record;
}
namespace META{
	struct tableMeta;
}
namespace STORE {
constexpr auto S_CACHED = 0x01;
constexpr auto S_SAVE = 0x03; //save must have cache,so its not 0x02 ,but 0x02|0x01
class DBStream :public job {
private:
		void * m_data;
		DATABASE_INCREASE::record * m_currentData;
		META::tableMeta * m_meta;
		uint32_t m_flag;
		DATABASE::database * m_blocks;
		void(*m_workFunc)(DBStream * s);
		void(*m_destroy)(DBStream * s);
		void(*m_finish)(DBStream * );
public:
	DBStream() : m_data(nullptr),m_meta(nullptr),m_workFunc(nullptr), m_destroy(nullptr), m_finish(nullptr){}
	~DBStream()
	{
		if (m_destroy)
			m_destroy(this);
	}
	DATABASE::iterator * find(uint64_t startRecordID,const META::tableMeta * meta,META::KEY_TYPE type,uint16_t keyIndex,const void * v)//todo
	{
		if (!(m_flag&S_CACHED))
			return nullptr;
		DATABASE::iterator *iter = m_blocks->createIndexIterator(0,meta,type,keyIndex);
		if(iter!=nullptr)
		{
			if(!iter->seek(v))
			{
				delete iter;
				return nullptr;
			}
		}
		return iter;
	}

};
}
