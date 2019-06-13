#pragma once
#include <stdint.h>
#include "iterator.h"
#include "filter.h"
#include "job.h"
#include "blockManager.h"
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
		blockManager * m_blocks;
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
	template<typename T>
	iterator * find(uint64_t startRecordID,META::tableMeta * meta,uint16_t columnIndex,const T * v)//todo
	{
		if (!m_flag&S_CACHED)
			return nullptr;
		blockManagerIterator *iter = new blockManagerIterator(m_flag,nullptr,m_blocks);//todo
		return iter;
	}

};
}
