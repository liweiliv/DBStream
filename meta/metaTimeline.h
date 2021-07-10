#pragma once
#include "util/likely.h"
#include <string>
#include "glog/logging.h"
namespace META {
	template<typename T>
	class MetaTimeline
	{
	private:
		struct MetaInfo
		{
			T* meta;
			uint64_t startPos;
			MetaInfo* prev;
			MetaInfo() :meta(nullptr), startPos(0), prev(nullptr)
			{
			}
		};
		MetaInfo* m_current;
		uint64_t m_id;
		uint16_t m_version;
		std::string m_name;
	public:
		MetaTimeline(uint64_t id, const char* name) :
			m_current(nullptr), m_id(id), m_version(0), m_name(name)
		{
		}
		~MetaTimeline()
		{
			purge(0xffffffffffffffffULL);
		}
		void setID(uint64_t id) {
			m_id = id;
		}
		const std::string& getName()
		{
			return m_name;
		}
		/*can be concurrent*/
		inline T* get(uint64_t originCheckPoint = 0xffffffffffffffffULL)
		{
			MetaInfo* current = m_current;
			barrier;
			if (likely(current->startPos <= originCheckPoint))
			{
				barrier;
				MetaInfo* newer = m_current;
				while (newer != current)
				{
					if (newer->startPos < originCheckPoint)
						return newer->meta;
					else
						newer = newer->prev;
				}
				return current->meta;
			}
			else
			{
				MetaInfo* m = current->prev;
				while (m != nullptr)
				{
					if (m->startPos < originCheckPoint)
						return m->meta;
					else
						m = m->prev;
				}
				return nullptr;
			}
		}
		/*must be serial*/
		inline int put(T* meta, uint64_t originCheckPoint)
		{
			MetaInfo* m = new MetaInfo;
			m->startPos = originCheckPoint;
			m->meta = meta;
			if (meta != nullptr)
				meta->m_id = TableMeta::genTableId(m_id, m_version++);
			if (m_current == nullptr)
			{
				barrier;
				m_current = m;
				return 0;
			}
			else
			{
				if (m_current->startPos >= m->startPos)
				{
					LOG(ERROR) << "put meta failed for new pos:" << m->startPos << " less than current :" << m_current->startPos;
					delete m;
					return -1;
				}
				m->prev = m_current;
				barrier;
				m_current = m;
				return 0;
			}
		}
		int disableCurrent(uint64_t originCheckPoint)
		{
			return put(nullptr, originCheckPoint);
		}
		void purge(uint64_t originCheckPoint)
		{
			MetaInfo* m = m_current;
			while (m != nullptr)
			{
				if (originCheckPoint < m->startPos)
					m = m->prev;
				else
					break;
			}
			if (m == nullptr)
				return;
			while (m != nullptr)
			{
				MetaInfo* tmp = m->prev;
				if (m->meta != nullptr)
					delete m->meta;
				delete m;
				m = tmp;
			}
		}
		static int destroy(void* m, int force)
		{
			delete static_cast<MetaTimeline*>(m);
			return 0;
		}
	};
}
