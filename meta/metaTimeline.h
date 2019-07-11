#pragma once
namespace META {
	template<typename T>
	class MetaTimeline
	{
	private:
		struct MetaInfo
		{
			T* meta;
			uint64_t startPos;
			//uint64_t endPos;
			MetaInfo* prev;
		};
		MetaInfo* m_current;
		uint64_t m_id;
		uint16_t m_version;
	public:
		MetaTimeline(uint64_t id) :
			m_current(NULL), m_id(id), m_version(0)
		{
		}
		~MetaTimeline()
		{
			purge(0xffffffffffffffffUL);
		}
		void setID(uint64_t id) {
			m_id = id;
		}
		/*can be concurrent*/
		inline T* get(uint64_t originCheckPoint)
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
				while (m != NULL)
				{
					if (m->startPos < originCheckPoint)
						return m->meta;
					else
						m = m->prev;
				}
				return NULL;
			}
		}
		/*must be serial*/
		inline int put(T* meta, uint64_t originCheckPoint)
		{
			MetaInfo* m = new MetaInfo;
			m->startPos = originCheckPoint;
			m->meta = meta;
			meta->m_id = tableMeta::genTableId(m_id, m_version++);
			if (m_current == NULL)
			{
				barrier;
				m_current = m;
				return 0;
			}
			else
			{
				if (m_current->startPos >= m->startPos)
				{
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
			return put(NULL, originCheckPoint);
		}
		void purge(uint64_t originCheckPoint)
		{
			MetaInfo* m = m_current;
			while (m != NULL)
			{
				if (originCheckPoint < m->startPos)
					m = m->prev;
				else
					break;
			}
			if (m == NULL)
				return;
			while (m != NULL)
			{
				MetaInfo* tmp = m->prev;
				if (m->meta != NULL)
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
