/*
 * kWaySortIterator.h
 *
 *  Created on: 2019年1月17日
 *      Author: liwei
 */

#ifndef KWAYSORTITERATOR_H_
#define KWAYSORTITERATOR_H_
#include "block.h"
#include "util/heap.h"
#include "database.h"
namespace DATABASE {
	template<typename T>
	class increaseCompare {
	public:
		inline bool operator()(const BlockIndexIterator* i, const BlockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) < *static_cast<const T*>(j->key());
		}
		static inline bool equal(const BlockIndexIterator* i, const BlockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) == *static_cast<const T*>(j->key());
		}
	};
	template<typename T>
	class decreaseCompare {
	public:
		inline bool operator()(const BlockIndexIterator* i, const BlockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) > * static_cast<const T*>(j->key());
		}
		static inline bool equal(const BlockIndexIterator* i, const BlockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) == *static_cast<const T*>(j->key());
		}
	};
	template<typename T, typename COMPARE>
	class KWaySortIterator :public Iterator {
	private:
		BlockIndexIterator** m_iters;
		uint32_t m_iterCount;
		heap< BlockIndexIterator*, COMPARE > m_heap;
		BlockIndexIterator* m_current;
	public:
		KWaySortIterator(BlockIndexIterator** iters, uint32_t iterCount) :Iterator(0, nullptr), m_iters(iters), m_iterCount(iterCount),
			m_heap(iterCount), m_current(nullptr) {
		}
		virtual ~KWaySortIterator()
		{
			for (uint32_t idx = 0; idx < m_iterCount; idx++)
				delete m_iters[idx];
			delete[]m_iters;
		}
		inline bool valid()
		{
			return m_current != nullptr && m_current->valid();
		}
		inline bool seek(const void* key)
		{
			m_heap.clear();
			for (uint32_t i = 0; i < m_iterCount; i++)
			{
				if (m_iters[i]->seek(key))
					m_heap.insert(m_iters[i]);
			}
			if (m_heap.size() > 0)
				m_current = m_heap.get();
			return true;
		}
		inline Status next()
		{
			if (m_heap.size() == 0)
				return Status::ENDED;
			if (m_current->next() == Status::OK)
			{
				m_heap.popAndInsert(m_current);
			}
			else
			{
				m_heap.pop();
				if (m_heap.size() == 0)
					return m_status = Status::ENDED;
			}
			m_current = m_heap.get();
			while (m_heap.size() > 1)
			{
				BlockIndexIterator* second;
				int sid = m_heap.getSecond(second);
				if (COMPARE::equal(m_current, second))
				{
					if (second->getBlockId() > m_current->getBlockId())
					{
						if (m_current->next() != Status::OK)
							m_heap.pop();
						else
							m_heap.popAndInsert(m_current);
						m_current = second;
					}
					else
					{
						if (second->next() != Status::OK)
							m_heap.popAt(sid);
						else
							m_heap.replaceAt(second, sid);
					}
				}
				else
					break;
			}
			return Status::OK;
		}
		inline const void* key() const
		{
			return  m_current->key();
		}
		inline const void* value()
		{
			return  m_current->value();
		}
		inline bool end()
		{
			return m_heap.size() == 0;
		}

	};
}




#endif /* KWAYSORTITERATOR_H_ */
