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
		inline bool operator()(const blockIndexIterator* i, const blockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) < *static_cast<const T*>(j->key());
		}
		static inline bool equal(const blockIndexIterator* i, const blockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) == *static_cast<const T*>(j->key());
		}
	};
	template<typename T>
	class decreaseCompare {
	public:
		inline bool operator()(const blockIndexIterator* i, const blockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) > * static_cast<const T*>(j->key());
		}
		static inline bool equal(const blockIndexIterator* i, const blockIndexIterator* j)
		{
			return *static_cast<const T*>(i->key()) == *static_cast<const T*>(j->key());
		}
	};
	template<typename T, typename COMPARE>
	class kWaySortIterator :public iterator {
	private:
		blockIndexIterator** m_iters;
		uint32_t m_iterCount;
		heap< blockIndexIterator*, COMPARE > m_heap;
		blockIndexIterator* m_current;
	public:
		kWaySortIterator(blockIndexIterator** iters, uint32_t iterCount) :iterator(0, nullptr), m_iters(iters), m_iterCount(iterCount),
			m_heap(iterCount), m_current(nullptr) {
		}
		virtual ~kWaySortIterator()
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
		inline status next()
		{
			if (m_heap.size() == 0)
				return status::ENDED;
			if (m_current->next() == status::OK)
			{
				m_heap.popAndInsert(m_current);
			}
			else
			{
				m_heap.pop();
				if (m_heap.size() == 0)
					return m_status = status::ENDED;
			}
			m_current = m_heap.get();
			while (m_heap.size() > 1)
			{
				blockIndexIterator* second;
				int sid = m_heap.getSecond(second);
				if (COMPARE::equal(m_current, second))
				{
					if (second->getBlockId() > m_current->getBlockId())
					{
						if (m_current->next() != status::OK)
							m_heap.pop();
						else
							m_heap.popAndInsert(m_current);
						m_current = second;
					}
					else
					{
						if (second->next() != status::OK)
							m_heap.popAt(sid);
						else
							m_heap.replaceAt(second, sid);
					}
				}
				else
					break;
			}
			return status::OK;
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
