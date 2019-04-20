#pragma once
#include <atomic>
#include "iterator.h"
#include "../util/unblockedQueue.h"
namespace STORE
{
	class waitCondition {
	private:
		unblockedQueue<iterator*> m_queue;
	public:
		waitCondition()
		{}
		inline void wakeUp()
		{
			iterator *iter;
			while (nullptr != (iter = m_queue.pop()))
			{
				iter->s
			}
		}
		inline void wait(iterator * iter)
		{
			iterator * end = m_end.load(std::memory_order_acq_rel),*next = nullptr;
			if (end == nullptr)
			{
				while (false == m_end.compare_exchange_weak(end, iter))
				{
					if ((end = m_end.load(std::memory_order_acq_rel)) == nullptr)
						continue;
					else
					{
						goto NOT_END;
					}
				}
			}
		NOT_END:
			next = end->m_next.load()
		}

	};
}
