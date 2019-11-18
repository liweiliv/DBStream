#pragma once
#include <atomic>
#include "likely.h"
struct ref {
	std::atomic<int> m_ref;
	ref():m_ref(0) {
	}
	inline void reset()
	{
		m_ref.store(0, std::memory_order_release);
	}
	inline int use()
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (unlikely(ref < 0))
				return ref;
			if (m_ref.compare_exchange_weak(ref, ref + 1, std::memory_order_release, std::memory_order_relaxed))
				return ref + 1 ;
		} while (1);
	}
	inline int unuse()
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (m_ref.compare_exchange_weak(ref, ref - 1, std::memory_order_release, std::memory_order_relaxed))
				return ref -1;
		} while (1);
	}
	inline bool own()//call use before own
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		if (ref < 0)
			return false;
		return m_ref.compare_exchange_strong(ref, -ref, std::memory_order_release, std::memory_order_relaxed);
	}
	inline void waitForShare()//call use before own
	{
		for (;;)
		{
			if (m_ref.load(std::memory_order_relaxed) > 0)
				return;
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		}
	}
	inline bool share()//call use before own
	{
		int ref;
		for (;;)
		{
			if ((ref = m_ref.load(std::memory_order_relaxed)) < 0)
			{
				if (m_ref.compare_exchange_weak(ref, -ref, std::memory_order_release, std::memory_order_relaxed))
					return true;
				else
					continue;
			}
			else
				return false;
		}
	}

	inline bool tryUnuseIfZero()
	{
		int ref;
		for(;;)
		{
			if((ref = m_ref.load(std::memory_order_relaxed))== 0)
			{
				if (m_ref.compare_exchange_weak(ref, ref - 1, std::memory_order_release, std::memory_order_relaxed))
					return true;
				else
					continue;
			}
			else
				return false;
		}
	}
};
