#pragma once
#include <atomic>
#include <chrono>
#include <thread>
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
		return m_ref.fetch_add(1, std::memory_order_relaxed);
	}
	inline int unuse()
	{
		return m_ref.fetch_add(-1, std::memory_order_relaxed);
	}
	inline bool own()//call use before own
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (ref < 0)
				return false;
			if (m_ref.compare_exchange_weak(ref, -ref, std::memory_order_relaxed, std::memory_order_relaxed))
				return true;
		} while (true);
	}
	inline int unuseForLru()
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (ref == 1)
			{
				if (m_ref.compare_exchange_weak(ref, -ref, std::memory_order_relaxed, std::memory_order_relaxed))
					return 0;
			}
			else
			{
				if (m_ref.compare_exchange_weak(ref, ref -1, std::memory_order_relaxed, std::memory_order_relaxed))
					return ref - 1;
			}
		} while (true);
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
