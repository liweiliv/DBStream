#pragma once
#include <atomic>
struct ref {
	std::atomic<int> m_ref;
	ref():m_ref(0) {
	}
	inline bool use()
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (ref < 0)
				return false;
			if (m_ref.compare_exchange_weak(ref, ref + 1, std::memory_order_release, std::memory_order_relaxed))
				break;
		} while (1);
		return true;
	}
	inline bool unuse()
	{
		int ref = m_ref.load(std::memory_order_relaxed);
		do {
			if (m_ref.compare_exchange_weak(ref, ref - 1, std::memory_order_release, std::memory_order_relaxed))
				return ref>0;
		} while (1);
	}
};
