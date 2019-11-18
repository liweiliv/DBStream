#pragma once
#include <mutex>
#include <atomic>
template <typename T>
class linkList {
	std::atomic<T *>head;
	std::atomic<T *>end;
	std::atomic<int>count;
	std::mutex pushLock;
	std::mutex popLock;

	inline void changeCount(int c)
	{
		int _count = count.load(std::memory_order_relaxed), newCount;
		do
		{
			newCount = _count + c;
		} while (!count.compare_exchange_weak(_count, newCount, std::memory_order_acq_rel));
	}
public:
	linkList()
	{
		head.store(nullptr, std::memory_order_relaxed);
		end.store(nullptr, std::memory_order_relaxed);
		count.store(0, std::memory_order_relaxed);
	}
	inline int getCount()
	{
		return count.load(std::memory_order_relaxed);
	}
	inline void push(T *& n)
	{
		n->next = nullptr;
		std::lock_guard<std::mutex> lock(pushLock);
		T * _head = head.load(std::memory_order_relaxed);
		if (_head == nullptr)
		{
			head.store(n, std::memory_order_relaxed);
			end.store(n, std::memory_order_release);
			return;
		}
		_head->next = n;
		head.store(n, std::memory_order_relaxed);
		changeCount(1);
	}
	inline T* pop()
	{
		std::lock_guard<std::mutex> plock(popLock);
		T * _end = end.load(std::memory_order_relaxed);
		if (_end == nullptr)
			return _end;
		if (_end->next == nullptr)
		{
			if (head.load(std::memory_order_acquire) == _end)
			{
				std::lock_guard<std::mutex> lock(pushLock);
				if (head.load(std::memory_order_acquire) == _end)
				{
					head.store(nullptr, std::memory_order_relaxed);
					end.store(nullptr, std::memory_order_relaxed);
					changeCount(-1);
					return _end;
				}
			}
		}
		end.store(_end->next, std::memory_order_release);
		changeCount(-1);
		return _end;
	}
};
