#pragma once
#include <atomic>
#include <chrono>
#include <thread>
#include "barrier.h"
#include "likely.h"
template<class T>
class ringFixedQueue
{
private:
	T* array;
	uint32_t arraySize;
	uint32_t mask;
	int32_t head;
	std::atomic<int32_t> end;
public:
	ringFixedQueue(uint32_t size = 32):arraySize(1)
	{
		for (int i = 0;; i++)
		{
			if (arraySize >= size)
				break;
			arraySize <<= 1;
		}
		mask = arraySize - 1;
		array = new T[arraySize];
		head = 0;
		end.store(0, std::memory_order_relaxed);
	}
	~ringFixedQueue()
	{
		delete[]array;
	}
	/*
	only one thread can push
	if queue is full,return false
	outtime: in us
	*/
	inline bool push(const T& v, int32_t outtime = 0)
	{
		int32_t next = (head + 1) & mask;
		while (unlikely(next == end.load(std::memory_order_relaxed)))
		{
			if (outtime <= 0)
				return false;
			std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
			outtime--;
		}
		array[head] = v;
		head = next;
		return true;
	}
	/*
	thread safe,multi thread can pop
	if queue is empty,return false
	outtime: in us
	*/
	inline bool pop(T& v, int32_t outtime = 0)
	{
		int32_t e = end.load(std::memory_order_relaxed);
		do
		{
			while (e == head)
			{
				if (outtime <= 0)
					return false;
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
				outtime--;
			}
			v = array[e];
		} while (!end.compare_exchange_weak(e,(e+1)&mask));
		return true;
	}
};
