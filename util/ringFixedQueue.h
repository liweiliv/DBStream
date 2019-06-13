#pragma once
#include <atomic>
#include <chrono>
#include "likely.h"
template<class T>
class ringFixedQueue
{
private:
	T* array;
	uint32_t arraySize;
	std::atomic<int32_t> head;
	std::atomic<int32_t> end;
public:
	ringFixedQueue(uint32_t size = 32):array(new T[size]),arraySize(size)
	{
		head.store(0, std::memory_order_relaxed);
		end.store(0, std::memory_order_relaxed);
	}
	~ringFixedQueue()
	{
		delete[]array;
	}
	/*
	only one thread can push
	if queue is full,return false
	outtime: in ms
	*/
	inline bool push(const T& v, int32_t outtime = 0)
	{
		int32_t h;
		while (unlikely((h=(head.load(std::memory_order_relaxed) + 1 % arraySize)) == end.load(std::memory_order_relaxed)))
		{
			if (outtime <= 0)
				return false;
			std::this_thread::sleep_for(std::chrono::nanoseconds(1000000));
			outtime--;
		}
		array[h] = v;
		head.store(h, std::memory_order_acquire);
		return true;
	}
	/*
	thread safe,multi thread can pop
	if queue is empty,return false
	outtime: in ms
	*/
	inline bool pop(T& v, int32_t outtime = 0)
	{
		int32_t e = end.load(std::memory_order_relaxed);
		do
		{
			while (e == head.load(std::memory_order_relaxed))
			{
				if (outtime <= 0)
					return false;
				std::this_thread::sleep_for(std::chrono::nanoseconds(1000000));
				outtime--;
			}
			v = array[e];
		} while (!end.compare_exchange_weak(e,(e+1)%arraySize));
		return true;
	}
};
