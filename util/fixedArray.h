#pragma once
#include <stdint.h>
template<class T>
struct fixedArray {
	T * array;
	uint32_t size;
	uint32_t volumn;
	fixedArray(uint32_t _volumn = 128):size(0),volumn(_volumn)
	{
		array = new T[volumn];
	}
	~fixedArray()
	{
		delete []array;
	}
	inline bool empty()
	{
		return size == 0;
	}
	inline bool full()
	{
		return size >= volumn;
	}
	inline T& get(uint32_t idx)
	{
		assert(idx < size);
		return array[idx];
	}
	inline T& operator[](uint32_t idx)
	{
		assert(idx < volumn);
		return array[idx];
	}
	inline T& pop()
	{
		assert(size > 0);
		return array[--size];
	}
	inline bool push(T &v)
	{
		if (full())
			return false;
		array[size++] = v;
		return true;
	}
};
