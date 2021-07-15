#pragma once
#include <stdint.h>
#include <string.h>
template <typename T>
class SimpleMemCache
{
private:
	uint32_t m_volumn;
	uint32_t m_size;
	T** m_cache;
public:
	SimpleMemCache(uint32_t volumn = 128) :m_volumn(volumn), m_size(0)
	{
		m_cache = new T * [volumn];
		memset(m_cache, 0, sizeof(T*) * volumn);
	}

	~SimpleMemCache()
	{
		for (uint32_t i = 0; i < m_size; i++)
			free(m_cache[i]);
		delete[] m_cache;
	}

	inline T* alloc()
	{
		if (m_size == 0)
			return new T;

		T* t = new (m_cache[m_size])T();
		m_cache[m_size--] = nullptr;
		return t;
	}


	template<typename R>
	inline T* alloc(R r)
	{
		if (m_size == 0)
			return new T(r);

		T* t = new (m_cache[m_size])T(r);
		m_cache[m_size--] = nullptr;
		return t;
	}


	template<typename R1, typename R2>
	inline T* alloc(R1 r1, R2 r2)
	{
		if (m_size == 0)
			return new T(r1, r2);

		T* t = new (m_cache[m_size])T(r1, r2);
		m_cache[m_size--] = nullptr;
		return t;
	}

	inline void free(T* t)
	{
		if (m_size < m_volumn)
		{
			t->~T();
			m_cache[m_size++] = t;
		}
		else
		{
			delete t;
		}
	}
};