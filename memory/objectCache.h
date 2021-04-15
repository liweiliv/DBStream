#pragma once
#include <stdint.h>
#include <string>
#include <mutex>
#include"thread/threadLocal.h"
template <typename T>
class objectCache {
private:
	struct cache {
		T** c;
		uint32_t volumn;
		uint32_t count;
		cache* next;
		cache(uint32_t volumn) :volumn(volumn), count(0), next(nullptr)
		{
			c = new T * [volumn];
			memset(c, 0, sizeof(T*) * volumn);
		}
		~cache()
		{
			for (int i = 0; i < count; i++)
				delete c[i];
		}
	};
	struct dualCache {
		cache* c1;
		cache* c2;
		dualCache() :c1(nullptr), c2(nullptr) {}
		~dualCache()
		{
			if (c1 != nullptr)
				delete c1;
			if (c2 != nullptr)
				delete c2;
		}
	};
private:
	std::mutex m_lock;
	uint32_t m_maxGlobalIdleCacheCount;
	uint32_t m_maxThreadLoaclCacheCount;
	threadLocal<dualCache> m_threadLocalCache;
	cache* m_globalHead;
	volatile uint32_t m_globalCacheCount;
public:
	objectCache(uint32_t maxGlobalIdleCacheCount = 1024, uint32_t maxThreadLoaclCacheCount = 32) :m_maxGlobalIdleCacheCount(maxGlobalIdleCacheCount),
		m_maxThreadLoaclCacheCount(maxThreadLoaclCacheCount > 2 ? 2 : maxThreadLoaclCacheCount), m_globalHead(nullptr), m_globalCacheCount(0)
	{}
	~objectCache()
	{
		m_threadLocalCache.clear();
		cache* c = m_globalHead;
		while (c != nullptr)
		{
			cache* next = c->next;
			delete c;
			c = next;
		}
	}
private:
	inline T* getByDualCache(dualCache* c)
	{
		if (unlikely(c == nullptr))
			m_threadLocalCache.set(c = new dualCache());
		if (c->c1->count > 0)
			return c->c1->c[--c->c1->count];
		if (c->c2->count > 0)
			return c->c2->c[--c->c2->count];
		if (m_globalHead != nullptr)
		{
			std::lock_guard<std::mutex> guard(m_lock);
			if (m_globalHead == nullptr)
				return new T();
			delete  c->c1;
			c->c1 = m_globalHead;
			m_globalHead = m_globalHead->next;
			return  c->c1->c[--c->c1->count];
		}
		else
			return new T();
	}
	inline void freeByDualCache(dualCache* c, T* t)
	{
		if (unlikely(c == nullptr))
			m_threadLocalCache.set(c = new dualCache());
		if (c->c1->count < c->c1->volumn)
		{
			c->c1->c[c->c1->count++] = t;
			return;
		}
		if (c->c2->count < c->c2->volumn)
		{
			c->c2->c[c->c2->count++] = t;
			return;
		}
		cache* cn = new cache(m_maxGlobalIdleCacheCount >> 1);
		cn->c[cn->count++] = t;
		std::lock_guard<std::mutex> guard(m_lock);
		c->c2->next = m_globalHead;
		m_globalHead = c->c2;
		c->c2 = c->c1;
		c->c1 = cn;
	}
public:
	inline T* getByTid(uint32_t tid)
	{
		return  getByDualCache(m_threadLocalCache.get(tid));
	}
	inline T* get()
	{
		return  getByDualCache(m_threadLocalCache.get());
	}
	inline void freeObject(T* t)
	{
		freeByDualCache(m_threadLocalCache.get(), t);
	}
	inline void freeObjectByTid(uint32_t tid, T* t)
	{
		freeByDualCache(m_threadLocalCache.get(tid), t);
	}
};