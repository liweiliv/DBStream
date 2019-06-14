#pragma once
#include <string.h>
#include"likely.h"
#include "winDll.h"
extern thread_local int threadid;
static constexpr int maxThreadCount = 256;

void registerThreadLocalVar(void (*_unset)(void* v), void* v);
void destroyThreadLocalVar(void* v);
class threadLocalWrap {
public:
	threadLocalWrap(); //we use this function to init threadid
	~threadLocalWrap();
	inline void idle() {}//do no thing ,thread_local varaiable will init when use it first
};
extern thread_local threadLocalWrap _threadLocalWrap;
template<class T>
class DLL_EXPORT threadLocal
{
protected:
	T* m_var[maxThreadCount];
public:
	threadLocal()
	{
		memset(m_var, 0, sizeof(m_var));
		registerThreadLocalVar(_unset, this);
	}
	virtual ~threadLocal() 
	{
		destroyThreadLocalVar(this);
		for (int i = 0; i < maxThreadCount; i++)
		{
			if (m_var[i] != nullptr)
				delete m_var[i];
		}
	}
	inline T* get()
	{
		_threadLocalWrap.idle();
		return m_var[threadid];
	}
	inline void set(T* v)
	{
		_threadLocalWrap.idle();
		m_var[threadid] = v;
	}
	inline void unset()
	{
		_threadLocalWrap.idle();
		if (likely(m_var[threadid] != nullptr))
		{
			delete m_var[threadid];
			m_var[threadid] = nullptr;
		}
	}
	static void _unset(void * v)
	{
		static_cast<threadLocal<T>*>(v)->unset();
	}
};

