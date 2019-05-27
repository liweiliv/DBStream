#pragma once
#include"likely.h"
static constexpr int maxThreadCount = 256;
extern thread_local int threadid;
bool initLocalThreadId();
template<class T>
class threadLocal
{
protected:
	T* m_var[maxThreadCount];
	virtual T * createVar()
	{
		return new T;
	}
public:
	threadLocal()
	{
		memset(m_var, 0, sizeof(m_var));
	}
	virtual ~threadLocal() 
	{
		for (int i = 0; i < maxThreadCount; i++)
		{
			if (m_var[i] != nullptr)
				delete m_var[i];
		}
	}
	inline T* get()
	{
		if (unlikely(threadid >= maxThreadCount))
		{
			if (!initLocalThreadId())
				return nullptr;
		}
		if (likely(m_var[threadid]!=nullptr))
		{
			return m_var[threadid];
		}
		else
		{
			return (m_var[threadid] = createVar());
		}
	}
};
