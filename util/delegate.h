#pragma once
template <class F>
F* create_delegate(F* f)
{
	return f;
}
#define _MEM_DELEGATES(_Q,_NAME)\
template <class T, class R, class ... P>\
struct _mem_delegate ## _NAME\
{\
    T* m_t;\
    R  (T::*m_f)(P ...) _Q;\
    _mem_delegate ## _NAME(T* t, R  (T::*f)(P ...) _Q):m_t(t),m_f(f) {}\
    _mem_delegate ## _NAME(const _mem_delegate ## _NAME &m):m_t(m.m_t),m_f(m.m_f){}\
    _mem_delegate ## _NAME& operator=(const _mem_delegate ## _NAME &m){m_t=m.m_t;m_f=m.m_f;return *this;}\
    R operator()(P ... p) _Q\
    {\
        return (m_t->*m_f)(p ...);\
    }\
};\
\
template <class T, class R, class ... P>\
    _mem_delegate ## _NAME<T,R,P ...> create_delegate(T* t, R (T::*f)(P ...) _Q)\
{\
    _mem_delegate ##_NAME<T,R,P ...> d(t,f);\
    return d;\
}

_MEM_DELEGATES(, Z)
_MEM_DELEGATES(const, X)
_MEM_DELEGATES(volatile, Y)
_MEM_DELEGATES(const volatile, W)

