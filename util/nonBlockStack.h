/*
 * nonBlockLinkList.h
 * actually it is a no block stack
 *  Created on: 2018年12月20日
 *      Author: liwei
 */
#ifndef NONBLOCKLINKLIST_H_
#define NONBLOCKLINKLIST_H_
#include <atomic>
/*
T must hava a [next] pointer
*/
template <class T>
struct nonBlockStack
{
	std::atomic<T*> m_head;
#ifndef ST_WITHOUT_COUNT
	std::atomic<int> m_count;
#endif
	nonBlockStack()
	{
		m_head.store(nullptr, std::memory_order_relaxed);
#ifndef ST_WITHOUT_COUNT
		m_count.store(0,std::memory_order_relaxed);
#endif
	}
#ifndef ST_WITHOUT_COUNT
	inline void changeCount(int c)
	{
		int _count = m_count.load(std::memory_order_relaxed),newCount;
		do
		{
			newCount = _count + c;
		} while (!m_count.compare_exchange_weak(_count, newCount, std::memory_order_acq_rel));
	}
#endif
	/*thread not safe*/
	inline void pushFast(T * data)
	{
		T* end = data;
#ifndef ST_WITHOUT_COUNT
		int count = 1;
#endif
		while (end->next != nullptr)
		{
#ifndef ST_WITHOUT_COUNT
			count++;
#endif
			end = end->next;
		}
		T* head = m_head.load(std::memory_order_relaxed);
		end->next = head;
		m_head = data;
#ifndef ST_WITHOUT_COUNT
		m_count += count;
#endif
	}
#ifndef ST_WITHOUT_COUNT
	inline T * pushFastUntilCount(T* data,int max)
	{
		T* end = data;
		T* next;
		int count = 1;
		while (end->next != nullptr
			&&count+m_count<max)
		{
			count++;
			end = end->next;
		}
		T* head = m_head.load(std::memory_order_relaxed);
		next = end->next;
		end->next = head;
		m_head = data;
		m_count += count;
		return next;
	}
#endif
	inline void push(T * data)
	{
		T *end = data;
#ifndef ST_WITHOUT_COUNT
		int count = 1;
#endif
		while (end->next != nullptr)
		{
#ifndef ST_WITHOUT_COUNT
			count++;
#endif
			end = end->next;
		}
		T *head = m_head.load(std::memory_order_relaxed);
		do
		{
			end->next = head;
		} while (!m_head.compare_exchange_weak(head, data, std::memory_order_acq_rel));
#ifndef ST_WITHOUT_COUNT
		changeCount(count);
#endif
	}
	/*thread not safe*/
	inline T *popFast()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		m_head.store(head->next, std::memory_order_relaxed);
#ifndef ST_WITHOUT_COUNT
		m_count.fetch_sub(1, std::memory_order_relaxed);
#endif
		head->next = nullptr;
		return head;
	}
	inline T *pop()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		T *prev = nullptr;
		do
		{
			prev = head->next;
		} while (!m_head.compare_exchange_weak(head, prev, std::memory_order_acq_rel));
#ifndef ST_WITHOUT_COUNT
		changeCount(-1);
#endif
		head->next = nullptr;
		return head;
	}
	/*thread not safe*/
	inline T * popAllFast()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		m_head.store(nullptr, std::memory_order_relaxed);
#ifndef ST_WITHOUT_COUNT
		m_count.store(0, std::memory_order_relaxed);
#endif
		return head;
	}
	inline T * popAll()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
#ifndef ST_WITHOUT_COUNT
		int count;
#endif
		do
		{
#ifndef ST_WITHOUT_COUNT
			count = m_count.load(std::memory_order_relaxed);
#endif
		} while (!m_head.compare_exchange_weak(head, nullptr, std::memory_order_acq_rel));
#ifndef ST_WITHOUT_COUNT
		changeCount(-count);
#endif
		return head;
	}
};

#endif /* NONBLOCKLINKLIST_H_ */

