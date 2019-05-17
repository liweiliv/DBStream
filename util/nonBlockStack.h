/*
 * nonBlockLinkList.h
 * actually it is a no block statck
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
	std::atomic<int> m_count;
	nonBlockStack()
	{
		m_head.store(nullptr, std::memory_order_relaxed);
		m_count.store(0,std::memory_order_relaxed);
	}
	inline void changeCount(int c)
	{
		int _count = m_count.load(std::memory_order_relaxed),newCount;
		do
		{
			newCount = _count + c;
		} while (!m_count.compare_exchange_weak(_count, newCount, std::memory_order_acq_rel));
	}
	/*thread no safe*/
	inline void pushFast(T * data)
	{
		data->next = m_head.load(std::memory_order_relaxed);
		m_head.store(data, std::memory_order_relaxed);
		m_count.fetch_add(1, std::memory_order_relaxed);
	}
	inline void push(T * data)
	{
		T *end = data;
		int count = 1;
		while (end->next != nullptr)
		{
			count++;
			end = end->next;
		}
		T *head = m_head.load(std::memory_order_relaxed);
		do
		{
			end->next = head;
		} while (!m_head.compare_exchange_weak(head, data, std::memory_order_acq_rel));
		changeCount(count);
	}
	/*thread no safe*/
	inline T *popFast()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		m_head.store(head->next, std::memory_order_relaxed);
		m_count.fetch_sub(1, std::memory_order_relaxed);
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
		changeCount(-1);
		head->next = nullptr;
		return head;
	}
	/*thread no safe*/
	inline T * popAllFast()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		m_head.store(nullptr, std::memory_order_relaxed);
		m_count.store(0, std::memory_order_relaxed);
		return head;
	}
	inline T * popAll()
	{
		T *head = m_head.load(std::memory_order_relaxed);
		if (head == nullptr)
			return nullptr;
		int count;
		do
		{
			count = m_count.load(std::memory_order_relaxed);
		} while (!m_head.compare_exchange_weak(head, nullptr, std::memory_order_acq_rel));
		changeCount(-count);
		return head;
	}
};

#endif /* NONBLOCKLINKLIST_H_ */
