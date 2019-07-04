#pragma once
#include <atomic>
#include <mutex>
#include <assert.h>
#include "spinlock.h"
#include "shared_mutex.h"
#include <stddef.h>
struct dualLinkListNode
{
	dualLinkListNode *prev;
	dualLinkListNode* next;
	spinlock lock;
};
#ifndef container_of
#ifdef OS_LINUX
#define container_of(ptr, type, member) ({\
	decltype( ((type *)0)->member ) *__mptr = (ptr);\
	(type *)( (char *)__mptr - offsetof(type,member) );})
#endif
#ifdef OS_WIN
#define container_of(ptr, type, member)  (type *)( ((char *)ptr) - offsetof(type,member) )
#endif
#endif
struct dualLinkList
{
	dualLinkListNode head;
	dualLinkListNode end;
	std::atomic<int32_t> count;
	dualLinkList()
	{
		head.next = &end;
		head.prev = &end;
		end.next = &head;
		end.prev = &head;
		count.store(0, std::memory_order_relaxed);
	}
	inline void changeCount(int c)
	{
		int _count = count.load(std::memory_order_relaxed), newCount;
		do
		{
			newCount = _count + c;
		} while (!count.compare_exchange_weak(_count, newCount, std::memory_order_acquire));
	}
	inline void insertAfter(dualLinkListNode* a, dualLinkListNode * n)
	{
		dualLinkListNode * next;
		n->prev = a;
		n->lock.lock();
		do
		{
			a->lock.lock();
			next = a->next;
			if (likely(next->lock.trylock()))
			{
				n->next = next;
				a->next = n;
				next->prev = n;
				assert(next->prev!=next);
				next->lock.unlock();
				a->lock.unlock();
				n->lock.unlock();
				changeCount(1);
				return;
			}
			a->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	inline void insertAfterForHandleLock(dualLinkListNode* a, dualLinkListNode * n)
	{
		dualLinkListNode * next;
		n->prev = a;
		do
		{
			a->lock.lock();
			next = a->next;
			if (likely(next->lock.trylock()))
			{
				n->next = next;
				a->next = n;
				next->prev = n;
				assert(next->prev!=next);
				next->lock.unlock();
				a->lock.unlock();
				n->lock.unlock();
				changeCount(1);
				return;
			}
			a->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	inline void insertForHandleLock(dualLinkListNode * n)
	{
		return insertAfterForHandleLock(&head, n);
	}
	inline void insert(dualLinkListNode * n)
	{
		return insertAfter(&head, n);
	}
	inline void erase(dualLinkListNode * n)
	{
		do
		{
			n->lock.lock();
			if (likely(n->prev->lock.trylock()))
			{
				n->next->lock.lock();
				n->prev->next = n->next;
				n->next->prev = n->prev;
				assert(n->next->prev!=n->next);
				n->next->lock.unlock();
				n->prev->lock.unlock();
				n->lock.unlock();
				changeCount(-1);
				return;
			}
			n->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	inline void eraseWithHandleLock(dualLinkListNode * n)
	{
		do
		{
			n->lock.lock();
			if (likely(n->prev->lock.trylock()))
			{
				n->next->lock.lock();
				n->prev->next = n->next;
				n->next->prev = n->prev;
				assert(n->next->prev!=n->next);
				n->next->lock.unlock();
				n->prev->lock.unlock();
				changeCount(-1);
				return;
			}
			n->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	inline bool tryEraseForHandleLock(dualLinkListNode * n)
	{
		if (likely(n->prev->lock.trylock()))
		{
			n->next->lock.lock();
			n->prev->next = n->next;
			n->next->prev = n->prev;
			assert(n->next->prev!=n->next);
			n->next->lock.unlock();
			n->prev->lock.unlock();
			changeCount(-1);
			return true;
		}
		return true;
	}
	inline dualLinkListNode* getFirst()
	{
		head.lock.lock();
		dualLinkListNode * next = head.next;
		if (next == &end)
		{
			head.lock.unlock();
			return nullptr;
		}
		head.lock.unlock();
		return next;
	}
	inline dualLinkListNode* popFirst()
	{
		do
		{
			if (likely(head.lock.trylock()))
			{
				dualLinkListNode * n = head.next;
				if (n == &end)
				{
					head.lock.unlock();
					return nullptr;
				}

				if (likely(n->lock.trylock()))
				{
					n->next->lock.lock();
					head.next = n->next;
					n->next->prev = n->prev;
					n->next->lock.unlock();
					head.lock.unlock();
					n->lock.unlock();
					changeCount(-1);
					return n;
				}
				head.lock.unlock();
			}
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	inline dualLinkListNode* getLast()
	{
		end.lock.lock();
		dualLinkListNode * prev = end.prev;
		if (prev == &head)
		{
			end.lock.unlock();
			return nullptr;
		}
		end.lock.unlock();
		return prev;
	}
	inline dualLinkListNode* popLastAndHandleLock()
	{
		do
		{
			if (likely(end.lock.trylock()))
			{
				dualLinkListNode * n = end.prev;
				if (n == &head)
				{
					end.lock.unlock();
					return nullptr;
				}

				if (likely(n->lock.trylock()))
				{
					if (likely(n->prev->lock.trylock()))
					{
						end.prev = n->prev;
						n->prev->next = &end;
						n->prev->lock.unlock();
						n->lock.unlock();
						end.lock.unlock();
						changeCount(-1);
						return n;
					}
					n->lock.unlock();
				}
				end.lock.unlock();
			}
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
	}
	
	inline dualLinkListNode* popLast()
	{
		dualLinkListNode* n = popLastAndHandleLock();
		if (nullptr!=n)
			n->lock.unlock();
		return n;
	}
};
struct globalLockDualLinkListNode
{
	globalLockDualLinkListNode * prev;
	globalLockDualLinkListNode * next;
};
struct globalLockDualLinkList
{
	globalLockDualLinkListNode head;
	shared_mutex lock;
	globalLockDualLinkList()
	{
		head.next = &head;
		head.prev = &head;
	}
	inline void insertAfter(globalLockDualLinkListNode* a, globalLockDualLinkListNode * n)
	{
		n->prev = a;
		lock.lock();
		globalLockDualLinkListNode * next = a->next;
		n->next = next;
		a->next = n;
		next->prev = n;
		lock.unlock();
	}
	inline void insert(globalLockDualLinkListNode * n)
	{
		return insertAfter(&head, n);
	}
	inline void erase(globalLockDualLinkListNode * n)
	{
		lock.lock();
		n->next->prev = n->prev;
		n->prev->next = n->next;
		lock.unlock();
	}
	struct iterator {
		globalLockDualLinkListNode *m_node;
		globalLockDualLinkList *m_list;
		iterator(globalLockDualLinkList * list):m_list(list)
		{
			m_list->lock.lock_shared();
			m_node = m_list->head.next;
		}
		~iterator()
		{
			m_list->lock.unlock_shared();
		}
		inline globalLockDualLinkListNode* value()
		{
			return m_node;
		}
		inline bool valid()
		{
			return m_list != nullptr && m_node != &m_list->head;
		}
		inline bool next()
		{
			if (m_node->next == &m_list->head)
				return false;
			m_node = m_node->next;
			return true;
		}
	};

};
