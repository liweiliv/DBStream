#pragma once
#include <atomic>
#include <mutex>
#include "spinlock.h"
#include <stddef.h>
struct dualLinkListNode
{
	dualLinkListNode *prev;
	dualLinkListNode* next;
	spinlock lock;
};
#ifndef container_of
#define container_of(ptr, type, member) ({\
	decltype( ((type *)0)->member ) *__mptr = (ptr);\
	(type *)( (char *)__mptr - offsetof(type,member) );})
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
		} while (!count.compare_exchange_weak(_count, newCount, std::memory_order_acq_rel));
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
					n->next = n->prev = nullptr;
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
						n->next->prev = &end;
						n->next->lock.unlock();
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
	std::mutex lock;
	globalLockDualLinkList()
	{
		head.next = nullptr;
		head.prev = nullptr;
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

};
