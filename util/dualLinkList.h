#pragma once
#include <atomic>
#include <mutex>
#include "spinlock.h"
struct dualLinkListNode
{
	dualLinkListNode *prev;
	dualLinkListNode* next;
	spinlock lock;
};
struct dualLinkList
{
	dualLinkListNode head;
	dualLinkListNode end;
	dualLinkList()
	{
		head.next = &end;
		head.prev = &end;
		end.next = &head;
		end.prev = &head;
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
				return;
			}
			a->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
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
				return;
			}
			n->lock.unlock();
			std::this_thread::sleep_for(std::chrono::nanoseconds(100));
		} while (1);
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
