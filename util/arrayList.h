#pragma once
#include "arena.h"
#include "barrier.h"
static constexpr int arrayListNodeSize = 32;
template <class T>
struct arrayList
{
	struct node {
		T data[arrayListNodeSize];
		node * next;
		uint8_t size;
	};
	struct iterator {
		arrayList * list;
		node * n;
		uint32_t idx;
		inline T& value();
		inline bool next();
	};
	node * head;
	node * end;
	uint32_t nodeCount;
	leveldb::Arena *arena;
	arrayList(leveldb::Arena * arena):head((node*)arena->Allocate(sizeof(node))),end(head), nodeCount(1),arena(arena)
	{
		head->size = 0;
		head->next = nullptr;
	}
	inline void init(leveldb::Arena * _arena)
	{
		arena = _arena;
		head = (node*)arena->Allocate(sizeof(node));
		head->size = 0;
		head->next = nullptr;
		end = head;
		nodeCount = 1;
	}
	inline void append(const T &d)
	{
		if (end->size < arrayListNodeSize)
		{
			end->data[end->size] = d;
	//		barrier;
			end->size++;
		}
		else
		{
			node * next = (node*)arena->Allocate(sizeof(node));
			next->next = nullptr;
			next->data[0] = d;
			next->size = 1;
	//		barrier;
			end->next = next;
			end = next;
			nodeCount++;
		}
	}
	inline uint32_t size()
	{
		return (nodeCount - 1)*arrayListNodeSize + end->size;
	}
	inline bool empty()
	{
		return head == nullptr || head->size == 0;
	}
	void begin(iterator &iter)
	{
		iter.list = this;
		iter.n = head;
		iter.idx = 0;
	}
};
template<class T>
inline bool arrayList<T>::iterator::next()
{
       if (++idx >= n->size)
       {
             if (n->next == nullptr)
                    return false;
             n = n->next;
             idx = 0;
       }
       return true;
}
template<class T>
inline T & arrayList<T>::iterator::value()
{
       return n->data[idx];
}

