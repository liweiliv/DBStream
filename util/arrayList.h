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
		arrayList<T> * list;
		node<T> * n;
		uint32_t idx;
		inline T& value();
		inline bool next();
	};
	node * head;
	node * end;
	uint32_t nodeCount;
	leveldb::Arena *arena;
	arrayList(leveldb::Arena * arena):head((node*)arena->Allocate(sizeof(node))),end(head), endSize(0), nodeCount(1),arena(arena)
	{}
	inline void append(const T &d)
	{
		if (endSize < arrayListNodeSize)
		{
			end->data[end->size] = d;
			barrier;
			end->size++;
		}
		else
		{
			node * next = (node*)arena->Allocate(sizeof(node));
			next->next = nullptr;
			next->data[0] = d;
			next->size = 1;
			barrier;
			end->next = next;
			nodeCount++;
		}
	}
	inline uint32_t size()
	{
		return (nodeCount - 1)*arrayListNodeSize + end->size;
	}
	void begin(iterator &iter)
	{
		iter.list = this;
		iter.n = head;
		iter.idx = 0;
	}
	inline bool iterator::next()
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
	inline T & iterator::value()
	{
		return n->data[idx];
	}
};
