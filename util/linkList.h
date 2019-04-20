#pragma once
#include "arena.h"
template <typename T>
#define L_DEFAULT_NODE_SIZE 1024
#define L_NODE_VOLUMN (L_DEFAULT_NODE_SIZE-offsetof(node,data))/sizeof(T)
class linkList {
private:
	struct node {
		node * next;
		uint16_t count;
		T data[1];
	};
	leveldb::Arena *m_arena;
	node *m_head;
	node *m_end;
public:
	linkList(leveldb::Arena *arena) :m_arena(arena), m_endNodeValueCount(0)
	{
		m_head = arena->AllocateAligned(L_DEFAULT_NODE_SIZE);
		m_end = m_head;
	}
	inline void put(const T & value)
	{
		if (m_end->count >= L_DEFAULT_NODE_SIZE)
		{
			node * next  = arena->AllocateAligned(L_DEFAULT_NODE_SIZE);
			next->data[0] = value;
			next->count = 1;
			m_end->next = next;
			barrier;
			m_end = next;
		}
		else
		{
			m_end->data[m_end->count] = value;
			barrier;
			m_end->count;
		}
	}
public:
	class iterator {
		linkList * l;
		node *n;
		int16_t idx;
		iterator(linkList * list):l(list),n(list->m_head),idx(0)
		{
			if (n->count == 0)
				idx = -1;
		}
		inline bool valid()
		{
			return idx >= 0;
		}
		inline bool next()
		{
			if (idx >= n->count - 1)
			{
				if (n == l->m_end)
					return false;
				else
				{
					n = n->next;
					idx = 0;
					return true;
				}
			}
			else
			{
				idx++;
				return true;
			}	
		}
		inline T &value()
		{
			return n->data[idx];
		}
	};

};
