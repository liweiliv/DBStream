#pragma once
#include <stdint.h>
#include "../memory/bufferPool.h"
#include "../util/barrier.h"
#ifndef TEST_BITMAP
#define TEST_BITMAP(m,i) (((m)[(i)>>3]>>(i&0x7))&0x1)
#endif
#ifndef SET_BITMAP
#define SET_BITMAP(m,i)  (m)[(i)>>3]|= (0x01<<(i&0x7))
#endif
namespace REPLICATOR {
	static constexpr auto DEFAULT_LOAD_FACTOR = 0.75f;
	static constexpr auto MAX_KEY_VOLUMN = 256;
	static constexpr auto DEFAULT_NODE_COUNT = 16;

	template<typename Hash,typename Key, typename Value>
	struct hashTableNode {
		hashTableNode* next;
		const Hash hash;
		const Key key;
		Value value;
	};
	template<typename Hash, typename Key, typename Value>
	struct  hashTableVersion
	{
		hashTableNode<Hash,Key,Value>** nodes;
		uint8_t* filter;
		uint16_t nodeCount;
		uint16_t nodeCountMask;
		uint16_t maxVolumn;
		uint16_t currentKeyCount;
		hashTableVersion* prev;
		char buf[1];
		static inline uint32_t allocExternSize(uint16_t nodeCount)
		{
			return (sizeof(hashTableNode<Hash, Key, Value>*) * nodeCount) + nodeCount;
		}
		static inline uint32_t allocSize(uint16_t nodeCount)
		{
			return sizeof(hashTableVersion) + allocExternSize(nodeCount);
		}
		void init(uint16_t nodeCount)
		{
			this->nodeCount = nodeCount;
			nodeCountMask = nodeCount - 1;
			maxVolumn = nodeCount * DEFAULT_LOAD_FACTOR;
			currentKeyCount = 0;
			nodes = (hashTableNode<Hash, Key, Value> * *)(void*) & buf[0];
			filter = (uint8_t*)((&buf[0]) + sizeof(hashTableNode<Hash, Key, Value>*) * nodeCount);
			memset(buf, 0, allocExternSize(nodeCount));
		}
		void clear()
		{
			for (uint16_t idx = 0; idx < count; idx++)
			{
				hashTableNode<Hash, Key, Value>* node = nodes[idx];
				while (node != nullptr)
				{
					hashTableNode<Hash, Key, Value>* tmp = node->next;
					bufferPool::free(node);
					node = tmp;
				}
			}
			bufferPool::free(nodes);
		}
		inline Value get(const Key& key, const Hash& hash)
		{
			hashTableNode<Hash, Key, Value>* node = nodes[hash & nodeCountMask];
			if (node == nullptr)
				return (decltype(Value))(0L);
			do {
				if (node->hash == hash&& node->key == key)
					return node->value;
				node = node->next;
			} while (node != nullptr);
			return (decltype(Value))(0L);
		}
		inline bool erase(const Key& key, const Hash& hash)
		{
			hashTableNode<Hash, Key, Value>* node = nodes[hash & nodeCountMask],*prev = nullptr;
			if (node == nullptr)
				return;
			do {
				if (node->hash == hash && node->key == key)
				{
					if (prev != nullptr)
						prev->next = node->next;
					else
						nodes[hash & nodeCountMask] = node->next;
					currentKeyCount--;
					return true;
				}
				prev = node;
				node = node->next;
			} while (node != nullptr);
			return false;
		}
		inline bool insert(bufferPool* pool,const Key& key, const Hash& hash, Value& value)
		{
			uint16_t idx = hash & nodeCountMask;
			hashTableNode<Hash, Key, Value>* node = nodes[idx];
			if (node != nullptr)
			{
				do {
					if (node->hash == hash && node->key == key)
						return false;
					node = node->next;
				} while (node != nullptr);
			}
			node = static_cast<hashTableNode<Hash, Key, Value>*>(pool->alloc(sizeof(hashTableNode<Hash, Key, Value>)));
			node->hash = hash;
			node->key = key;
			node->value = value;
			if (nodes[idx] == nullptr)
			{
				nodes[idx] = newNode;
				node->next = nullptr;
			}
			else
			{
				node->next = nodes[idx];
				nodes[idx] = newNode;
			}
			currentKeyCount++;
			return true;
		}
		static inline void destroy(hashTableVersion* v)
		{
			bufferPool::free(v->nodes);
			bufferPool::free(v);
		}
		inline bool notExist(const Hash& hash)
		{
			if (!TEST_BITMAP(filter, hash & ((nodeCount << 3) - 1)))
				return true;
#if sizeof(hash)<=2
			if (!TET_BITMAP(filter, (hash >> 8) & ((nodeCount << 3) - 1)))
				return true;
#else
			if (!TEST_BITMAP(filter, (hash >> 16) & ((nodeCount << 3) - 1)))
				return true;
#endif
			return false;
		}
		void setFilter()
		{
			for (uint16_t idx = 0; idx < count; idx++)
			{
				hashTableNode<Hash, Key, Value>* node = nodes[idx];
				while (node != nullptr)
				{
					SET_BITMAP(filter, node->hash & ((nodeCount << 3) - 1));
#if sizeof(Hash)<=2
					SET_BITMAP(filter, (node->hash >> 8) & ((nodeCount << 3) - 1));
#else
					SET_BITMAP(filter, (node->hash >> 16) & ((nodeCount << 3) - 1));
#endif
					node = node->next;
				}
				nodes[idx] = nullptr;
			}

		}
		hashTableVersion * resize(bufferPool *pool)
		{
			hashTableVersion* v = static_cast<hashTableVersion*>(pool->alloc(allocSize(nodeCount << 1)));
			v->init(nodeCount << 1);
			for (uint16_t idx = 0; idx < count; idx++)
			{
				hashTableNode<Hash, Key, Value>* node = nodes[idx];
				while (node != nullptr)
				{
					hashTableNode<Hash, Key, Value>* head = v->nodes[node->hash & v->nodeCount];
					hashTableNode<Hash, Key, Value>* tmp = node->next;
					v->nodes[node->hash & v->nodeCount] = node;
					node->next = head;
					node = tmp;
				}
				nodes[idx] = nullptr;
			}
			return v;
		}
	};
	template<typename Hash, typename Key, typename Value>
	struct hashTable {
		bufferPool* pool;
		hashTableVersion< Hash, Key, Value>* current;
		hashTable(bufferPool* pool) :pool(pool)
		{
			current = static_cast<hashTableVersion<Hash, Key, Value>*>(pool->alloc(hashTableVersion<Hash, Key, Value>::allocSize(DEFAULT_NODE_COUNT)));
			_new->init(DEFAULT_NODE_COUNT);
			current->prev = nullptr;
		}
		~hashTable()
		{
			hashTableVersion< Hash, Key, Value>* v = current;
			while (v != nullptr)
			{
				hashTableVersion< Hash, Key, Value> prev = v->prev;
				hashTableVersion< Hash, Key, Value>::destroy(v);
				v = prev;
			}
		}
		inline Value get(const Key& key, const Hash& hash)
		{
			Value _v;
			if ((_v = current->get(key, hash)) != (decltype(Value))(0L))
				return _v;
			hashTableVersion< Hash, Key, Value>* version = current->prev;
			if (version != nullptr)
			{
				do{
					if ((_v = version->get(key, hash)) != (decltype(Value))(0L))
					{
						version->erase(key, hash);
						insert(key, hash, _v);
						return _v;
					}
					version = version->prev;
				} while (version != nullptr);
			}
			return (decltype(Value))(0L);
		}
		inline void erase(const Key& key, const Hash& hash)
		{
			hashTableVersion< Hash, Key, Value>* v = current,*next = nullptr;
			while (v != nullptr)
			{
				if (v->erase(key, hash))
				{
					if (v != current&&v->currentKeyCount==0)
					{
						next->prev = v->prev;
						hashTableVersion< Hash, Key, Value>::destroy(v);
					}
					break;
				}
				next = v;
				v = v->prev;
			}
		}
		inline bool insert(const Key& key, const Hash& hash, Value& value)
		{
			if (get(key, hash) != (decltype(Value))(0L))
				return false;
			if (current->currentKeyCount >= current->maxVolumn)
			{
				if (current->currentKeyCount >= MAX_KEY_VOLUMN)
				{
					hashTableVersion< Hash, Key, Value>* _new = current->resize(pool);
					hashTableVersion< Hash, Key, Value>* tmp = current;
					current = _new;
					barrier;
					hashTableVersion< Hash, Key, Value>::destroy(tmp);
				}
				else
				{
					hashTableVersion< Hash, Key, Value>* _new = static_cast<hashTableVersion<Hash, Key, Value>*>(pool->alloc(hashTableVersion<Hash, Key, Value>::allocSize(DEFAULT_NODE_COUNT)));
					_new->init(DEFAULT_NODE_COUNT);
					_new->prev = current;
					current = _new;
				}
			}
			current->insert(key, hash, value);
		}
	};
}
