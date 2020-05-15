#pragma once
#include <stdint.h>
#include <mutex>
#include "metaData.h"
#include "thread/barrier.h"
#include "util/winDll.h"
namespace META {
	class tableIdTree
	{
	private:
		struct node
		{
			uint64_t startId;
			uint64_t endId;
			void* child[512];
			node* parent;
			uint16_t level;
			bool serial;
			node()
			{
				memset(child, 0, sizeof(child));
			}
		};
		struct tableMetaWrap
		{
			tableMeta* meta;
			tableMetaWrap* prev;
		};
		node* m_root;
		std::mutex m_writeLock;

	public:
		tableIdTree()
		{
			m_root = new node();
			m_root->endId = 0;
			m_root->startId = 0;
			m_root->level = 0;
			m_root->parent = nullptr;
			m_root->serial = true;
		}
		~tableIdTree()
		{
			cleanNode(m_root);
		}
		void cleanNode(node* n)
		{
			for (int i = 0; i < 512; i++)
			{
				if (n->child[i] != nullptr)
				{
					if (n->level > 0)
						cleanNode(static_cast<node*>(n->child[i]));
					else
					{
						tableMetaWrap* w = static_cast<tableMetaWrap*>(n->child[i]), * prev;
						while (w != nullptr)
						{
							prev = w->prev;
							delete w;
							w = prev;
						}
					}
					n->child[i] = nullptr;
				}
			}
			delete n;
		}
		void put(tableMeta* meta)
		{
			uint64_t tableId = tableMeta::tableID(meta->m_id);
			m_writeLock.lock();
			node* n = m_root;
			while (true)
			{
				if (n->serial)
				{
					uint16_t cid = (uint16_t)((tableId - n->startId) >> (n->level * 9));//every node has 512(1<<9) slot
					if (cid >= 512)
					{
						assert(n == m_root);
						node* newNode = new node;
						newNode->child[0] = m_root;
						newNode->parent = nullptr;
						newNode->level = m_root->level + 1;
						newNode->serial = true;
						newNode->startId = m_root->startId;
						newNode->endId = m_root->endId;
						barrier;
						n = m_root = newNode;
						continue;
					}
					if (n->child[cid] == nullptr)
					{
						if (n->level == 0)
						{
							tableMetaWrap* m = new tableMetaWrap;
							m->meta = meta;
							m->prev = nullptr;
							barrier;
							n->child[cid] = m;
							while (n != nullptr && n->endId < meta->m_id)
							{
								n->endId = meta->m_id;
								n = n->parent;
							}
							m_writeLock.unlock();
							return;
						}
						else
						{
							node* newNode = new node;
							newNode->level = n->level - 1;
							newNode->parent = n;
							newNode->startId = n->startId + cid * (1 << (n->level * 9));
							newNode->endId = 0;
							newNode->serial = true;
							barrier;
							n->child[cid] = newNode;
							n = newNode;
							continue;
						}
					}
					else
					{
						if (n->level == 0)
						{
							tableMetaWrap* m = static_cast<tableMetaWrap*>(n->child[cid]);
							assert(tableMeta::tableID(m->meta->m_id) == tableId);
							assert(tableMeta::tableVersion(m->meta->m_id) < tableMeta::tableVersion(meta->m_id));
							tableMetaWrap* newMetaWrap = new tableMetaWrap;
							newMetaWrap->meta = meta;
							newMetaWrap->prev = m;
							barrier;
							n->child[cid] = newMetaWrap;
							while (n != nullptr && n->endId < meta->m_id)
							{
								n->endId = meta->m_id;
								n = n->parent;
							}
							m_writeLock.unlock();
							return;
						}
						else
						{
							n = static_cast<node*>(n->child[cid]);
							continue;
						}
					}
				}
				else
				{
					abort();//not support now
				}
			}
		}
		inline tableMeta* getPrevVersion(uint64_t tableId)
		{
			uint64_t id = tableMeta::tableID(tableId);
			node* n = m_root;
			for (;;)
			{
				if (n->serial)
				{
					uint16_t cid = (uint16_t)((id - n->startId) >> (n->level * 9));
					if (cid >= 512)
						return nullptr;
					if (n->child[cid] == nullptr)
						return nullptr;
					if (n->level == 0)
					{
						tableMetaWrap* m = static_cast<tableMetaWrap*>(n->child[cid]);
						assert(tableMeta::tableID(m->meta->m_id) == id);
						if (m->meta->m_id < tableId)
							return nullptr;
						for (; m != nullptr;)
						{
							if (m->meta->m_id == tableId)
								return m->prev == nullptr ? nullptr : m->prev->meta;
							m = m->prev;

						}
						return nullptr;
					}
					else
						n = static_cast<node*>(n->child[cid]);
				}
				else
				{
					abort();//not support now
				}
			}
		}
		inline tableMeta* get(uint64_t tableId)
		{
			uint64_t id = tableMeta::tableID(tableId);
			node* n = m_root;
			for (;;)
			{
				if (n->serial)
				{
					uint16_t cid = (uint16_t)((id - n->startId) >> (n->level * 9));
					if (cid >= 512)
						return nullptr;
					if (n->child[cid] == nullptr)
						return nullptr;
					if (n->level == 0)
					{
						tableMetaWrap* m = static_cast<tableMetaWrap*>(n->child[cid]);
						assert(tableMeta::tableID(m->meta->m_id) == id);
						if (m->meta->m_id < tableId)
							return nullptr;
						for (; m != nullptr;)
						{
							if (m->meta->m_id == tableId)
								return m->meta;
							m = m->prev;

						}
						return nullptr;
					}
					else
						n = static_cast<node*>(n->child[cid]);
				}
				else
				{
					abort();//not support now
				}
			}

		}

	};
}
