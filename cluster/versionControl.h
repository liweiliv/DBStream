#pragma once
#include <atomic>
#include <mutex>
#include <map>
#include "rpc.h"
#include "memory/bufferPool.h"
#include "util/heap.h"
#include "util/timer.h"
namespace CLUSTER {


	class versionControlNode
	{
		friend class versionControl;
	protected:
		versionControlNode* m_next;
		versionControlNode* m_prev;
		bool m_isDeleted;
		uint64_t m_txnId;
	public:

	};
	struct versionHandle
	{
		uint32_t uid;
		uint64_t txnId;
		bool inTransaction;
		timer::timestamp transactionStartTime;
		timer::timestamp transactionLastTime;

	};
	class versionHandleCompare {
		inline bool operator()(versionHandle* s, versionHandle* d)
		{
			return s->txnId < d->txnId;
		}
	};
	class globalVersionControl
	{
	private:
		bufferPool* m_pool;
		uint64_t m_txnId;
		uint64_t m_minTxnId;
		std::map<uint32_t, versionHandle* > m_handles;
		std::map<uint64_t, versionHandle* > m_activeHandles;
		std::mutex m_lock;
	public:
		globalVersionControl(bufferPool* pool):m_pool(pool), m_txnId(1), m_minTxnId(1), m_handles(1024*512)
		{

		}
		bool begin(versionHandle* handle)
		{
			if (handle->inTransaction)
				return false;
			handle->inTransaction = true;
			handle->transactionStartTime.time = handle->transactionLastTime.time =  timer::getNowTimestamp();
			std::lock_guard<std::mutex> guard(m_lock);
			handle->txnId = ++m_txnId;
			if (m_activeHandles.empty())
				m_minTxnId = handle->txnId;
			m_activeHandles.insert(std::pair<uint64_t, versionHandle*>(handle->txnId, handle));
			return true;
		}
		bool commit(versionHandle* handle)
		{
			if (!handle->inTransaction)
				return false;
			handle->inTransaction = false;
			handle->transactionStartTime.time = handle->transactionLastTime.time = timer::getNowTimestamp();
			std::lock_guard<std::mutex> guard(m_lock);


		}
	};
	class versionControl
	{
	protected:
		versionControlNode* m_current;
		versionControlNode* m_first;
		versionControlNode* m_last;
		std::atomic_uint32_t m_owner;
	private:
		bool lock(uint32_t uid)
		{
			uint32_t owner;
			do {
				owner = m_owner.load(std::memory_order_relaxed);
				if (owner != 0 && owner != uid)
					return false;
			} while (!m_owner.compare_exchange_weak(owner, uid, std::memory_order_relaxed));
			return true;
		}
		bool unlock(uint32_t uid)
		{
			uint32_t owner;
			do {
				owner = m_owner.load(std::memory_order_relaxed);
				if (owner != uid)
					return false;
			} while (!m_owner.compare_exchange_weak(owner, 0, std::memory_order_relaxed));
			return true;
		}

	public:
		inline versionControlNode* get(const logIndexInfo& checkpoint)
		{
			return m_current;
		}
		inline bool rollback(uint32_t uid, const logIndexInfo& checkpoint)
		{
			if (!lock())
				return false;
			versionControlNode* last = m_last, * tmp = last;
			for (; last != nullptr && last->m_checkpoint > checkpoint; last = tmp->m_prev, tmp = last)
				delete last;
			m_last = last;
			if (m_last == nullptr)
				m_current = m_first = nullptr;
			else
				m_last->m_next = nullptr;
			return true;
		}
		inline void add(uint32_t uid, versionControlNode* node)
		{
			if (!lock())
				return false;
			if (m_current == nullptr)
				m_current = m_last = node;
			else
			{
				m_last->m_next = node;
				node->m_prev = m_last;
				m_last = node;
			}
		}
		inline void drop(uint32_t uid, const logIndexInfo& checkpoint)
		{
			if (!lock())
				return false;
			if (m_last == nullptr || m_last->m_isDeleted)
				return true;
			m_last->
		}
		inline void commit(uint32_t uid, const logIndexInfo& checkpoint)
		{
			versionControlNode* current = m_current, * tmp = current;
			for (; current != nullptr && current->m_checkpoint <= checkpoint; current = tmp->m_next, tmp = current)
				delete current;
			m_last = last;
			if (m_last == nullptr)
				m_current = nullptr;
			else
				m_last->m_next = nullptr;
		}
	};
}