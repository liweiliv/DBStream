#pragma once
#include <string>
#include <mutex>
#include "meta/metaData.h"
#include "tbb/concurrent_hash_map.h"
#include "util/status.h"
#include "util/likely.h"
#include "errorCode.h"
#include "row.h"
namespace KVDB
{

	class tableInterface {
	protected:
		std::string m_name;
		META::tableMeta* m_meta;
		std::mutex m_lock;
	public:
		tableInterface(const char* name, META::tableMeta* meta) :m_name(name), m_meta(meta) {}
		virtual ~tableInterface() {}
		virtual dsStatus& select(bufferPool* pool, clientHandle* client, const rowChange* condition, const version *& result) = 0;
		virtual dsStatus& insert(bufferPool* pool, clientHandle* client, const rowChange* rowChange) = 0;
		virtual dsStatus& update(bufferPool* pool, clientHandle* client, const rowChange* change, const rowChange* condition) = 0;
		virtual dsStatus& drop(bufferPool* pool, clientHandle* client, const rowChange* condition) = 0;
		const std::string& getName() { return m_name; }
	};
	template<class C>
	class hashWrap {
	public:
		inline bool equal(const C& s, const C& d) const
		{
			return s == d;
		}
		inline size_t hash(const C& s) const
		{
			std::hash<C> h;
			return h(s);
		}
	};
	template<>
	inline size_t hashWrap<META::unionKey>::hash(const META::unionKey& s)const
	{
		return s.hash();
	}
	template<>
	inline size_t hashWrap<META::binaryType>::hash(const META::binaryType& s)const
	{
		return s.hash();
	}

	template<class T>
	class table :public tableInterface {
	private:


		tbb::concurrent_hash_map<T, row*, hashWrap<T> > rowMap;
	private:
		inline void destroyKey(T& key, bool afterCopy) {}
		inline void copyKeyValueFromRecord(bufferPool* pool, T& key) {}
		inline void createKey(bufferPool* pool, const DATABASE_INCREASE::DMLRecord* record, T& key)
		{
			key = *(const T*)record->column(m_meta->m_primaryKey->columnInfo[0].columnId);
		}
		inline dsStatus& createKeyFromCondition(bufferPool* pool, T& key, const rowChange* condition)
		{
			if (condition->count != 1 || condition->columnChanges[0].columnId != m_meta->m_primaryKey->columnInfo[0].columnId)
				dsFailedAndLogIt(errorCode::KEY_COLUMN_NOT_MATCH, "condition must be primary key column", WARNING);
			key = *(const T*)(condition->columnChanges->newValue);
			dsOk();
		}
	public:
		table(const char* name, META::tableMeta* m_meta):tableInterface(name,m_meta){};
		~table()
		{
			clear();
		}
		void clear()
		{
			for (typename tbb::concurrent_hash_map<T, row*, hashWrap<T> >::iterator iter = rowMap.begin(); iter != rowMap.end(); iter++)
			{
				T* key = (T*)&iter->first;
				destroyKey(*key, true);
				if (iter->second != nullptr)
					row::destroy(iter->second);
			}
			rowMap.clear();
		}
		dsStatus& select(bufferPool* pool, clientHandle* client, const rowChange* condition, const version*& result)
		{
			T key;
			dsReturnIfFailed(createKeyFromCondition(pool, key, condition));
			typename tbb::concurrent_hash_map<T, row*, hashWrap<T> >::const_accessor accessor;
			if (!rowMap.find(accessor, key))
			{
				destroyKey(key, false);
				dsFailedAndLogIt(errorCode::ROW_NOT_EXIST, "select failed for row not exist", WARNING);
			}
			if (unlikely(!dsCheck(accessor->second->select(pool, client, result))))
			{
				destroyKey(key, false);
				dsReturn(getLocalStatus());
			}
			destroyKey(key, false);
			dsOk();
		}
		dsStatus& insert(bufferPool* pool, clientHandle* client, const rowChange* change)
		{
			version* v;
			dsReturnIfFailed(version::allocForInsert(v, pool, m_meta, change));
			T key;
			createKey(pool, &v->data, key);
			typename tbb::concurrent_hash_map<T, row*, hashWrap<T> >::accessor accessor;
			if (!rowMap.find(accessor, key))
			{
				row* r = (row*)pool->alloc(sizeof(row));
				r->init();
				if (unlikely(!dsCheck(r->insert(pool, client, v))))
				{
					destroyKey(key, false);
					dsReturn(getLocalStatus());
				}
				copyKeyValueFromRecord(pool, key);
				if (rowMap.insert(accessor, std::pair<T, row*>(key, r)))
					dsOk();
			}
			if (unlikely(!dsCheck(accessor->second->insert(pool, client, v))))
			{
				destroyKey(key, false);
				dsReturn(getLocalStatus());
			}
			dsOk();
		}

		dsStatus& update(bufferPool* pool, clientHandle* client, const rowChange* change, const rowChange* condition)
		{
			T key;
			dsReturnIfFailed(createKeyFromCondition(pool, key, condition));
			typename tbb::concurrent_hash_map<T, row*, hashWrap<T> >::const_accessor accessor;
			if (!rowMap.find(accessor, key))
			{
				destroyKey(key, false);
				dsFailedAndLogIt(errorCode::ROW_NOT_EXIST, "update failed for row not exist", WARNING);
			}
			if (unlikely(!dsCheck((accessor->second)->update(pool, client, change))))
			{
				destroyKey(key, false);
				dsReturn(getLocalStatus());
			}
			destroyKey(key, false);
			dsOk();
		}

		dsStatus& drop(bufferPool* pool, clientHandle* client, const rowChange* condition)
		{
			T key;
			dsReturnIfFailed(createKeyFromCondition(pool, key, condition));
			typename tbb::concurrent_hash_map<T, row*, hashWrap<T> >::const_accessor accessor;
			if (!rowMap.find(accessor, key))
			{
				destroyKey(key, false);
				dsFailedAndLogIt(errorCode::ROW_NOT_EXIST, "delete failed for row not exist", WARNING);
			}
			if (unlikely(!dsCheck(accessor->second->drop(pool, client))))
			{
				destroyKey(key, false);
				dsReturn(getLocalStatus());
			}
			destroyKey(key, false);
			dsOk();
		}
	};

	template<>
	inline void table<META::unionKey>::createKey(bufferPool* pool, const DATABASE_INCREASE::DMLRecord* record, META::unionKey& key)
	{
		key.meta = m_meta->m_primaryKey;
		uint16_t size = META::unionKey::memSize(record, key.meta, false);
		key.key = (char*)pool->alloc(size + (key.meta->fixed ? 0 : sizeof(uint16_t)));
		META::unionKey::initKey((char*)key.key, size, key.meta, record, false);
	}

	template<>
	inline void table<META::binaryType>::createKey(bufferPool* pool, const DATABASE_INCREASE::DMLRecord* record, META::binaryType& key)
	{
		uint16_t id = m_meta->m_primaryKey->columnInfo[0].columnId;
		key.size = record->varColumnSize(id);
		key.data = record->column(id);
	}

	template<>
	inline void table<META::binaryType>::copyKeyValueFromRecord(bufferPool* pool, META::binaryType& key)
	{
		if (key.data == nullptr || key.size == 0)
			return;
		char* data = (char*)pool->alloc(key.size);
		memcpy(data, key.data, key.size);
		key.data = data;
	}

	template<>
	inline void table<META::unionKey>::destroyKey(META::unionKey& key, bool afterCopy)
	{
		bufferPool::free((char*)key.key);
	}

	template<>
	inline void table<META::binaryType>::destroyKey(META::binaryType& key, bool afterCopy)
	{
		if (afterCopy)
			bufferPool::free((char*)key.data);
	}

	template<>
	inline dsStatus& table<META::binaryType>::createKeyFromCondition(bufferPool* pool, META::binaryType& key, const rowChange* condition)
	{
		if (condition->count != 1 || condition->columnChanges[0].columnId != m_meta->m_primaryKey->columnInfo[0].columnId)
			dsFailedAndLogIt(errorCode::KEY_COLUMN_NOT_MATCH, "condition must be primary key column", WARNING);
		key.data = condition->columnChanges[0].newValue;
		key.size = condition->columnChanges[0].size;
		dsOk();
	}

	template<>
	inline dsStatus& table<META::unionKey>::createKeyFromCondition(bufferPool* pool, META::unionKey& key, const rowChange* condition)
	{
		if (condition->count != m_meta->m_primaryKey->columnCount)
			dsFailedAndLogIt(errorCode::KEY_COLUMN_NOT_MATCH, "condition must have all primary key column", WARNING);
		uint16_t columnIds[1024];
		for (int i = 0; i < m_meta->m_primaryKey->columnCount; i++)
			columnIds[m_meta->m_primaryKey->columnInfo[i].columnId] = 0xffffu;
		for (int i = 0; i < condition->count; i++)
		{
			if (!m_meta->getColumn(condition->columnChanges[i].columnId)->m_isPrimary)
				dsFailedAndLogIt(errorCode::KEY_COLUMN_NOT_MATCH, "condition must be primary key column", WARNING);
			columnIds[condition->columnChanges[i].columnId] = i;
		}
		key.key = (char*)pool->alloc(m_meta->m_primaryKey->size + condition->varColumnSize);
		char* ptr = ((char*)key.key) + (m_meta->m_primaryKey->fixed ? 0 : sizeof(uint16_t));
		for (int i = 0; i < m_meta->m_primaryKey->columnCount; i++)
		{
			const META::uniqueKeyTypePair& columnMeta = m_meta->m_primaryKey->columnInfo[i];
			uint16_t cid = columnIds[columnMeta.columnId];
			if (cid == 0xffffu)
			{
				bufferPool::free((char*)key.key);
				dsFailedAndLogIt(errorCode::KEY_COLUMN_NOT_MATCH, "condition must have all primary key column", WARNING);
			}
			if (META::columnInfos[static_cast<int>(columnMeta.type)].fixed)
			{
				memcpy(ptr, condition->columnChanges[cid].newValue, META::columnInfos[columnMeta.type].columnTypeSize);
				ptr += META::columnInfos[columnMeta.type].columnTypeSize;
			}
			else
			{
				*(uint16_t*)ptr = condition->columnChanges[cid].size;
				memcpy(ptr + sizeof(uint16_t), condition->columnChanges[cid].newValue, *(uint16_t*)ptr);
				ptr += sizeof(uint16_t) + *(uint16_t*)ptr;
			}
		}
		*(uint16_t*)ptr = ptr - key.key;
		dsOk();
	}
}
