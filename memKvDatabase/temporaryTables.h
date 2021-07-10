#pragma once
#include <stdint.h>
#include "thread/shared_mutex.h"
#include "meta/metaData.h"
#include "util/status.h"
#include "util/sparsepp/spp.h"
#include "clientHandle.h"
namespace KVDB {
	constexpr static int MAX_JOIN_TABLE_COUNT = 32;
	struct joinColumnMap {
		uint16_t tableIdx;
		uint16_t columnId;
	};
	struct joinTableInfo {
		uint16_t tableCount;
		const META::TableMeta *joinTables[MAX_JOIN_TABLE_COUNT];
		joinColumnMap columnMap[1];
	};

	typedef spp::sparse_hash_map<uint64_t, META::TableMeta*> tableMap;
	class temporaryTables {
	private:
		tableMap m_tables;
		bool m_caseSensitive;
	public:
		META::TableMeta* createJoinTable(const joinTableInfo* info)
		{
			META::TableMeta* meta = new META::TableMeta(m_caseSensitive);

		}
	};
}