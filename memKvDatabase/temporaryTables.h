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
		const META::tableMeta *joinTables[MAX_JOIN_TABLE_COUNT];
		joinColumnMap columnMap[1];
	};

	typedef spp::sparse_hash_map<uint64_t, META::tableMeta*> tableMap;
	class temporaryTables {
	private:
		tableMap m_tables;
		bool m_caseSensitive;
	public:
		META::tableMeta* createJoinTable(const joinTableInfo* info)
		{
			META::tableMeta* meta = new META::tableMeta(m_caseSensitive);

		}
	};
}