#pragma once
#include <vector>
#include "util/status.h"
#include "util/sparsepp/spp.h"
namespace KVDB {
	struct row;
	struct version;
}
typedef spp::sparse_hash_set<KVDB::row*> rowMap;
typedef dsStatus& (*logApplyFunc) (const std::vector<KVDB::version*> &vesionList);
namespace KVDB {
	enum class TRANS_ISOLATION_LEVEL {
		READ_UNCOMMITTED,
		READ_COMMITTED,
		REPEATABLE_READ
	};
	class transaction {
	public:
		bool m_start;
		TRANS_ISOLATION_LEVEL m_level;
		rowMap m_rows;
		std::vector<version*> m_vesionList;
		version* m_begin;
		void newOperator(row* r);
		dsStatus& commit(logApplyFunc func);
		dsStatus& rollback();
	};
}