#pragma once
#include <vector>
#include "util/status.h"
#include "util/sparsepp/spp.h"
namespace KVDB {
	struct row;
	struct version;
	struct tableInterface;
}
typedef spp::sparse_hash_set<KVDB::row*> rowMap;
typedef spp::sparse_hash_set<KVDB::tableInterface*> tableMap;
namespace KVDB {
	enum class TRANS_ISOLATION_LEVEL {
		READ_UNCOMMITTED,
		READ_COMMITTED,
		REPEATABLE_READ
	};
	class transaction {
	public:
		bool m_start;
		bool m_autoCommit;
		TRANS_ISOLATION_LEVEL m_level;
		rowMap m_rows;
		tableMap m_tables;
		version* m_redoListHead;
		version* m_redoListTail;

		version* m_begin;
		void newOperator(row* r);
		void clear();
		dsStatus& commit();
		dsStatus& rollback();
	};
}