#pragma once
#include "../message/record.h"
namespace REPLICATOR {
	struct transaction;
	struct replicatorRecord;
	struct blockListNode {
		replicatorRecord* record;
		blockListNode* prev;
	};
	struct replicatorRecord {
		DATABASE_INCREASE::record* record;
		replicatorRecord* nextInTrans;
		transaction* trans;
		uint16_t prevRecordCount;
		blockListNode blocks[1];
	};
	struct transaction {
		replicatorRecord* firstRecord;
		replicatorRecord* lastRecord;

		uint32_t recordCount;
		uint32_t blockCount;
		bool finished;
		transaction* next;
		transaction* prev;
	};
}

