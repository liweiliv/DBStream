#pragma once
#include "message/record.h"
namespace REPLICATOR {
	struct transaction;
	struct replicatorRecord;
	enum blockListNodeType {
		NORMAL,
		HEAD,
		UNUSED
	};
	struct blockListNode {
		replicatorRecord* record;
		union {
			blockListNode* prev;
			uint64_t value;
		};
		blockListNodeType type;
	};
	struct replicatorRecord {

		DATABASE_INCREASE::record* record;
		replicatorRecord* nextInTrans;
		replicatorRecord* nextWaitForDDL;
		replicatorRecord* mergePrev;
		replicatorRecord* mergeNext;
		void* tableInfo; 
		transaction* trans;

		uint16_t prevRecordCount;
		blockListNode blocks[1];
	};
	struct transaction {
		replicatorRecord* firstRecord;
		replicatorRecord* lastRecord;

		uint32_t recordCount;
		uint32_t blockCount;
		transaction* next;
		transaction* prev;

		transaction* mergeNext;

		bool committed;
		transaction* preCommitNext;
		transaction* preCommitPrev;

	};
}
