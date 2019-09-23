#pragma once
#include "messageWrap.h"
namespace REPLICATOR {
	struct rowModle {
		META::tableMeta * table;
		int8_t opType;
		uint8_t updateColumnsbitMap[1];
	};
	struct transactionModle
	{

	};
}