#pragma once
#include <string>
#include "message/record.h"
namespace KVDB
{
	struct columnChange {
		uint16_t columnId;
		uint32_t size;
		const char* newValue;
	};
	struct rowImage {
		uint16_t count;
		uint32_t varColumnSize;
		columnChange* columnChanges;
	};
	struct rowChange {
		const char* sql;
		RPC::RecordType type;
		rowImage columns;
		rowImage condition;
		std::string database;
		std::string table;
	};
}