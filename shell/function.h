#pragma once
#include <stdint.h>
#include "message/record.h"
namespace SHELL
{
	class Field;
	class function
	{
	public:
		uint8_t argvCount;
		META::COLUMN_TYPE returnValueType;
		bool rowOrGroup;
		function(uint8_t argvCount, META::COLUMN_TYPE returnValueType, bool rowOrGroup) :argvCount(argvCount), returnValueType(returnValueType), rowOrGroup(rowOrGroup) {}
	};
	class rowFunction :public function
	{
	public:
		virtual void* exec(Field** const argvs, const RPC::DMLRecord** const row) const = 0;
		rowFunction(uint8_t argvCount, META::COLUMN_TYPE returnValueType) :function(argvCount, returnValueType, true) {}
		virtual ~rowFunction() {}
	};
	class groupFunction :public function {
	public:
		virtual void exec(Field** const argvs, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const = 0;
		virtual void* finalValueFunc(void* historyValue, uint32_t count) const = 0;
		groupFunction(uint8_t argvCount, META::COLUMN_TYPE returnValueType) :function(argvCount, returnValueType, false) {}
		virtual ~groupFunction() {}
	};
	void initFunction();
	const rowFunction* getRowFunction(const char* name, const char* argvTypes);
	const groupFunction* getGroupFunction(const char* name, const char* argvTypes);
}
