#pragma once
#include <stdint.h>
#include "message/record.h"
namespace STORE
{
	namespace SHELL
	{
		class field;
		class function
		{
		public:
			uint8_t argvCount;
			uint8_t returnValueType;
			virtual void* exec( field** const argvs,const DATABASE_INCREASE::DMLRecord * row) const = 0;
			function(uint8_t argvCount, uint8_t returnValueType) :argvCount(argvCount), returnValueType(returnValueType){}
			virtual ~function(){}
		};
		class groupFunction {
		public:
			uint8_t argvType;
			uint8_t valueType;
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const = 0;
			virtual void* finalValueFunc(void* historyValue, uint32_t count) const  = 0;
			groupFunction(uint8_t argvType, uint8_t valueType) :argvType(argvType), valueType(valueType) {}
			virtual ~groupFunction() {}
		};
		void initFunction();
		const function* getFunction(const char* name, const char* argvTypes);
		const groupFunction* getGroupFunction(const char* name, const char* argvTypes);
	}

}
