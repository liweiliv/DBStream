#include "field.h"
#include "sqlParser/operationInfo.h"
#include "util/threadLocal.h"
#include "util/likely.h"
namespace STORE {
	namespace SHELL {
		bufferPool shellGlobalBufferPool;
		static threadLocal<field*> localFieldStack;
		static threadLocal<void*> localValueStack;


#define DUAL_ARGV_MATH_OP_FUNC_TYPE 0x8000000000000000ULL
#define SINGLE_ARGV_MATH_OP_FUNC_TYPE 0x4000000000000000ULL
#define DUAL_ARGV_LOGIC_OP_FUNC_TYPE 0x2000000000000000ULL
#define SINGLE_ARGV_LOGIC_OP_FUNC_TYPE 0x1000000000000000ULL
		inline void getStack(field**& fieldStack, void**& valueStack)
		{
			if (unlikely(nullptr == (fieldStack = localFieldStack.get())))
				localFieldStack.set(fieldStack = new field * [MAX_EXPRESSION_LENGTH]);
			if (unlikely(nullptr == (valueStack = localValueStack.get())))
				localValueStack.set(valueStack = new void* [MAX_EXPRESSION_LENGTH]);
		}


		void* twoArgvOperator(SQL_PARSER::OPERATOR op, void* argv)
		{
			return nullptr;
		}
		void* expressionField::getValue(const DATABASE_INCREASE::DMLRecord* row)const
		{
			field** fieldStack;
			int16_t  fieldStackSize = 0;
			void** valueStack;
			int16_t  valueStackSize = 0;
			getStack(fieldStack, valueStack);
			for (uint16_t idx = 0; idx < listSize; idx++)
			{
				if (((uint64_t)list[idx] & DUAL_ARGV_MATH_OP_FUNC_TYPE) !=0)
				{
					fieldStack[fieldStackSize++] = list[idx];
				}
				else
				{
					SQL_PARSER::OPERATOR op = (SQL_PARSER::OPERATOR)(uint64_t)(void*)list[idx];
					if (SQL_PARSER::operationInfos[op].optType == SQL_PARSER::MATHS && logicOrMath)
					{
						return nullptr;//todo
					}
					if (!SQL_PARSER::operationInfos[op].hasLeftValues)
					{

					}
				}
			}
		}
	}
}