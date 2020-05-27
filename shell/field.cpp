#include "field.h"
#include "expressionOperator.h"
#include "sqlParser/operationInfo.h"
#include "thread/threadLocal.h"
#include "util/likely.h"
namespace SHELL {
	bufferPool * shellGlobalBufferPool = nullptr;
	static threadLocal<Field*> localFieldStack;
	static threadLocal<void*> localValueStack;


	inline void getStack(Field**& fieldStack, void**& valueStack)
	{
		if (unlikely(nullptr == (fieldStack = localFieldStack.get())))
			localFieldStack.set(fieldStack = new Field * [MAX_EXPRESSION_LENGTH]);
		if (unlikely(nullptr == (valueStack = localValueStack.get())))
			localValueStack.set(valueStack = new void* [MAX_EXPRESSION_LENGTH]);
	}
	inline void * getNextValueInStack(int16_t &valueStackSize,void** valueStack,int16_t &fieldStackSize,Field** fieldStack,const DATABASE_INCREASE::DMLRecord** const row)
	{
		return valueStackSize>0?valueStack[--valueStackSize]:(fieldStackSize > 0?fieldStack[--fieldStackSize]->getValue(row):(abort(),nullptr));
	}

#define pullOneValue  if(valueTypeStackSize>0)valueTypeStackSize--;else if(fieldStackSize>0)fieldStackSize--;else return false;
	void* twoArgvOperator(SQL_PARSER::OPERATOR op, void* argv)
	{
		return nullptr;
	}
	bool expressionField::checkAndGetType(Field** list, uint16_t listSize, bool logicOrMath, META::COLUMN_TYPE& type, bool& group)
	{
		int16_t  fieldStackSize = 0;
		int16_t  valueTypeStackSize = 0;
		group = false;
		for (uint16_t idx = 0; idx < listSize; idx++)
		{
			if (((uint64_t)list[idx] & DUAL_ARGV_MATH_OP_FUNC_TYPE) != 0)
			{
				pullOneValue;
				pullOneValue;
				valueTypeStackSize++;
			}
			else if (((uint64_t)list[idx] & SINGLE_ARGV_MATH_OP_FUNC_TYPE) != 0)
			{
				pullOneValue;
				valueTypeStackSize++;
			}
			else if (((uint64_t)list[idx] & DUAL_ARGV_LOGIC_OP_FUNC_TYPE) != 0)
			{
				pullOneValue;
				pullOneValue;
				valueTypeStackSize++;
			}
			else if (((uint64_t)list[idx] & SINGLE_ARGV_LOGIC_OP_FUNC_TYPE) != 0)
			{
				pullOneValue;
				valueTypeStackSize++;
			}
			else
			{
				fieldStackSize++;
				if (list[idx]->fieldType == GROUP_FUNCTION_FIELD)
					group = true;
				fieldStackSize++;
			}
		}
		if (fieldStackSize != 0 || valueTypeStackSize != 1)
			return false;
		if (((uint64_t)list[listSize - 1] & FUNC_ARGV_MASK) != 0)
			type = ((operatorFuncInfo*)(void*)(((uint64_t)list[listSize - 1]) & ~FUNC_ARGV_MASK))->returnType;
		else
			type = list[listSize - 1]->valueType;
		return true;
	}
	void expressionField::_clean(Field* field)
	{
		expressionField* exp = static_cast<expressionField*>(field);
		for (uint16_t idx = 0; idx < exp->listSize; idx++)
		{
			if (((uint64_t)exp->list[idx] & FUNC_ARGV_MASK) != 0)
				continue;
			else
			{
				exp->list[idx]->clean();
				shellGlobalBufferPool->free(exp->list[idx]);
			}
		}
		shellGlobalBufferPool->free(exp->list);
	}
	#define _getNextValueInStack getNextValueInStack(valueStackSize,valueStack,fieldStackSize,fieldStack,row)
	void* expressionField::_getValue(Field* field, const DATABASE_INCREASE::DMLRecord** const row)
	{
		const expressionField* exp = static_cast<const expressionField*>(field);
		Field** fieldStack;
		int16_t  fieldStackSize = 0;
		void** valueStack;
		int16_t  valueStackSize = 0;
		getStack(fieldStack, valueStack);
		for (uint16_t idx = 0; idx < exp->listSize; idx++)
		{
			if (((uint64_t)exp->list[idx] & DUAL_ARGV_MATH_OP_FUNC_TYPE) != 0)
			{
				operatorFuncInfo* ofi = (operatorFuncInfo*)(void*)(((uint64_t)exp->list[idx]) & ~DUAL_ARGV_MATH_OP_FUNC_TYPE);
				valueStack[valueStackSize] = ((DualArgvMathOperater)(ofi->func))(_getNextValueInStack, _getNextValueInStack);
				valueStackSize++;
			}
			else if (((uint64_t)exp->list[idx] & SINGLE_ARGV_MATH_OP_FUNC_TYPE) != 0)
			{
				operatorFuncInfo* ofi = (operatorFuncInfo*)(void*)(((uint64_t)exp->list[idx]) & ~SINGLE_ARGV_MATH_OP_FUNC_TYPE);
				valueStack[valueStackSize] = ((SingleArgvMathOperater)(ofi->func))(_getNextValueInStack);
				valueStackSize++;
			}
			else if (((uint64_t)exp->list[idx] & DUAL_ARGV_LOGIC_OP_FUNC_TYPE) != 0)
			{
				operatorFuncInfo* ofi = (operatorFuncInfo*)(void*)(((uint64_t)exp->list[idx]) & ~DUAL_ARGV_LOGIC_OP_FUNC_TYPE);
				valueStack[valueStackSize] = ((SingleArgvLogicOperater)(ofi->func))(_getNextValueInStack) ? (void*)0x01 : (void*)0x00;
				valueStackSize++;
			}
			else if (((uint64_t)exp->list[idx] & SINGLE_ARGV_LOGIC_OP_FUNC_TYPE) != 0)
			{
				operatorFuncInfo* ofi = (operatorFuncInfo*)(void*)(((uint64_t)exp->list[idx]) & ~SINGLE_ARGV_LOGIC_OP_FUNC_TYPE);
				valueStack[valueStackSize] = ((SingleArgvLogicOperater)(ofi->func))(_getNextValueInStack) ? (void*)0x01 : (void*)0x00;
				valueStackSize++;
			}
			else
			{
				fieldStack[fieldStackSize] = exp->list[idx];
				fieldStackSize++;
			}
		}
		assert(fieldStackSize == 0);
		assert(valueStackSize == 1);
		return valueStack[0];
	}
}
