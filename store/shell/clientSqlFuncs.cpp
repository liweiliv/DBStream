#include "meta/metaChangeInfo.h"
#include "meta/metaDataCollection.h"
#include "meta/charset.h"
#include "sqlParser/sqlParserHandle.h"
#include  "util/winDll.h"
#include "sqlInfo.h"
#include "field.h"
#include "function.h"
#include "expressionOperator.h"
namespace STORE {
	namespace SHELL {
		META::metaDataCollection* metaCenter;
		threadLocal<uint8_t> typeListBuffer;
		static inline uint8_t* getTypeListBuf()
		{
			uint8_t* buffer = typeListBuffer.get();
			if (unlikely(buffer == nullptr))
				typeListBuffer.set(buffer = new uint8_t[1024]);
			return buffer;
		}
#define GET_SELECT_TABLE static_
		inline void createSelectSqlInfo(SQL_PARSER::handle* h)
		{
			assert(h->userData == nullptr);
			selectSqlInfo* sql = (selectSqlInfo*)shellGlobalBufferPool.alloc(sizeof(selectSqlInfo));
			sql->init();
			h->userData = sql;
		}
		inline selectSqlInfo* getSelectSqlInfoFromHandle(SQL_PARSER::handle* h)
		{
			assert(h->userData != nullptr && static_cast<sqlBasicInfo*>(h->userData)->type == SELECT_SQL);
			return static_cast<selectSqlInfo*>(h->userData);
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectTableName(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::TABLE_NAME_TYPE);
			sql->table = static_cast<SQL_PARSER::SQLTableNameValue*>(value);
			return SQL_PARSER::OK;
		}
		static inline rawField* SQLValue2String(SQL_PARSER::SQLValue* value)
		{
			rawField *field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
			varLenValue * v = (varLenValue*)shellGlobalBufferPool.alloc(sizeof(varLenValue));
			v->size = static_cast<SQL_PARSER::SQLStringValue*>(value)->value.size();
			v->value = static_cast<SQL_PARSER::SQLStringValue*>(value)->value.c_str();
			v->alloced = false;
			field->init(v, META::T_STRING);
			return field;
		}
		static inline rawField* SQLValue2Int(SQL_PARSER::SQLValue* value)
		{
			rawField* field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
			field->init(*(void**) & static_cast<SQL_PARSER::SQLIntNumberValue*>(value)->number, META::T_INT64);
			return field;
		}
		static inline rawField* SQLValue2Float(SQL_PARSER::SQLValue* value)
		{
			rawField* field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
			field->init(*(void**) & static_cast<SQL_PARSER::SQLFloatNumberValue*>(value)->number, META::T_DOUBLE);
			return field;
		}
		static inline columnFiled* SQLValue2Column(SQL_PARSER::SQLValue* value)
		{
			return nullptr;//todo
		}
		static inline Field* createFieldFromSqlValue(SQL_PARSER::SQLValue* value);
		static inline Field* SQLValue2FunctionField(SQL_PARSER::SQLValue* value)
		{
			SQL_PARSER::SQLFunctionValue* funcValue = static_cast<SQL_PARSER::SQLFunctionValue*>(value);
			uint8_t* argvTypeList = getTypeListBuf();
			uint16_t argvListSize = 0;
			//Field* field = (Field*)shellGlobalBufferPool.alloc(sizeof(Field));
			Field ** argvs = (Field**)shellGlobalBufferPool.alloc(sizeof(Field*)* funcValue->argvs.size());
			for (decltype(funcValue->argvs)::const_iterator iter = funcValue->argvs.begin(); iter != funcValue->argvs.end(); iter++)
			{
				argvs[argvListSize] = createFieldFromSqlValue(*iter);
				argvTypeList[argvListSize] = argvs[argvListSize]->valueType;
				argvListSize++;
			}
			argvTypeList[argvListSize] = '\0';
			const rowFunction* rowFunc = getRowFunction(funcValue->funcName.c_str(), (char*)argvTypeList);
			if (rowFunc != nullptr)
			{
				rowFunctionFiled* field = (rowFunctionFiled*)shellGlobalBufferPool.alloc(sizeof(rowFunctionFiled));
				field->init(argvs, rowFunc);
				return field;
			}
			const groupFunction* groupFunc = getGroupFunction(funcValue->funcName.c_str(), (char*)argvTypeList);
			if (groupFunc != nullptr)
			{
				groupFunctionFiled* field = (groupFunctionFiled*)shellGlobalBufferPool.alloc(sizeof(groupFunctionFiled));
				field->init(argvs, groupFunc);
				return field;
			}
			return nullptr;
		}

		static inline expressionField* SQLValue2ExpressionField(SQL_PARSER::SQLValue* value)
		{
			assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
			SQL_PARSER::SQLExpressionValue* expValue = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
			if (expValue->count <=1)
				return nullptr;
			Field** fields = (Field * *)shellGlobalBufferPool.alloc(sizeof(Field*) * expValue->count);
			uint8_t* typeBuffer = getTypeListBuf();
			uint16_t typeBufferSize = 0;
			bool group = false;
			for (uint16_t idx = 0; idx < expValue->count; idx++)
			{
				if (expValue->valueStack[idx]->type == SQL_PARSER::OPERATOR_TYPE)
				{
					if (typeBufferSize >= 2)
					{
						SQL_PARSER::SQLOperatorValue* op = static_cast<SQL_PARSER::SQLOperatorValue*>(expValue->valueStack[idx]);
						operatorFuncInfo* func = getDualArgvMathFunc(op->opera, typeBuffer[typeBufferSize - 1], typeBufferSize - 2);
						if (func != nullptr)
						{
							typeBufferSize -= 2;
							typeBuffer[typeBufferSize++] = func->returnType;
							fields[idx] = (Field*)(void*)(((uint64_t)func) | DUAL_ARGV_MATH_OP_FUNC_TYPE);
							continue;
						}
						func = getDualArgvLogicFunc(op->opera, typeBuffer[typeBufferSize - 1], typeBufferSize - 2);
						if (func != nullptr)
						{
							typeBufferSize -= 2;
							typeBuffer[typeBufferSize++] = func->returnType;
							fields[idx] = (Field*)(void*)(((uint64_t)func) | DUAL_ARGV_LOGIC_OP_FUNC_TYPE);
							continue;
						}
					}
					if (typeBufferSize >= 1)
					{
						SQL_PARSER::SQLOperatorValue* op = static_cast<SQL_PARSER::SQLOperatorValue*>(expValue->valueStack[idx]);
						operatorFuncInfo* func = getSingleArgvMathFunc(op->opera, typeBuffer[typeBufferSize - 1]);
						if (func != nullptr)
						{
							typeBuffer[typeBufferSize - 1] = func->returnType;
							fields[idx] = (Field*)(void*)(((uint64_t)func) | SINGLE_ARGV_MATH_OP_FUNC_TYPE);
							continue;
						}
						func = getSingleArgvLogicFunc(op->opera, typeBuffer[typeBufferSize - 1]);
						if (func != nullptr)
						{
							typeBuffer[typeBufferSize - 1] = func->returnType;
							fields[idx] = (Field*)(void*)(((uint64_t)func) | SINGLE_ARGV_LOGIC_OP_FUNC_TYPE);
							continue;
						}
					}
					shellGlobalBufferPool.free(fields);
					return nullptr;
				}
				else
				{
					fields[idx] = createFieldFromSqlValue(expValue->valueStack[idx]);
					if (fields[idx]->valueType == GROUP_FUNCTION_FIELD)
						group = true;
					typeBuffer[typeBufferSize++] = fields[idx]->valueType;
				}
			}
			if (typeBufferSize != 1)
			{
				shellGlobalBufferPool.free(fields);
				return nullptr;
			}

			expressionField* exp = (expressionField*)shellGlobalBufferPool.alloc(sizeof(expressionField));
			exp->init(fields, expValue->count, false, group, typeBuffer[0]);
			return exp;
		}
		static inline Field* createFieldFromSqlValue(SQL_PARSER::SQLValue* value)
		{
			switch (value->type)
			{
			case SQL_PARSER::INT_NUMBER_TYPE:
				return SQLValue2Int(value);
			case SQL_PARSER::STRING_TYPE:
				return SQLValue2String(value);
			case SQL_PARSER::COLUMN_NAME_TYPE:
				return SQLValue2Column(value);
			case SQL_PARSER::FUNCTION_TYPE:
				return SQLValue2FunctionField(value);
			case SQL_PARSER::EXPRESSION_TYPE:
				return SQLValue2ExpressionField(value);
			case SQL_PARSER::FLOAT_NUMBER_TYPE:
				return SQLValue2Float(value);
			default:
				return nullptr;
			}
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectSqlType(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			createSelectSqlInfo(h);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectFunctionField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::FUNCTION_TYPE);
			Field* field = SQLValue2FunctionField(value);
			if(field==nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(field);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectExpressionField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
			SQL_PARSER::SQLExpressionValue* expValue = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
			expressionField* exp = SQLValue2ExpressionField(value);
			if (exp == nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(exp);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectColumnField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::COLUMN_NAME_TYPE);
			Field* field = SQLValue2Column(value);
			if (field == nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(field);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectIntField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::INT_NUMBER_TYPE);
			Field* field = SQLValue2Int(value);
			if (field == nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(field);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectFloatField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::FLOAT_NUMBER_TYPE);
			Field* field = SQLValue2Float(value);
			if (field == nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(field);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectStringField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::STRING_TYPE);
			Field* field = SQLValue2String(value);
			if (field == nullptr)
				return SQL_PARSER::INVALID;
			sql->selectFields.add(field);
			return SQL_PARSER::OK;
		}

		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectWhereField(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
			SQL_PARSER::SQLExpressionValue* expValue = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
			expressionField* exp = SQLValue2ExpressionField(value);
			if (exp == nullptr)
				return SQL_PARSER::INVALID;
			if (static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::T_BOOL)
			{
				exp->clean();
				shellGlobalBufferPool.free(exp);
				return SQL_PARSER::INVALID;
			}
			sql->selectFields.add(exp);
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinTable(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::TABLE_NAME_TYPE);
			sql->joinedTables.add(static_cast<SQL_PARSER::SQLTableNameValue*>(value));
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectLeftJoin(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			sql->joinType = LEFT_JOIN;
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectRightJoin(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			sql->joinType = RIGHT_JOIN;
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectInnerJoin(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			sql->joinType = INNER_JOIN;
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinOnCondition(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			if(sql->joinedUsingColumns.size!=0)
				return SQL_PARSER::INVALID;
			assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
			SQL_PARSER::SQLExpressionValue* expValue = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
			expressionField* exp = SQLValue2ExpressionField(value);
			if (exp == nullptr)
				return SQL_PARSER::INVALID;
			if (static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::T_BOOL)
			{
				exp->clean();
				shellGlobalBufferPool.free(exp);
				return SQL_PARSER::INVALID;
			}
			sql->joinedCondition = exp;
			return SQL_PARSER::OK;
		}
		extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinUsingColumn(SQL_PARSER::handle* h, SQL_PARSER::SQLValue* value)
		{
			selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
			assert(value->type == SQL_PARSER::STRING_TYPE);
			if(sql->joinedCondition!=nullptr)
				return SQL_PARSER::INVALID;
			sql->joinedUsingColumns.add(static_cast<SQL_PARSER::SQLStringValue*>(value)->value.c_str());
			return SQL_PARSER::OK;
		}
		
	}
}