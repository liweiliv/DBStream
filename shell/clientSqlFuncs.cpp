#include "meta/metaChangeInfo.h"
#include "meta/metaDataCollection.h"
#include "meta/charset.h"
#include "sqlParser/sqlParserHandle.h"
#include "util/winDll.h"
#include "sqlInfo.h"
#include "field.h"
#include "function.h"
#include "expressionOperator.h"
#include "thread/threadLocal.h"
namespace SHELL {
	static threadLocal<SQL_PARSER::SQLValue*> selectValues;
	static threadLocal<char*> selectValueAlias;
	static inline SQL_PARSER::SQLValue** getSelectValues()
	{
		SQL_PARSER::SQLValue** values;
		if (unlikely(nullptr == (values = selectValues.get())))
		{
			selectValues.set(values = new SQL_PARSER::SQLValue * [1024]);
			selectValueAlias.set(new char* [1024]);
		}
		return values;
	}
	META::metaDataCollection* shellMetaCenter;
	threadLocal<uint8_t> typeListBuffer;
	static inline Field* createFieldFromSqlValue(SQL_PARSER::SQLValue* value, selectSqlInfo* select, bool isSelectedColumn);
	static inline uint8_t* getTypeListBuf()
	{
		uint8_t* buffer = typeListBuffer.get();
		if (unlikely(buffer == nullptr))
			typeListBuffer.set(buffer = new uint8_t[1024]);
		return buffer;
	}
	static inline const META::tableMeta* getMeta(SQL_PARSER::handle* h, const SQL_PARSER::SQLTableNameValue* table)
	{
		const char* database = table->database.empty() ? (h->dbName.empty() ? nullptr : h->dbName.c_str()) : table->database.c_str();
		if (database == nullptr)
			return nullptr;
		return shellMetaCenter->get(database, table->table.c_str());
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
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectTableName(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::TABLE_NAME_TYPE);
		const char* database = static_cast<SQL_PARSER::SQLTableNameValue*>(value)->database.empty() ? (h->dbName.empty() ? nullptr : h->dbName.c_str()) : static_cast<SQL_PARSER::SQLTableNameValue*>(value)->database.c_str();
		if (database == nullptr)
			return SQL_PARSER::INVALID;
		if (nullptr == (sql->table = shellMetaCenter->get(database, static_cast<SQL_PARSER::SQLTableNameValue*>(value)->table.c_str())))
			return SQL_PARSER::INVALID;
		return SQL_PARSER::OK;
	}
	static inline rawField* SQLValue2String(SQL_PARSER::SQLValue* value)
	{
		rawField* field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
		varLenValue* v = (varLenValue*)shellGlobalBufferPool.alloc(sizeof(varLenValue));
		v->size = static_cast<SQL_PARSER::SQLStringValue*>(value)->size;
		v->value = static_cast<SQL_PARSER::SQLStringValue*>(value)->value;
		v->alloced = false;
		field->init(v, META::COLUMN_TYPE::T_STRING);
		return field;
	}
	static inline rawField* SQLValue2Int(SQL_PARSER::SQLValue* value)
	{
		rawField* field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
		field->init(*(void**) & static_cast<SQL_PARSER::SQLIntNumberValue*>(value)->number, META::COLUMN_TYPE::T_INT64);
		return field;
	}
	static inline rawField* SQLValue2Float(SQL_PARSER::SQLValue* value)
	{
		rawField* field = (rawField*)shellGlobalBufferPool.alloc(sizeof(rawField));
		field->init(*(void**) & static_cast<SQL_PARSER::SQLFloatNumberValue*>(value)->number, META::COLUMN_TYPE::T_DOUBLE);
		return field;
	}
	static inline Field* SQLValue2Column(SQL_PARSER::SQLValue* value, selectSqlInfo* sql, bool isSelectedColumn)
	{
		SQL_PARSER::SQLColumnNameValue* columnValue = static_cast<SQL_PARSER::SQLColumnNameValue*>(value);
		const META::columnMeta* columnMeta;
		columnFiled* field = nullptr;
		if (columnValue->database.empty() && columnValue->table.empty())
		{
			char** alias = selectValueAlias.get();
			for (int idx = 0; idx < sql->selectFieldCount; idx++)
			{
				if (alias[idx] != nullptr && strcmp(alias[idx], columnValue->columnName.c_str()) == 0)
				{
					if (sql->selectFields.size <= (uint32_t)idx)
						return nullptr;
					sql->selectFields.list[idx]->ref++;
					return sql->selectFields.list[idx];
				}
			}
		}
		int tableId = sql->getTable(columnValue->database.empty() ? nullptr : columnValue->database.c_str(), columnValue->table.empty() ? nullptr : columnValue->table.c_str());
		if (tableId < 0)
			return nullptr;

		if (tableId == 0)
		{
			if (nullptr == (columnMeta = sql->table->getColumn(columnValue->columnName.c_str())))
				return nullptr;
			if (isSelectedColumn && sql->groupByColumnNames.size > 0 && !sql->isGroupColumn(columnValue->columnName.c_str(), META::tableMeta::tableID(sql->table->m_id)))
				return nullptr;

			field = (columnFiled*)shellGlobalBufferPool.alloc(sizeof(columnFiled*));
			field->init(sql->table, columnMeta, tableId);
		}
		else
		{
			if (nullptr == (columnMeta = sql->joinedTables.list[tableId]->getColumn(columnValue->columnName.c_str())))
				return nullptr;
			if (isSelectedColumn && sql->groupByColumnNames.size > 0 && !sql->isGroupColumn(columnValue->columnName.c_str(), META::tableMeta::tableID(sql->joinedTables.list[tableId]->m_id)))
				return nullptr;
			field = (columnFiled*)shellGlobalBufferPool.alloc(sizeof(columnFiled*));
			field->init(sql->joinedTables.list[tableId], columnMeta, tableId);
		}
		return field;
	}
	static inline Field* SQLValue2FunctionField(SQL_PARSER::SQLValue* value, selectSqlInfo* sql, bool isSelectedColumn)
	{
		SQL_PARSER::SQLFunctionValue* funcValue = static_cast<SQL_PARSER::SQLFunctionValue*>(value);
		uint8_t* argvTypeList = getTypeListBuf();
		uint16_t argvListSize = 0;
		Field** argvs = (Field**)shellGlobalBufferPool.alloc(sizeof(Field*) * funcValue->argvs.size());
		for (decltype(funcValue->argvs)::const_iterator iter = funcValue->argvs.begin(); iter != funcValue->argvs.end(); iter++)
		{
			argvs[argvListSize] = createFieldFromSqlValue(*iter, sql, isSelectedColumn);
			if (argvs[argvListSize] == nullptr)
			{
				for (int idx = 0; idx <= argvListSize; idx++)
					delete argvs[idx];
				delete[]argvs;
				return nullptr;
			}
			argvTypeList[argvListSize] = static_cast<uint8_t>(argvs[argvListSize]->valueType);
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

	static inline expressionField* SQLValue2ExpressionField(SQL_PARSER::SQLValue* value, selectSqlInfo* sql, bool isSelectedColumn)
	{
		assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
		SQL_PARSER::SQLExpressionValue* expValue = static_cast<SQL_PARSER::SQLExpressionValue*>(value);
		if (expValue->count <= 1)
			return nullptr;
		Field** fields = (Field**)shellGlobalBufferPool.alloc(sizeof(Field*) * expValue->count);
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
						typeBuffer[typeBufferSize++] = static_cast<uint8_t>(func->returnType);
						fields[idx] = (Field*)(void*)(((uint64_t)func) | DUAL_ARGV_MATH_OP_FUNC_TYPE);
						continue;
					}
					func = getDualArgvLogicFunc(op->opera, static_cast<META::COLUMN_TYPE>(typeBuffer[typeBufferSize - 1]), static_cast<META::COLUMN_TYPE>(typeBufferSize - 2));
					if (func != nullptr)
					{
						typeBufferSize -= 2;
						typeBuffer[typeBufferSize++] = static_cast<uint8_t>(func->returnType);
						fields[idx] = (Field*)(void*)(((uint64_t)func) | DUAL_ARGV_LOGIC_OP_FUNC_TYPE);
						continue;
					}
				}
				if (typeBufferSize >= 1)
				{
					SQL_PARSER::SQLOperatorValue* op = static_cast<SQL_PARSER::SQLOperatorValue*>(expValue->valueStack[idx]);
					operatorFuncInfo* func = getSingleArgvMathFunc(op->opera, static_cast<META::COLUMN_TYPE>(typeBuffer[typeBufferSize - 1]));
					if (func != nullptr)
					{
						typeBuffer[typeBufferSize - 1] = static_cast<uint8_t>(func->returnType);
						fields[idx] = (Field*)(void*)(((uint64_t)func) | SINGLE_ARGV_MATH_OP_FUNC_TYPE);
						continue;
					}
					func = getSingleArgvLogicFunc(op->opera, static_cast<META::COLUMN_TYPE>(typeBuffer[typeBufferSize - 1]));
					if (func != nullptr)
					{
						typeBuffer[typeBufferSize - 1] = static_cast<uint8_t>(func->returnType);
						fields[idx] = (Field*)(void*)(((uint64_t)func) | SINGLE_ARGV_LOGIC_OP_FUNC_TYPE);
						continue;
					}
				}
				shellGlobalBufferPool.free(fields);
				return nullptr;
			}
			else
			{
				fields[idx] = createFieldFromSqlValue(expValue->valueStack[idx], sql, isSelectedColumn);
				if (fields[idx]->fieldType == GROUP_FUNCTION_FIELD)
					group = true;
				typeBuffer[typeBufferSize++] = static_cast<uint8_t>(fields[idx]->valueType);
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
	static inline Field* createFieldFromSqlValue(SQL_PARSER::SQLValue* value, selectSqlInfo* select, bool isSelectedColumn)
	{
		switch (value->type)
		{
		case SQL_PARSER::INT_NUMBER_TYPE:
			return SQLValue2Int(value);
		case SQL_PARSER::STRING_TYPE:
			return SQLValue2String(value);
		case SQL_PARSER::COLUMN_NAME_TYPE:
			return SQLValue2Column(value, select, isSelectedColumn);
		case SQL_PARSER::FUNCTION_TYPE:
			return SQLValue2FunctionField(value, select, isSelectedColumn);
		case SQL_PARSER::EXPRESSION_TYPE:
			return SQLValue2ExpressionField(value, select, isSelectedColumn);
		case SQL_PARSER::FLOAT_NUMBER_TYPE:
			return SQLValue2Float(value);
		default:
			return nullptr;
		}
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectSqlType(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		createSelectSqlInfo(h);
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectField(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		getSelectValues()[sql->selectFieldCount] = value;
		selectValueAlias.get()[sql->selectFieldCount++] = nullptr;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectFiledAlias(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectValueAlias.get()[getSelectSqlInfoFromHandle(h)->selectFieldCount - 1] = static_cast<SQL_PARSER::SQLStringValue*>(value)->value;
		return SQL_PARSER::OK;
	}

	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectWhereField(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
		expressionField* exp = SQLValue2ExpressionField(value, sql, false);
		if (exp == nullptr)
			return SQL_PARSER::INVALID;
		/*not allowed group expression in where condition,and return type of this expression must be bool*/
		if (exp->group || static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::COLUMN_TYPE::T_BOOL)
		{
			exp->clean();
			shellGlobalBufferPool.free(exp);
			return SQL_PARSER::INVALID;
		}
		sql->selectFields.add(exp);
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinTable(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::TABLE_NAME_TYPE);
		const META::tableMeta* meta = getMeta(h, static_cast<SQL_PARSER::SQLTableNameValue*>(value));
		if (meta == nullptr)
		{
			return SQL_PARSER::INVALID;
		}
		sql->joinedTables.add(meta);
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectLeftJoin(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		sql->joinType = LEFT_JOIN;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectRightJoin(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		sql->joinType = RIGHT_JOIN;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectInnerJoin(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		sql->joinType = INNER_JOIN;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinOnCondition(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		if (sql->joinedUsingColumns.size != 0)
			return SQL_PARSER::INVALID;
		assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
		expressionField* exp = SQLValue2ExpressionField(value, sql, false);
		if (exp == nullptr)
			return SQL_PARSER::INVALID;
		if (static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::COLUMN_TYPE::T_BOOL)
		{
			exp->clean();
			shellGlobalBufferPool.free(exp);
			return SQL_PARSER::INVALID;
		}
		sql->joinedCondition = exp;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectJoinUsingColumn(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::STRING_TYPE);
		if (sql->joinedCondition != nullptr)
			return SQL_PARSER::INVALID;
		sql->joinedUsingColumns.add(static_cast<SQL_PARSER::SQLStringValue*>(value)->value);
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectGroupByColumn(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		if (value->type == SQL_PARSER::COLUMN_NAME_TYPE)
		{
			return SQL_PARSER::OK;
		}
		else if (value->type == SQL_PARSER::EXPRESSION_TYPE)
		{
			expressionField* exp = SQLValue2ExpressionField(value, sql, false);
			if (exp == nullptr)
				return SQL_PARSER::INVALID;
			if (exp->valueType == META::COLUMN_TYPE::T_BOOL)
			{
				exp->clean();
				shellGlobalBufferPool.free(exp);
				return SQL_PARSER::INVALID;
			}
			sql->groupBy.add(exp);
			return SQL_PARSER::OK;
		}
		else if (value->type == SQL_PARSER::FUNCTION_TYPE)
		{
			Field* field = SQLValue2FunctionField(value, sql, false);
			if (field == nullptr)
				return SQL_PARSER::INVALID;
			if (field->fieldType == GROUP_FUNCTION_FIELD)
			{
				field->clean();
				shellGlobalBufferPool.free(field);
				return SQL_PARSER::INVALID;
			}
			sql->groupBy.add(field);
			return SQL_PARSER::OK;
		}
		else
			return SQL_PARSER::INVALID;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectHavingCondition(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
		expressionField* exp = SQLValue2ExpressionField(value, sql, false);
		if (exp == nullptr)
			return SQL_PARSER::INVALID;
		/*return type of this expression must be bool*/
		if (static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::COLUMN_TYPE::T_BOOL)
		{
			exp->clean();
			shellGlobalBufferPool.free(exp);
			return SQL_PARSER::INVALID;
		}
		sql->havCondition = exp;
		return SQL_PARSER::OK;
	}
	extern "C" DLL_EXPORT  SQL_PARSER::parseValue selectOrderByExpressionField(SQL_PARSER::handle * h, SQL_PARSER::SQLValue * value)
	{
		selectSqlInfo* sql = getSelectSqlInfoFromHandle(h);
		assert(value->type == SQL_PARSER::EXPRESSION_TYPE);
		expressionField* exp = SQLValue2ExpressionField(value, sql, false);
		if (exp == nullptr)
			return SQL_PARSER::INVALID;
		/*return type of this expression must be bool*/
		if (static_cast<const operatorFuncInfo*>((void*)(((uint64_t)exp->list[exp->listSize - 1]) & ~FUNC_ARGV_MASK))->returnType != META::COLUMN_TYPE::T_BOOL)
		{
			exp->clean();
			shellGlobalBufferPool.free(exp);
			return SQL_PARSER::INVALID;
		}
		sql->havCondition = exp;
		return SQL_PARSER::OK;
	}
	inline bool processSelectFields(selectSqlInfo* sql)
	{
		SQL_PARSER::SQLValue** columns = getSelectValues();
		for (int i = 0; i < sql->selectFieldCount; i++)
		{
			Field* field = createFieldFromSqlValue(columns[i], sql, true);
			if (field == nullptr)
				return false;
			sql->selectFields.add(field);
		}
		return true;
	}
}
