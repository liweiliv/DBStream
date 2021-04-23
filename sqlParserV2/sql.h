#pragma once
#include "meta/charset.h"
#include "meta/metaData.h"
#include "token.h"
#include "sqlHandle.h"
#include "sqlStack.h"
namespace SQL_PARSER
{
	enum class SQL_TYPE {
		BEGIN,
		COMMIT,
		ROLLBACK,
		ROLLBACK_TO,
		CREATE_DATABASE,
		CREATE_DATABASE_IF_NOT_EXIST,
		DROP_DATABASE,
		DROP_DATABASE_IF_EXIST,
		ALTER_DATABASE,
		CREATE_TABLE,
		CREATE_TABLE_IF_NOT_EXIST,
		CREATE_TABLE_LIKE,
		CREATE_TABLE_LIKE_IF_NOT_EXIST,
		CREATE_TABLE_FROM_QUERY,
		CREATE_TABLE_FROM_QUERY_IF_NOT_EXIST,
		RENAME_TABLE,
		DROP_TABLE,
		DROP_TABLE_IF_EXIST,
		ALTER_TABLE,
		CREATE_INDEX,
		ALTER_INDEX,
		DROP_INDEX,
		INSERT_DML,
		DELETE_DML,
		UPDATE_DML,
		REPLACE_DML,
		LOAD_DATA_FROM_FILE,
		SELECT
	};

	constexpr static auto DEFAULT_ARRAY_LIST_VOLUMN = 8;
	template<typename T>
	struct arrayList {
		T** list;
		uint16_t volumn;
		uint16_t count;
		inline void init(sqlHandle* handle)
		{
			list = (T**)handle->stack->arena.Allocate(sizeof(T*) * (volumn = DEFAULT_ARRAY_LIST_VOLUMN));
			count = 0;
		}
		inline void add(sqlHandle* handle, T* t)
		{
			if (unlikely(count == volumn))
			{
				T** tmp = (T**)handle->stack->arena.Allocate(sizeof(T*) * (volumn += DEFAULT_ARRAY_LIST_VOLUMN));
				memcpy(tmp, list, sizeof(T*) * count);
				list = tmp;
			}
			list[count++] = t;
		}
		inline T* get(uint16_t id)
		{
			if (id >= count)
				return nullptr;
			return list[id];
		}
		inline T* last()
		{
			if (count == 0)
				return nullptr;
			return list[count - 1];
		}
	};

	struct sql {
		SQL_TYPE type;
		const char* currentDatabase;
		void* userData;
		inline void init(SQL_TYPE type, const char* currentDatabase)
		{
			this->type = type;
			this->currentDatabase = currentDatabase;
			userData = nullptr;
		}
	};

	struct dataBaseDDL : public sql {
		identifier* database;
		CHARSET charset;
		inline void init(const char* currentDatabase)
		{
			sql::init(SQL_TYPE::CREATE_DATABASE,  currentDatabase);
			database = nullptr;
			charset = CHARSET::MAX_CHARSET;
		}
	};

	struct createTableDDL : public sql {
		identifier* table;
		META::tableMeta* tableDefine;
		inline void init(const char* currentDatabase)
		{
			sql::init(SQL_TYPE::CREATE_TABLE,currentDatabase);
			table = nullptr;
			tableDefine = nullptr;
		}
		inline DS setTable(sqlHandle* handle, identifier* table)
		{
			if (table->count == 1)
			{
				if (currentDatabase == nullptr)
					dsFailed(1, "No database selected");
				tableDefine = new META::tableMeta(true);
				tableDefine->m_dbName.assign(currentDatabase);
				tableDefine->m_tableName.assign(table->identifiers[0].pos, table->identifiers[0].size);
			}
			else if (table->count == 2)
			{
				tableDefine = new META::tableMeta(true);
				tableDefine->m_dbName.assign(table->identifiers[0].pos, table->identifiers[0].size);
				tableDefine->m_tableName.assign(table->identifiers[1].pos, table->identifiers[1].size);
			}
			else if (table->count == 3)
			{
				if (handle->instance->type == DATABASE_TYPE::POSTGRESQL)
				{
					tableDefine = new META::tableMeta(true);
					tableDefine->m_dbName.assign(table->identifiers[0].pos, table->identifiers[0].size);
					tableDefine->m_schemaName.assign(table->identifiers[1].pos, table->identifiers[1].size);
					tableDefine->m_tableName.assign(table->identifiers[2].pos, table->identifiers[2].size);
					//db.schema.table
				}
				else
					dsFailed(1, "Invalid table name" << table->toString());
			}
			else
			{
				dsFailed(1, "Invalid table name" << table->toString());
			}
			this->table = table;
			dsOk();
		}
	};


	struct createTableLikeDDL : public sql {
		identifier* table;
		identifier* sourceTable;
		inline void init(const char* currentDatabase)
		{
			sql::init(SQL_TYPE::CREATE_TABLE, currentDatabase);
			table = nullptr;
			sourceTable = nullptr;
		}
		inline DS setTable(sqlHandle* handle, identifier* table)
		{
			if(table->count == 1 || table->count == 2 || (table->count == 3&& handle->instance->type == DATABASE_TYPE::POSTGRESQL))
				this->table = table;
			else
				dsFailed(1, "Invalid table name" << table->toString());
			dsOk();
		}
		inline DS setSourceTable(sqlHandle* handle, identifier* table)
		{
			if (table->count == 1 || table->count == 2 || (table->count == 3 && handle->instance->type == DATABASE_TYPE::POSTGRESQL))
				this->sourceTable = table;
			else
				dsFailed(1, "Invalid table name" << table->toString());
			dsOk();
		}
	};

	struct selectSql;
	struct createTableFromSelectDDL : public sql {
		identifier* table;
		selectSql* query;
	};

	struct dropTableDDL : public sql {
		arrayList<identifier> tables;
		inline void init(sqlHandle* handle, const char* currentDatabase)
		{
			sql::init(SQL_TYPE::DROP_TABLE, currentDatabase);
			tables.init(handle);
		}
		inline void add(sqlHandle* handle, identifier* t)
		{
			tables.add(handle, t);
		}
	};

	enum class AlterTableOperatorType {
		ADD_COLUMN,
		ADD_COLUMNS,
		RENAME_COLUMN,
		DROP_COLUMN,
		CHANGE_COLUMN,
		MODIFY_COLUMN,
		RENAME_TABLE,
		ADD_INDEX,
		DROP_INDEX,
		DISABLE_KEY,
		ENABLE_KEY,
		RENAME_INDEX,
		ADD_PRIMARY_KEY,
		DROP_PRIMARY_KEY,
		ADD_UNIQUE_KEY,
		DROP_UNIQUE_KEY,
		ADD_FOREIGN_KEY,
		DROP_FOREIGN_KEY,
		ADD_PARTITION,
		LOCK_TABLE,
		DISCARD_TABLE_SPACE,
		IMPORT_TABLE_SPACE,
		ALTER_COLUMN_DEFAULT,
		ALTER_COLUMN_DROP_DEFAULT,
		ALTER_COLUMN_VISIBLE,
		ALTER_VALIDATION,
		ALTER_FORCE
	};

	struct alterTableOperator {
		AlterTableOperatorType opType;
	};
	struct alterTableAddColumnOp :public alterTableOperator {
		META::columnMeta * col;
		identifier* afterColumn;
		bool first;
		DS init(sqlHandle* handle)
		{
			opType = AlterTableOperatorType::ADD_COLUMN;
			col = new META::columnMeta;
			if (unlikely(!handle->stack->allocedColumns.push(col)))
				dsFailed(1, "column over limit");
			afterColumn = nullptr;
			first = false;
		}
	};

	struct alterTableAddColumnsOp :public alterTableOperator {
		arrayList<META::columnMeta> cols;
		inline void init(sqlHandle* handle)
		{
			cols.init(handle);
			opType = AlterTableOperatorType::ADD_COLUMNS;
		}
		inline DS add(sqlHandle* handle)
		{
			META::columnMeta* col = new META::columnMeta;
			if (unlikely(!handle->stack->allocedColumns.push(col)))
			{
				delete col;
				dsFailed(1, "column over limit");
			}
			cols.add(handle, col);
		}
	};

	struct renameColumnOp :public alterTableOperator {
		identifier* from;
		identifier* to;
		inline void init(sqlHandle* handle)
		{
			opType = AlterTableOperatorType::RENAME_COLUMN;
			from = nullptr;
			to = nullptr;
		}
	};

	struct changeColumnOp :public alterTableAddColumnOp
	{
		identifier* name;
		inline DS init(sqlHandle* handle, AlterTableOperatorType opTtype)
		{
			if (opTtype != AlterTableOperatorType::CHANGE_COLUMN && opTtype != AlterTableOperatorType::MODIFY_COLUMN)
				dsFailed(2, "invalid sql");
			this->opType = opTtype;
			name = nullptr;
		}
	};
	struct dropColumn :public alterTableOperator {
		identifier* name;
		inline void init(sqlHandle* handle)
		{
			opType = AlterTableOperatorType::DROP_COLUMN;
			name = nullptr;
		}
	};
	struct renameTableOp :public alterTableOperator
	{
		identifier* name;
		inline void init(sqlHandle* handle)
		{
			opType = AlterTableOperatorType::RENAME_TABLE;
			name = nullptr;
		}
	};




	struct alterTableDDL :public sql {
		identifier* table;
		arrayList<alterTableOperator>  operators;
		inline void init(sqlHandle* handle, const char* currentDatabase)
		{
			sql::init(SQL_TYPE::DROP_TABLE, currentDatabase);
			operators.init(handle);
		}
		inline void add(sqlHandle* handle, alterTableOperator* o)
		{
			operators.add(handle, o);
		}
	};
}
