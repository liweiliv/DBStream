#pragma once
#include "sqlParser/sqlValue.h"
#include "field.h"
namespace SHELL {
	enum SQL_TYPE {
		SELECT_SQL,
		ALTER_SQL,
		SHOW_SQL
	};
	enum JOIN_TYPE {
		LEFT_JOIN,
		RIGHT_JOIN,
		INNER_JOIN
	};
	struct sqlBasicInfo {
		SQL_TYPE type;
	};
	template<typename T>
	struct sqlList
	{
		T* list;
		uint32_t volumn;
		uint32_t size;
		inline void init()
		{
			list = (T*)shellGlobalBufferPool->alloc(sizeof(T) * (volumn = 16));
			size = 0;
		}
		inline void add(T v)
		{
			if (unlikely(size >= volumn))
			{
				T* newList = (T*)shellGlobalBufferPool->alloc(sizeof(T) * (volumn = volumn << 1));
				memcpy(newList, list, sizeof(T) * size);
			}
			list[size++] = v;
		}
	};
	constexpr static int MAX_JOIN_TABLE_COUNT = 32;
	constexpr static int MAX_SELECT_FIELD_COUNT = 1024;
	constexpr static int MAX_GROUP_BY_COLUMN_COUNT = 32;
	constexpr static int MAX_JOIN_COLUMN_COUNT = 32;

	struct tableNameInfo {
		const char* tableName;
		uint16_t tableNameSize;
		const char* dbName;
		uint16_t dbNameSize;
		const char* alias;
		uint16_t aliasSize;
	};
	struct selectSqlInfo :public sqlBasicInfo {
		const char* usedDatabase;
		tableNameInfo selectTable;
		uint64_t selectTableId;
		tableNameInfo joinTables[MAX_JOIN_TABLE_COUNT];
		uint64_t joinedTableIds[MAX_JOIN_TABLE_COUNT];
		uint8_t joinTablesCount;
		int selectFieldCount;
		Field* selectField[MAX_SELECT_FIELD_COUNT];
		SQL_PARSER::SQLColumnNameValue* inGroupColumns[MAX_GROUP_BY_COLUMN_COUNT];
		uint8_t gourpColumnCount;

		sqlList<const char*> joinedUsingColumns;
		JOIN_TYPE joinType;
		expressionField* joinedCondition;
		expressionField* whereCondition;
		sqlList<Field*> groupBy;
		sqlList<columnFiled*> groupByColumnNames;
		expressionField* havCondition;
		uint32_t limitCount;
		uint32_t limitOffset;
		Field* orderBy;
		bool orderByDesc;
		void init()
		{
			usedDatabase = nullptr;
			type = SELECT_SQL;
			table = nullptr;
			selectFields.init();
			joinedTables.init();
			joinedUsingColumns.init();
			joinedCondition = nullptr;
			joinType = LEFT_JOIN;
			whereCondition = nullptr;
			groupBy.init();
			groupByColumnNames.init();
			havCondition = nullptr;
			limitCount = 0;
			limitOffset = 0;
			orderBy = nullptr;
			orderByDesc = false;
		}
		bool isGroupColumn(const char* columnName, uint64_t tableId)
		{
			for (uint32_t idx = 0; idx < groupByColumnNames.size; idx++)
			{
				if (tableId == groupByColumnNames.list[idx]->tableId)
				{
					if (strcmp(groupByColumnNames.list[idx]->name, columnName) == 0 ||
						strcmp(groupByColumnNames.list[idx]->alias, columnName) == 0)
					{
						return true;
					}
				}
			}
			return false;
		}
		bool addGroupColumn(columnFiled* column)
		{
			if (isGroupColumn(column->name, column->tableId))
				return false;
			groupByColumnNames.add(column);
			return true;
		}
		int8_t getTable(const char* database, const char* tableName)
		{
			if (database == nullptr)
			{
				if (tableName == nullptr)
					return 0;
				if (strcmp(alias, tableName) == 0)
					return 0;
				for (uint32_t idx = 0; idx < joinedTablesAlias.size; idx++)
				{
					if (joinedTablesAlias.list[idx] != nullptr && strcmp(joinedTablesAlias.list[idx], tableName) == 0)
						return 0;
				}
				if (usedDatabase == nullptr)
					return -1;
				database = usedDatabase;
			}
			if (table->isMe(database, tableName))
				return 0;
			for (uint32_t idx = 0; idx < joinedTables.size; idx++)
			{
				if (joinedTables.list[idx]->isMe(database, tableName))
					return idx;
			}
			return -1;
		}
	};
}
