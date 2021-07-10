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
	enum class FIELDS_TYPE {
		SELECT_FROM_TABLE,
		SELECT_FROM_JOIN_TABLE
	};
	struct fieldsInfo :public sqlBasicInfo {
		uint64_t tableId;
		const META::TableMeta* table;
		tableNameInfo alias;
	};
	struct joinInfo :public sqlBasicInfo {
		fieldsInfo* join;
		bool innerJoin;
		bool crossJoin;
		bool straightJoin;
		bool leftjoin;
		bool rightJoin;
		bool outerJoin;
		bool naturalJoin;
		fieldsInfo* joined[MAX_JOIN_TABLE_COUNT];
		uint32_t joinedCount;
		expressionField* joinedCondition;
		tableNameInfo joinOnColumns[MAX_JOIN_COLUMN_COUNT];
		uint32_t joinOnColumnsCount;
	};
	struct selectSqlInfo :public fieldsInfo {
		const char* usedDatabase;
		fieldsInfo* selectTable;
		int selectFieldCount;
		//select field
		Field* selectField[MAX_SELECT_FIELD_COUNT];
		//group by columns
		SQL_PARSER::SQLColumnNameValue* inGroupColumns[MAX_GROUP_BY_COLUMN_COUNT];
		uint8_t gourpColumnCount;
		//join using(columnName)
		expressionField* whereCondition;
		expressionField* havCondition;
		uint32_t limitCount;
		uint32_t limitOffset;
		Field* orderBy;
		bool orderByDesc;
		void init()
		{
		}
		bool isGroupColumn(SQL_PARSER::SQLColumnNameValue* column)
		{
			for (uint32_t idx = 0; idx < gourpColumnCount; idx++)
			{
				if (*inGroupColumns[idx] == *column)
					return true;
			}
			return false;
		}
		bool addGroupColumn(SQL_PARSER::SQLColumnNameValue* column)
		{
			if (gourpColumnCount >= MAX_GROUP_BY_COLUMN_COUNT)
				return false;
			if (isGroupColumn(column))
				return false;
			inGroupColumns[gourpColumnCount] = column;
			return true;
		}
	};
}
