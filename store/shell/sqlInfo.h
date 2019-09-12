#pragma once
#include "sqlParser/sqlValue.h"
#include "field.h"
namespace STORE
{
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
				list = (T*)shellGlobalBufferPool.alloc(sizeof(T) * (volumn = 16));
				size = 0;
			}
			inline void add(T v)
			{
				if (unlikely(size >= volumn))
				{
					T* newList = (T*)shellGlobalBufferPool.alloc(sizeof(T) * (volumn = volumn << 1));
					memcpy(newList, list, sizeof(T) * size);
				}
				list[size++] = v;
			}
		};

		struct selectSqlInfo :public sqlBasicInfo {
			const META::tableMeta * table;
			uint64_t selectTableId;
			const char* tableAlias;
			sqlList<const META::tableMeta*> joinedTables;
			sqlList<Field*> selectFields;
			sqlList<const char*> selectFieldAlias;
			uint64_t* joinedTableIds;
			sqlList<SQL_PARSER::SQLValue*> rawSelectFields;
			sqlList<SQL_PARSER::SQLColumnNameValue*> inGroupColumns;
			sqlList<SQL_PARSER::SQLColumnNameValue*> notInGroupColumns;
			sqlList<const char *> joinedUsingColumns;
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
			bool isGroupColumn(const char* columnName,uint64_t tableId)
			{
				for (int idx = 0; idx < groupByColumnNames.size; idx++)
				{
					if (tableId == groupByColumnNames.list[idx]->tableId)
					{
						if (strcmp(groupByColumnNames.list[idx]->name, columnName) == 0||
							strcmp(groupByColumnNames.list[idx]->alias, columnName) == 0)
						{
							return true;
						}
					}
				}
				return false;
			}
			bool addGroupColumn(selectColumnInfo* column)
			{
				if (isGroupColumn(column->name, column->tableId))
					return false;
				groupByColumnNames.add(column);
				return true;
			}
		};
	}

}