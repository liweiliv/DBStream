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
				list = shellGlobalBufferPool.alloc(sizeof(T) * (volumn = 16));
				size = 0;
			}
			inline void add(T v)
			{
				if (unlikely(size >= volumn))
				{
					T* newList = shellGlobalBufferPool.alloc(sizeof(T) * (volumn = volumn << 1));
					memcpy(newList, list, sizeof(T) * size);
				}
				list[size++] = v;
			}
		};
		struct selectSqlInfo :public sqlBasicInfo {
			SQL_PARSER::SQLTableNameValue * table;
			sqlList<Field*> selectFields;
			sqlList<SQL_PARSER::SQLColumnNameValue*> inGroupColumns;
			sqlList<SQL_PARSER::SQLColumnNameValue*> notInGroupColumns;
			sqlList<SQL_PARSER::SQLTableNameValue*> joinedTables;
			sqlList<const char *> joinedUsingColumns;
			JOIN_TYPE joinType;
			expressionField* joinedCondition;
			expressionField* whereCondition;
			sqlList<Field*> groupBy;
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
				limitCount = 0;
				limitOffset = 0;
				orderBy = nullptr;
				orderByDesc = false;
			}
		};
	}

}