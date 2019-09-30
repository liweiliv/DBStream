#pragma once
#include "meta/metaChangeInfo.h"
#include "meta/mysqlTypes.h"
#include "mysql.h"
namespace META {
	class metaChangeInfoToMysqlSql :public metaChangeInfoToSql {
	private:
		virtual std::string dataBaseSql(const databaseInfo& database)
		{
			if (database.type == databaseInfo::CREATE_DATABASE)
			{
				return std::string("CREATE DATABASE `").append(database.name).append("`").
					append(database.charset == nullptr ? "" : (std::string(" DEFAULT CHARACTER SET ").append(database.charset->name))).
					append(database.collate.empty() ? "" : (std::string(" DEFAULT COLLATE ").append(database.collate)));
			}
			else if (type == databaseInfo::DROP_DATABASE)
			{
				return std::string("DROP DATABASE `").append(database.name).append("`");
			}
			else if (type == databaseInfo::ALTER_DATABASE)
			{
				if (charset != nullptr || !collate.empty())
					return std::string("ALTER DATABASE `").append(database.name).append("`").
					append(database.charset == nullptr ? "" : (std::string(" DEFAULT CHARACTER SET ").append(database.charset->name))).
					append(database.collate.empty() ? "" : (std::string(" DEFAULT COLLATE ").append(database.collate)));
			}
			else
				return "";
		}
		virtual std::string tableSql(const newTableInfo * table)
		{
			switch (table->type)
			{
			case newTableInfo::ALTER_TABLE:
			{

			}
			}
		}
	public:
		virtual std::string toSql(const metaChangeInfo* meta)
		{
			if (meta->database.type != databaseInfo::MAX_DATABASEDDL_TYPE)
			{
				return dataBaseSql(meta->database);
			}
			else if (meta->newTables.size > 0)
			{
				std::string sql;
				if (meta->oldTables.size > 0)//rename
				{
					if (!meta->database.name.empty())
						sql.append("USE `").append(meta->database.name).append("`;");
					sql.append("RENAME ")
					std::list<Table>::const_iterator oiter = meta->oldTables.begin();
					for (list<newTableInfo*>::const_iterator iter = meta->newTables.begin(); iter != meta->newTables.end(); iter++)
					{
						if (iter != meta->newTables.begin())
							sql.append(",");
						if (!(*oiter).database.empty())
							sql.append("`").append((*oiter).database).append("`.");
						sql.append("`").append((*oiter).table).append("` TO ");
						if (!(*iter)->table.database.empty())
							sql.append("`").append((*iter)->table.database).append("`.");
						sql.append("`").append((*iter)->table.table).append("`");
					}
				}
				else
				{

					for (list<newTableInfo*>::const_iterator iter = meta->newTables.begin(); iter != meta->newTables.end(); iter++)
					{
						sql.append(tableSql(*iter));
					}
				}
			}
		}

	};
}
