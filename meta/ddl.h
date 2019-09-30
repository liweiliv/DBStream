#pragma once
#include "metaDataCollection.h"
namespace META {
	enum DDL_TYPE {
		CREATE_DATABASE,
		DROP_DATABASE,
		ALTER_DATABASE,
		CREATE_TABLE,
		CREATE_TABLE_LIKE,
		CREATE_TABLE_FROM_SELECT,
		DROP_TABLE,
		RENAME_TABLE,
		ALTER_TABLE_ADD_COLUMN,
		ALTER_TABLE_ADD_COLUMNS,
		ALTER_TABLE_RENAME_COLUMN,
		ALTER_TABLE_MODIFY_COLUMN,
		ALTER_TABLE_CHANGE_COLUMN,
		ALTER_TABLE_DROP_COLUMN,
		ALTER_TABLE_ADD_INDEX,
		ALTER_TABLE_DROP_INDEX,
		ALTER_TABLE_ADD_KEY,
		ALTER_TABLE_DROP_KEY,
		ALTER_TABLE_RENAME_INDEX,
		ALTER_TABLE_ADD_FOREIGN_KEY,
		ALTER_TABLE_DROP_FOREIGN_KEY,
		ALTER_TABLE_ADD_PRIMARY_KEY,
		ALTER_TABLE_DROP_PRIMARY_KEY,
		ALTER_TABLE_ADD_UNIQUE_KEY,
		ALTER_TABLE_DROP_UNIQUE_KEY,
	};
	struct ddl {
		DDL_TYPE m_type;
		const char* rawDdl;
		std::string usedDb;
	};
	struct dataBaseDDL :public ddl {
		std::string name;
		const charsetInfo* charset;
		std::string collate;
	};
	struct createTableDDL:public ddl
	{
		tableMeta table;
	};
	struct createTableLike :public ddl
	{
		std::string database;
		std::string table;
		std::string likedDatabase;
		std::string likedTable;
	};
	struct createTableFromSelect :public ddl //todo
	{
		std::string database;
		std::string table;
	};
	struct dropTable :public ddl
	{
		std::string database;
		std::string table;
	};
	struct renameTable :public ddl
	{
		std::list<std::string> databases;
		std::list<std::string> tables;
		std::list<std::string> destDatabases;
		std::list<std::string> destTables;
	};
	struct addColumn :public ddl
	{
		std::string database;
		std::string table;
		columnMeta column;
		std::string afterColumnName;
	};
	struct addColumns :public ddl
	{
		std::string database;
		std::string table;
		std::list<columnMeta*> columns;
	};
	struct renameColumn :public ddl
	{
		std::string database;
		std::string table;
		std::string srcColumnName;
		std::string destColumnName;
	};
	struct modifyColumn :public ddl
	{
		std::string database;
		std::string table;
		std::string srcColumnName;
		columnMeta column;
		bool first;
		std::string afterColumnName;
	};
	struct changeColumn :public ddl
	{
		std::string database;
		std::string table;
		std::string srcColumnName;
		columnMeta newColumn;
		bool first;
		std::string afterColumnName;
	};
	struct dropColumn :public ddl
	{
		std::string database;
		std::string table;
		std::string columnName;
	};
}