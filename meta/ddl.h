#pragma once
#include "metaDataCollection.h"
namespace META {
	enum DDL_TYPE {
		UNKNOW_DDL_TYPE,
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
		ALTER_TABLE_RENAME_INDEX,
		ALTER_TABLE_ADD_FOREIGN_KEY,
		ALTER_TABLE_DROP_FOREIGN_KEY,
		ALTER_TABLE_ADD_PRIMARY_KEY,
		ALTER_TABLE_DROP_PRIMARY_KEY,
		ALTER_TABLE_ADD_UNIQUE_KEY,
		ALTER_TABLE_DROP_UNIQUE_KEY,
		ALTER_TABLE_DEFAULT_CHARSET,
		ALTER_TABLE_CONVERT_DEFAULT_CHARSET,
		CREATE_VIEW,
		CREATE_TRIGGER
	};
	struct ddl {
		DDL_TYPE m_type;
		const char* rawDdl;
		std::string usedDb;
		ddl():m_type(UNKNOW_DDL_TYPE), rawDdl(nullptr){}
		ddl& operator=(const ddl& cp) 
		{
			m_type = cp.m_type;
			rawDdl = cp.rawDdl;
			usedDb = cp.usedDb;
			return *this;
		}
		ddl(const ddl& cp) :m_type(cp.m_type), rawDdl(cp.rawDdl), usedDb(cp.usedDb) {}
		virtual ~ddl() {}
	};
	struct dataBaseDDL :public ddl {
		std::string name;
		const charsetInfo* charset;
		std::string collate;
	};
	struct addKey;
	struct createTableDDL:public ddl
	{
		tableMeta table;
		addKey primaryKey;
		std::list<addKey> uniqueKeys;
		std::list<addKey> indexs;
		createTableDDL() :table(true){}
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
	struct alterTable :public ddl {
		std::string database;
		std::string table;
	};
	struct addColumn :public alterTable
	{
		columnMeta column;
		std::string afterColumnName;
	};
	struct addColumns :public alterTable
	{
		std::list<columnMeta> columns;
	};
	struct renameColumn :public alterTable
	{
		std::string srcColumnName;
		std::string destColumnName;
	};
	struct modifyColumn :public alterTable
	{
		std::string srcColumnName;
		columnMeta column;
		bool first;
		std::string afterColumnName;
	};
	struct changeColumn :public alterTable
	{
		std::string srcColumnName;
		columnMeta newColumn;
		bool first;
		std::string afterColumnName;
	};
	struct dropColumn :public alterTable
	{
		std::string columnName;
	};
	struct renameKey :public alterTable
	{
		std::string srcKeyName;
		std::string destKeyName;
	};
	struct addKey :public alterTable
	{
		std::string name;
		std::list<std::string> columnNames;
		addKey() {}
		addKey(const struct addKey& key)
		{
			name = key.name;
			std::copy(key.columnNames.begin(), key.columnNames.end(), std::back_inserter(columnNames));
		}
		struct addKey& operator=(const struct addKey& key)
		{
			name = key.name;
			std::copy(key.columnNames.begin(), key.columnNames.end(), std::back_inserter(columnNames));
			return *this;
		}
	};
	struct dropKey :public alterTable
	{
		std::string name;
	};
	struct defaultCharset :public alterTable
	{
		const charsetInfo* charset;
		std::string collate;
	};
}