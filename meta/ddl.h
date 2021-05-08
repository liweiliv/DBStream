#pragma once
#include "metaDataCollection.h"
namespace META {
	enum DDL_TYPE {
		UNKNOW_DDL_TYPE,
		BEGIN,
		COMMIT,
		ROLLBACK,
		CREATE_DATABASE,
		DROP_DATABASE,
		ALTER_DATABASE,
		CREATE_TABLE,
		CREATE_TABLE_LIKE,
		CREATE_TABLE_FROM_SELECT,
		DROP_TABLE,
		CREATE_INDEX,
		DROP_INDEX,
		RENAME_TABLE,
		ALTER_TABLE,
		ALTER_TABLE_RENAME,
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
	struct tableName {
		std::string database;
		std::string table;
		tableName() {}
		tableName(const tableName& cp) :database(cp.database), table(cp.table) {}
		tableName& operator=(const tableName& cp)
		{
			database = cp.database;
			table = cp.table;
			return *this;
		}
	};
	struct ddl {
		DDL_TYPE m_type;
		const char* rawDdl;
		std::string usedDb;
		ddl(DDL_TYPE type = UNKNOW_DDL_TYPE) :m_type(type), rawDdl(nullptr) {}
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
	struct dropTable :public ddl
	{
		std::list<tableName> tables;
		dropTable() :ddl(DROP_TABLE) {}
	};
	struct tableDdl :public ddl {
		tableName table;
		tableDdl(DDL_TYPE type = UNKNOW_DDL_TYPE) :ddl(type) {}
		const char* getDatabase()const
		{
			if (!table.database.empty())
				return table.database.c_str();
			else if (!usedDb.empty())
				return usedDb.c_str();
			else
				return nullptr;
		}
	};
	struct alterTableHead {
		DDL_TYPE type;
		alterTableHead(DDL_TYPE type = UNKNOW_DDL_TYPE) :type(type) {}
		virtual ~alterTableHead() {}
	};
	struct addKey :public alterTableHead
	{
		std::string name;
		std::list<std::string> columnNames;
		addKey() :alterTableHead(ALTER_TABLE_ADD_INDEX) {}
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
	struct createTableDDL :public tableDdl
	{
		tableMeta tableDef;
		addKey primaryKey;
		std::list<addKey> uniqueKeys;
		std::list<addKey> indexs;
		createTableDDL() :tableDdl(CREATE_TABLE), tableDef(true) {}
	};
	struct createTableLike :public tableDdl
	{
		tableName likedTable;
		createTableLike() :tableDdl(CREATE_TABLE_LIKE) {}
	};
	struct renameTable :public ddl
	{
		std::list<tableName> src;
		std::list<tableName> dest;
		renameTable() :ddl(RENAME_TABLE) {}
	};

	struct createIndex :public tableDdl
	{
		addKey index;
		createIndex() :tableDdl(CREATE_INDEX) {}
	};
	struct dropIndex :public tableDdl
	{
		std::string name;
		dropIndex() :tableDdl(DROP_INDEX) {}
	};
	struct alterTable :public tableDdl
	{
		std::list<alterTableHead*> detail;
		alterTable() :tableDdl(ALTER_TABLE) {}
		~alterTable()
		{
			for (std::list<alterTableHead*>::iterator iter = detail.begin(); iter != detail.end(); iter++)
				delete* iter;
		}
	};
	struct addColumn :public alterTableHead
	{
		columnMeta column;
		bool first;
		std::string afterColumnName;
		addColumn() :alterTableHead(ALTER_TABLE_ADD_COLUMN), first(false) {}
	};
	struct addColumns :public alterTableHead
	{
		std::list<columnMeta*> columns;
		addColumns() :alterTableHead(ALTER_TABLE_ADD_COLUMNS) {}
		~addColumns()
		{
			for (std::list<columnMeta*>::iterator iter = columns.begin(); iter != columns.end(); iter++)
				delete* iter;
			columns.clear();
		}
	};
	struct renameColumn :public alterTableHead
	{
		std::string srcColumnName;
		std::string destColumnName;
		renameColumn() :alterTableHead(ALTER_TABLE_RENAME_COLUMN) {}

	};
	struct changeColumn :public addColumn
	{
		std::string srcColumnName;
		changeColumn(DDL_TYPE type) {
			assert(type == ALTER_TABLE_MODIFY_COLUMN || type == ALTER_TABLE_CHANGE_COLUMN);
			this->type = type;
		}
	};
	struct alterRenameTable :public alterTableHead
	{
		tableName destTable;
		alterRenameTable() :alterTableHead(ALTER_TABLE_RENAME) {}
	};
	struct dropColumn :public alterTableHead
	{
		std::string columnName;
		dropColumn() :alterTableHead(ALTER_TABLE_DROP_COLUMN) {}

	};
	struct renameKey :public alterTableHead
	{
		std::string srcKeyName;
		std::string destKeyName;
		renameKey() :alterTableHead(ALTER_TABLE_RENAME_INDEX) {}
	};

	struct dropKey :public alterTableHead
	{
		std::string name;
		dropKey() :alterTableHead(ALTER_TABLE_DROP_INDEX) {}
	};
	struct defaultCharset :public alterTableHead
	{
		const charsetInfo* charset;
		std::string collate;
		defaultCharset() :alterTableHead(ALTER_TABLE_DEFAULT_CHARSET), charset(nullptr) {}
	};
}
