/*
 * sqlParserFuncs.cpp
 *
 *  Created on: 2018年11月19日
 *      Author: liwei
 */
#include <assert.h>
#include "meta/metaChangeInfo.h"
#include "meta/charset.h"
#include "sqlParser/sqlParserHandle.h"
#include  "util/winDll.h"
#include "meta/ddl.h"
#include "util/threadLocal.h"
using namespace META;
namespace SQL_PARSER {
#define  NOT_FIXED_DEC 31
#define createMeta(h) (h)->userData = new metaChangeInfo();
	static threadLocal<META::alterTable> alterTableInfo;
	static inline META::alterTable* getAlterTableInfo()
	{
		META::alterTable* at = alterTableInfo.get();
		if (unlikely(at == nullptr))
			alterTableInfo.set(at = new META::alterTable);
		return at;
	}
	static inline void copyAlterTableHeadInfo(META::alterTable * ddl)
	{
		META::alterTable* at = getAlterTableInfo();
		ddl->database = at->database;
		ddl->table = at->table;
		ddl->rawDdl = at->rawDdl;
		ddl->usedDb = at->usedDb;
	}
	static inline void setTableName(Table* table, const string& tableName, handle* h)
	{
		table->table = tableName;
		if (table->database.empty())
			table->database = h->dbName;
	}
	static inline metaChangeInfo* getMeta(handle* h)
	{
		metaChangeInfo* meta =  static_cast<metaChangeInfo*>((h)->userData);
		if(meta == nullptr)
		{
			meta = new metaChangeInfo();
			h->userData = meta;
		}
		return meta;
	}
	static inline newTableInfo* getLastTable(handle* h)
	{
		metaChangeInfo* meta = getMeta(h);
		list<newTableInfo*>::reverse_iterator iter = meta->newTables.rbegin();
		return *iter;
	}
	static inline newColumnInfo* getLastColumn(handle* h)
	{
		return *getLastTable(h)->newColumns.rbegin();
	}
	extern "C" DLL_EXPORT  void createUserData(handle* h)
	{
		h->userData = new metaChangeInfo();
	}
	extern "C" DLL_EXPORT  void destroyUserData(handle* h)
	{
		if (h->userData != nullptr)
		{
			delete getMeta(h);
			h->userData = nullptr;
		}
	}
	static inline META::columnMeta* getColume(handle* h)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		switch (ddl->m_type)
		{
		case META::CREATE_TABLE:
			return &static_cast<META::createTableDDL*>(ddl)->table.m_columns[static_cast<META::createTableDDL*>(ddl)->table.m_columnsCount - 1];
		case META::ALTER_TABLE_ADD_COLUMN:
			return &static_cast<META::addColumn*>(ddl)->column;
		case META::ALTER_TABLE_ADD_COLUMNS:
			return &(*static_cast<META::addColumns*>(ddl)->columns.rbegin());
		case META::ALTER_TABLE_CHANGE_COLUMN:
			return &static_cast<META::changeColumn*>(ddl)->newColumn;
		case META::ALTER_TABLE_MODIFY_COLUMN:
			return &static_cast<META::modifyColumn*>(ddl)->column;
		default:
			return nullptr;
		}
	}
	static  inline META::columnMeta* createColume(handle* h,const char * name)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		switch (ddl->m_type)
		{
		case META::CREATE_TABLE:
		{
			META::columnMeta column;
			column.m_columnName.assign(name);
			static_cast<META::createTableDDL*>(ddl)->table.addColumn(&column);
			return &static_cast<META::createTableDDL*>(ddl)->table.m_columns[static_cast<META::createTableDDL*>(ddl)->table.m_columnsCount - 1];
		}
		case META::ALTER_TABLE_ADD_COLUMN:
		{
			static_cast<META::addColumn*>(ddl)->column.m_columnName.assign(name);
			return &static_cast<META::addColumn*>(ddl)->column;
		}
		case META::ALTER_TABLE_ADD_COLUMNS:
		{
			META::columnMeta column;
			column.m_columnName.assign(name);
			static_cast<META::addColumns*>(ddl)->columns.push_back(column);
			return &(*static_cast<META::addColumns*>(ddl)->columns.rbegin());
		}
		case META::ALTER_TABLE_CHANGE_COLUMN:
		{
			static_cast<META::changeColumn*>(ddl)->newColumn.m_columnName.assign(name);
			return &static_cast<META::changeColumn*>(ddl)->newColumn;
		}
		case META::ALTER_TABLE_MODIFY_COLUMN:
		{
			static_cast<META::modifyColumn*>(ddl)->column.m_columnName.assign(name);
			return &static_cast<META::modifyColumn*>(ddl)->column;
		}
		default:
			return nullptr;
		}
	}
	extern "C" DLL_EXPORT  parseValue bitType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_BIT];
		column->m_srcColumnType = MYSQL_TYPE_BIT;
		column->m_size = 1;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bitTypeSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_size = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY];
		column->m_srcColumnType = MYSQL_TYPE_TINY;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue BoolType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY];
		column->m_srcColumnType = MYSQL_TYPE_TINY;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue numertypeIsUnsigned(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		switch (column->m_srcColumnType)
		{
		case MYSQL_TYPE_TINY:
			column->m_columnType = T_UINT8;
			break;
		case MYSQL_TYPE_SHORT:
			column->m_columnType = T_UINT16;
			break;
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			column->m_columnType = T_UINT32;
			break;
		case MYSQL_TYPE_LONGLONG:
			column->m_columnType = T_UINT64;
			break;
		}
		column->m_signed = false;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue smallIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_SHORT];
		column->m_srcColumnType = MYSQL_TYPE_SHORT;
		column->m_signed = true;
	}

	extern "C" DLL_EXPORT   parseValue mediumIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_INT24];
		column->m_srcColumnType = MYSQL_TYPE_INT24;
		column->m_signed = true;
		return OK;
	}

	extern "C" DLL_EXPORT  parseValue intType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONG];
		column->m_srcColumnType = MYSQL_TYPE_LONG;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bigIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONGLONG];
		column->m_srcColumnType = MYSQL_TYPE_LONGLONG;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue decimalType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_NEWDECIMAL];
		column->m_srcColumnType = MYSQL_TYPE_NEWDECIMAL;
		column->m_signed = true;
		column->m_size = 10;
		column->m_decimals = 0;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_size = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatDigitsSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_decimals = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatType(handle* h, SQLValue * value)
	{

		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_FLOAT];
		column->m_srcColumnType = MYSQL_TYPE_FLOAT;
		column->m_signed = true;
		column->m_size = 12;
		column->m_decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue doubleType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DOUBLE];
		column->m_srcColumnType = MYSQL_TYPE_DOUBLE;
		column->m_signed = true;
		column->m_size = 22;
		column->m_decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATETIME];
		column->m_srcColumnType = MYSQL_TYPE_DATETIME;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATETIME2];
		column->m_srcColumnType = MYSQL_TYPE_DATETIME2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIMESTAMP];
		column->m_srcColumnType = MYSQL_TYPE_TIMESTAMP;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue dateType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATE];
		column->m_srcColumnType = MYSQL_TYPE_DATE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIMESTAMP2];
		column->m_srcColumnType = MYSQL_TYPE_TIMESTAMP2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIME];//todo
		column->m_srcColumnType = MYSQL_TYPE_TIME;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIME2];
		column->m_srcColumnType = MYSQL_TYPE_TIME2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_YEAR];
		column->m_srcColumnType = MYSQL_TYPE_YEAR;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue charType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_STRING];
		column->m_srcColumnType = MYSQL_TYPE_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varcharType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_VARCHAR];
		column->m_srcColumnType = MYSQL_TYPE_VARCHAR;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeCharSet(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_charset = getCharset(static_cast<SQLStringValue*>(value)->value.c_str());
		if (column->m_charset == nullptr)
			return NOT_SUPPORT;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_size = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue binaryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_BINARY;
		column->m_srcColumnType = MYSQL_TYPE_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varbinaryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_BINARY;
		column->m_srcColumnType = MYSQL_TYPE_VAR_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyBlobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_TINY_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue blobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumBlobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_MEDIUM_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_MEDIUM_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longBlobtype(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONG_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_LONG_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyTextType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_TINY_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue textType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumTextType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_MEDIUM_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longTexttype(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_LONG_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue enumType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_ENUM];
		column->m_srcColumnType = MYSQL_TYPE_ENUM;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue setType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_SET];
		column->m_srcColumnType = MYSQL_TYPE_SET;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  enumOrSetValueList(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_setAndEnumValueList.add(static_cast<SQLStringValue*>(value)->value.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue geometryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_GEOMETRY];
		column->m_srcColumnType = MYSQL_TYPE_GEOMETRY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue jsonType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_JSON];
		column->m_srcColumnType = MYSQL_TYPE_JSON;
		column->m_charset = &charsets[utf8mb4];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newColumn(handle* h, SQLValue * value)
	{
		createColume(h, static_cast<SQLStringValue*>(value)->value.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  columnCollate(handle* h, SQLValue* value)
	{
		META::columnMeta* column = getColume(h);
		column->m_collate = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue generatedColumn(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_generated = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsUK(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_isUnique = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsPK(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_isPrimary = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsKey(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColume(h);
		column->m_isIndex = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue primaryKeys(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl == nullptr)//create table also use primaryKeys,but do not need to create it
		{
			META::addKey* pk = new META::addKey;
			copyAlterTableHeadInfo(pk);
			pk->m_type = META::ALTER_TABLE_ADD_PRIMARY_KEY;
			pk->name = "PRIMARY KEY";
			h->userData = pk;
		}
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue primaryKeyColumn(handle* h, SQLValue* value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE_ADD_PRIMARY_KEY)
		{
			META::addKey* pk = static_cast<META::addKey*>(ddl);
			pk->columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else if (META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->primaryKey.columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue uniqueKeyName(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl == nullptr)
		{
			META::addKey* uk = new META::addKey;
			copyAlterTableHeadInfo(uk);
			uk->m_type = META::ALTER_TABLE_ADD_UNIQUE_KEY;
			uk->name = static_cast<SQLNameValue*>(value)->name;
			h->userData = uk;
		}
		else if (ddl->m_type == META::CREATE_TABLE)
		{
			META::addKey uk;
			uk.name = static_cast<SQLNameValue*>(value)->name;
			static_cast<META::createTableDDL*>(ddl)->uniqueKeys.push_back(uk);
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue uniqueKeyColumn(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE_ADD_UNIQUE_KEY)
		{
			META::addKey* uk = static_cast<META::addKey*>(ddl);
			uk->columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else if (META::CREATE_TABLE)
		{
			(*static_cast<META::createTableDDL*>(ddl)->uniqueKeys.rbegin()).columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tableCharset(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		const charsetInfo * charset = getCharset(static_cast<SQLStringValue*>(value)->value.c_str());
		if (charset == nullptr)
			return NOT_SUPPORT;
		if (ddl->m_type == META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->table.m_charset = charset;
		}
		else if (ddl->m_type == META::ALTER_TABLE_DEFAULT_CHARSET || ddl->m_type == META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET)
		{
			static_cast<META::defaultCharset*>(ddl)->charset = charset;
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue changeTableCharset(handle* h, SQLValue* value)
	{
		META::defaultCharset* ddl = new META::defaultCharset;
		copyAlterTableHeadInfo(ddl);
		ddl->m_type = META::ALTER_TABLE_DEFAULT_CHARSET;
		ddl->charset = nullptr;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue converTableCharset(handle* h, SQLValue* value)
	{
		META::defaultCharset* ddl = new META::defaultCharset;
		copyAlterTableHeadInfo(ddl);
		ddl->m_type = META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET;
		ddl->charset = nullptr;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tableCollate(handle* h, SQLValue* value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->table.m_collate = static_cast<SQLStringValue*>(value)->value;
		}
		else if (ddl->m_type == META::ALTER_TABLE_DEFAULT_CHARSET || ddl->m_type == META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET)
		{
			static_cast<META::defaultCharset*>(ddl)->collate = static_cast<SQLStringValue*>(value)->value;
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newTable(handle* h, SQLValue * value)
	{
		META::createTableDDL* ddl = new META::createTableDDL();
		ddl->m_type = META::CREATE_TABLE;
		ddl->usedDb = h->dbName;
		h->userData = ddl;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue NewTableDBName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue NewTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->type = META::newTableInfo::CREATE_TABLE;
		setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createTableLike(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->createLike = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  NewTableLikedDBName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->likedTable.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  NewTableLikedTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		setTableName(&t->likedTable, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAtFirst(handle* h, SQLValue * value)
	{
		newColumnInfo* c = getLastColumn(h);
		c->after = false;
		c->index = 0;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAfter(handle* h, SQLValue * value)
	{
		newColumnInfo* c = getLastColumn(h);
		c->after = true;
		c->afterColumnName = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndex(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumn(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = getLastColumn(h);
		c->after = true;
		c->afterColumnName = *t->oldColumns.rbegin();
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumnName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->oldColumns.push_back(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropColumn(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->oldColumns.push_back(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropPrimaryKey(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back("PRIMARY");
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropForeignKey(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTable(handle* h, SQLValue * value)
	{
		newTableInfo* t = new newTableInfo;
		t->type = newTableInfo::ALTER_TABLE;
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableDbName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUK(handle* h, SQLValue * value)
	{
		newTableInfo* t = new newTableInfo;
		t->type = newTableInfo::ALTER_TABLE;
		newKeyInfo* k = new newKeyInfo;
		k->type = newKeyInfo::UNIQUE_KEY;
		t->newKeys.push_back(k);
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	/*now we only parse uk ,key/index not parse*/
	extern "C" DLL_EXPORT  parseValue  createUKName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			(*(t->newKeys.begin()))->name = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUKONDatabaseName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			t->table.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUKONTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  tableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUkByColumn(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			(*(t->newKeys.begin()))->columns.push_back(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndexName(handle* h, SQLValue * value)
	{
		newTableInfo* t = new newTableInfo;
		t->type = newTableInfo::ALTER_TABLE;
		t->oldColumns.push_back(static_cast<SQLStringValue*>(value)->value);
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	/*now we only parse uk ,key/index not parse*/
	extern "C" DLL_EXPORT  parseValue  dropIndexOnDataBaseName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndexOnTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTable(handle* h, SQLValue * value)
	{
		getMeta(h)->oldTables.push_back(Table());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTableDatabaseName(handle* h, SQLValue * value)
	{
		(*getMeta(h)->oldTables.rbegin()).database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTableTableName(handle* h, SQLValue * value)
	{
		Table& table = *getMeta(h)->oldTables.rbegin();
		setTableName(&table, static_cast<SQLStringValue*>(value)->value, h);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableDatabaseName(handle* h, SQLValue * value)
	{
		getMeta(h)->oldTables.push_back(Table(static_cast<SQLStringValue*>(value)->value, ""));
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableTableName(handle* h, SQLValue * value)
	{
		Table& t = *getMeta(h)->oldTables.rbegin();
		if (t.table.empty() && !t.database.empty())//has setted dbname,only set table name
			t.table = static_cast<SQLStringValue*>(value)->value;
		else
		{
			getMeta(h)->oldTables.push_back(Table(h->dbName, static_cast<SQLStringValue*>(value)->value));//dot not set db name and table name,look as new table name 
		}
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameNewTable(handle* h, SQLValue * value)
	{
		newTableInfo* t = new newTableInfo();
		t->createLike = true;
		t->likedTable = *getMeta(h)->oldTables.rbegin();
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableToDatabaseName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableToTableName(handle* h, SQLValue * value)
	{
		newTableInfo* t = getLastTable(h);
		setTableName(&t->table, static_cast<SQLStringValue*>(value)->value, h);
		t->table.table = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  useDatabase(handle* h, SQLValue * value)
	{
		h->dbName = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterDatabase(handle* h, SQLValue * value)
	{
		getMeta(h)->database.name = static_cast<SQLStringValue*>(value)->value;
		getMeta(h)->database.type = databaseInfo::ALTER_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createDatabase(handle* h, SQLValue * value)
	{
		getMeta(h)->database.name = static_cast<SQLStringValue*>(value)->value;
		getMeta(h)->database.type = databaseInfo::CREATE_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropDatabase(handle* h, SQLValue * value)
	{
		getMeta(h)->database.name = static_cast<SQLStringValue*>(value)->value;
		getMeta(h)->database.type = databaseInfo::DROP_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  databaseCharset(handle* h, SQLValue * value)
	{
		getMeta(h)->database.charset = getCharset(static_cast<SQLStringValue*>(value)->value.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  databaseCollate(handle* h, SQLValue* value)
	{
		getMeta(h)->database.collate = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
}

