/*
 * sqlParserFuncs.cpp
 *
 *  Created on: 2018年11月19日
 *      Author: liwei
 */
#include <assert.h>
#include "meta/charset.h"
#include "sqlParser/sqlParserHandle.h"
#include "util/winDll.h"
#include "meta/ddl.h"
#include "thread/threadLocal.h"
#include "mysql.h"
using namespace META;
namespace SQL_PARSER {
#define  NOT_FIXED_DEC 31
#define createMeta(h) (h)->userData = new metaChangeInfo();
#define GET_CURRENT_ALTER_TABLE_INFO() (*static_cast<META::alterTable*>(h->userData)->detail.rbegin())
	static threadLocal<META::tableDdl> tableInfo;
	static inline META::tableDdl* getTableInfo()
	{
		META::tableDdl* at = tableInfo.get();
		if (unlikely(at == nullptr))
			tableInfo.set(at = new META::tableDdl);
		return at;
	}
	static inline void copyTableHeadInfo(META::tableDdl * ddl)
	{
		META::tableDdl* at = getTableInfo();
		ddl->table = at->table;
		ddl->rawDdl = at->rawDdl;
		ddl->usedDb = at->usedDb;
	}
	extern "C" DLL_EXPORT  void createUserData(handle* h)
	{
		h->userData = nullptr;
	}
	static inline void setDdl(handle* h, struct META::ddl* ddl)
	{
		ddl->rawDdl = h->sql;
		ddl->usedDb = h->dbName;
		h->userData = ddl;
	}
	extern "C" DLL_EXPORT  void destroyUserData(handle* h)
	{
		if (h->userData != nullptr)
		{
			delete static_cast<META::ddl*>(h->userData);
			h->userData = nullptr;
		}
	}
	static inline META::columnMeta* getColumn(handle* h)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		switch (ddl->m_type)
		{
		case META::CREATE_TABLE:
			return &static_cast<META::createTableDDL*>(ddl)->tableDef.m_columns[static_cast<META::createTableDDL*>(ddl)->tableDef.m_columnsCount - 1];
		case META::ALTER_TABLE:
		{
			META::alterTableHead* alterInfo = *static_cast<const META::alterTable*>(ddl)->detail.rbegin();
			switch (alterInfo->type)
			{
			case META::ALTER_TABLE_ADD_COLUMN:
				return &static_cast<META::addColumn*>(alterInfo)->column;
			case META::ALTER_TABLE_ADD_COLUMNS:
				return (*static_cast<META::addColumns*>(alterInfo)->columns.rbegin());
			case META::ALTER_TABLE_CHANGE_COLUMN:
				return &static_cast<META::changeColumn*>(alterInfo)->newColumn;
			case META::ALTER_TABLE_MODIFY_COLUMN:
				return &static_cast<META::modifyColumn*>(alterInfo)->column;
			default:
				return nullptr;
			}
		}
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
			static_cast<META::createTableDDL*>(ddl)->tableDef.addColumn(&column);
			return &static_cast<META::createTableDDL*>(ddl)->tableDef.m_columns[static_cast<META::createTableDDL*>(ddl)->tableDef.m_columnsCount - 1];
		}
		case META::ALTER_TABLE:
		{
			META::alterTableHead* alterInfo = *static_cast<const META::alterTable*>(ddl)->detail.rbegin();
			switch (alterInfo->type)
			{
			case META::ALTER_TABLE_ADD_COLUMN:
			{
				static_cast<META::addColumn*>(alterInfo)->column.m_columnName.assign(name);
				return &static_cast<META::addColumn*>(alterInfo)->column;
			}
			case META::ALTER_TABLE_ADD_COLUMNS:
			{
				META::columnMeta *column = new META::columnMeta();
				column->m_columnName.assign(name);
				static_cast<META::addColumns*>(alterInfo)->columns.push_back(column);
				return column;
			}
			case META::ALTER_TABLE_CHANGE_COLUMN:
			{
				static_cast<META::changeColumn*>(alterInfo)->newColumn.m_columnName.assign(name);
				return &static_cast<META::changeColumn*>(alterInfo)->newColumn;
			}
			case META::ALTER_TABLE_MODIFY_COLUMN:
			{
				static_cast<META::modifyColumn*>(alterInfo)->column.m_columnName.assign(name);
				return &static_cast<META::modifyColumn*>(alterInfo)->column;
			}
			default:
				return nullptr;
			}
		}

		default:
			return nullptr;
		}
	}
	extern "C" DLL_EXPORT  parseValue bitType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_BIT];
		column->m_srcColumnType = MYSQL_TYPE_BIT;
		column->m_size = 1;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bitTypeSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_size = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY];
		column->m_srcColumnType = MYSQL_TYPE_TINY;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue BoolType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY];
		column->m_srcColumnType = MYSQL_TYPE_TINY;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue numertypeIsUnsigned(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		switch (column->m_srcColumnType)
		{
		case MYSQL_TYPE_TINY:
			column->m_columnType = META::COLUMN_TYPE::T_UINT8;
			break;
		case MYSQL_TYPE_SHORT:
			column->m_columnType = META::COLUMN_TYPE::T_UINT16;
			break;
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			column->m_columnType = META::COLUMN_TYPE::T_UINT32;
			break;
		case MYSQL_TYPE_LONGLONG:
			column->m_columnType = META::COLUMN_TYPE::T_UINT64;
			break;
		}
		column->m_signed = false;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue smallIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_SHORT];
		column->m_srcColumnType = MYSQL_TYPE_SHORT;
		column->m_signed = true;
		return OK;
	}

	extern "C" DLL_EXPORT   parseValue mediumIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_INT24];
		column->m_srcColumnType = MYSQL_TYPE_INT24;
		column->m_signed = true;
		return OK;
	}

	extern "C" DLL_EXPORT  parseValue intType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONG];
		column->m_srcColumnType = MYSQL_TYPE_LONG;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bigIntType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONGLONG];
		column->m_srcColumnType = MYSQL_TYPE_LONGLONG;
		column->m_signed = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue decimalType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_NEWDECIMAL];
		column->m_srcColumnType = MYSQL_TYPE_NEWDECIMAL;
		column->m_signed = true;
		column->m_size = column->m_precision = 10;
		column->m_decimals = 0;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_size = column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatDigitsSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_decimals = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatType(handle* h, SQLValue * value)
	{

		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_FLOAT];
		column->m_srcColumnType = MYSQL_TYPE_FLOAT;
		column->m_signed = true;
		column->m_size = column->m_precision = 12;
		column->m_decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue doubleType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DOUBLE];
		column->m_srcColumnType = MYSQL_TYPE_DOUBLE;
		column->m_signed = true;
		column->m_size = column->m_precision = 22;
		column->m_decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATETIME];
		column->m_srcColumnType = MYSQL_TYPE_DATETIME;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATETIME2];
		column->m_srcColumnType = MYSQL_TYPE_DATETIME2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIMESTAMP];
		column->m_srcColumnType = MYSQL_TYPE_TIMESTAMP;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue dateType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_DATE];
		column->m_srcColumnType = MYSQL_TYPE_DATE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIMESTAMP2];
		column->m_srcColumnType = MYSQL_TYPE_TIMESTAMP2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIME];//todo
		column->m_srcColumnType = MYSQL_TYPE_TIME;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TIME2];
		column->m_srcColumnType = MYSQL_TYPE_TIME2;
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_YEAR];
		column->m_srcColumnType = MYSQL_TYPE_YEAR;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearTypePrec(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_precision = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue charType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_STRING];
		column->m_srcColumnType = MYSQL_TYPE_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varcharType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_VARCHAR];
		column->m_srcColumnType = MYSQL_TYPE_VARCHAR;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeCharSet(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_charset = getCharset(static_cast<SQLStringValue*>(value)->value);
		if (column->m_charset == nullptr)
			return NOT_SUPPORT;
		if (column->m_size > 0)
			column->m_size *= column->m_charset->byteSizePerChar;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeSize(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_size = static_cast<SQLIntNumberValue*>(value)->number;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue binaryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_BINARY;
		column->m_srcColumnType = MYSQL_TYPE_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varbinaryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_BINARY;
		column->m_srcColumnType = MYSQL_TYPE_VAR_STRING;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyBlobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_TINY_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_TINY_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue blobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumBlobType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_MEDIUM_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_MEDIUM_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longBlobtype(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_LONG_BLOB];
		column->m_srcColumnType = MYSQL_TYPE_LONG_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyTextType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_TINY_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue textType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumTextType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_MEDIUM_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longTexttype(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = META::COLUMN_TYPE::T_TEXT;
		column->m_srcColumnType = MYSQL_TYPE_LONG_BLOB;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue enumType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_ENUM];
		column->m_srcColumnType = MYSQL_TYPE_ENUM;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue setType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_SET];
		column->m_srcColumnType = MYSQL_TYPE_SET;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  enumOrSetValueList(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_setAndEnumValueList.add(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue geometryType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_GEOMETRY];
		column->m_srcColumnType = MYSQL_TYPE_GEOMETRY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue jsonType(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_columnType = mysqlTypeMaps[MYSQL_TYPE_JSON];
		column->m_srcColumnType = MYSQL_TYPE_JSON;
		column->m_charset = &charsets[utf8mb4];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newColumn(handle* h, SQLValue * value)
	{
		createColume(h, static_cast<SQLNameValue*>(value)->name.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  columnCollate(handle* h, SQLValue* value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_collate = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue generatedColumn(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_generated = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsUK(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_isUnique = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsPK(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_isPrimary = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsKey(handle* h, SQLValue * value)
	{
		META::columnMeta* column = getColumn(h);
		column->m_isIndex = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue primaryKeys(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE)//create table also use primaryKeys,but do not need to create it
		{
			META::addKey* pk = new META::addKey;
			pk->type = META::ALTER_TABLE_ADD_PRIMARY_KEY;
			pk->name = "PRIMARY KEY";
			static_cast<META::alterTable*>(ddl)->detail.push_back(pk);
		}
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue primaryKeyColumn(handle* h, SQLValue* value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE)
		{
			META::addKey* pk = static_cast<META::addKey*>(*static_cast<META::alterTable*>(ddl)->detail.rbegin());
			pk->columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else if (ddl->m_type == META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->primaryKey.columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue createUniqueKey(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE)
		{
			META::addKey* uk = new META::addKey;
			uk->type = META::ALTER_TABLE_ADD_UNIQUE_KEY;
			static_cast<META::alterTable*>(ddl)->detail.push_back(uk);
		}
		else if (ddl->m_type == META::CREATE_TABLE)
		{
			META::addKey uk;
			static_cast<META::createTableDDL*>(ddl)->uniqueKeys.push_back(uk);
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue uniqueKeyName(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::ALTER_TABLE)
		{
			META::addKey* uk = static_cast<META::addKey*>(*static_cast<META::alterTable*>(ddl)->detail.rbegin());
			uk->name = static_cast<SQLNameValue*>(value)->name;
		}
		else if (ddl->m_type == META::CREATE_TABLE)
		{
			META::addKey & uk = *static_cast<META::createTableDDL*>(ddl)->uniqueKeys.rbegin();
			uk.name = static_cast<SQLNameValue*>(value)->name;
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
			META::addKey* uk = static_cast<META::addKey*>(*static_cast<META::alterTable*>(ddl)->detail.rbegin());
			uk->columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else if (ddl->m_type == META::CREATE_TABLE)
		{
			META::addKey &uk = *static_cast<META::createTableDDL*>(ddl)->uniqueKeys.rbegin();
			uk.columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		}
		else if(ddl->m_type == META::ALTER_TABLE)
		{
			alterTableHead * lastAlter = *static_cast<META::alterTable*>(ddl)->detail.rbegin();
			if(lastAlter->type==META::ALTER_TABLE_ADD_UNIQUE_KEY)
			{

				META::addKey* uk = static_cast<META::addKey*>(lastAlter);
				uk->columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
			}
			else
				return INVALID;
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tableCharset(handle* h, SQLValue * value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		const charsetInfo * charset = getCharset(static_cast<SQLStringValue*>(value)->value);
		if (charset == nullptr)
			return NOT_SUPPORT;
		if (ddl->m_type == META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->tableDef.m_charset = charset;
		}
		else if (ddl->m_type == META::ALTER_TABLE_ADD_COLUMN)
		{
			META::alterTableHead* alterInfo = *static_cast<META::alterTable*>(ddl)->detail.rbegin();
			if (alterInfo->type == META::ALTER_TABLE_DEFAULT_CHARSET || alterInfo->type == META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET)
				static_cast<META::defaultCharset*>(alterInfo)->charset = charset;
			else
				return NOT_MATCH;
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue changeTableCharset(handle* h, SQLValue* value)
	{
		META::defaultCharset* ddl = new META::defaultCharset;
		ddl->type = META::ALTER_TABLE_DEFAULT_CHARSET;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue converTableCharset(handle* h, SQLValue* value)
	{
		META::defaultCharset* ddl = new META::defaultCharset;
		ddl->type = META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tableCollate(handle* h, SQLValue* value)
	{
		META::ddl* ddl = static_cast<META::ddl*>(h->userData);
		if (ddl->m_type == META::CREATE_TABLE)
		{
			static_cast<META::createTableDDL*>(ddl)->tableDef.m_collate = static_cast<SQLStringValue*>(value)->value;
		}
		else if (ddl->m_type == META::ALTER_TABLE_ADD_COLUMN)
		{
			META::alterTableHead* alterInfo = *static_cast<META::alterTable*>(ddl)->detail.rbegin();
			if (alterInfo->type == META::ALTER_TABLE_DEFAULT_CHARSET || alterInfo->type == META::ALTER_TABLE_CONVERT_DEFAULT_CHARSET)
				static_cast<META::defaultCharset*>(alterInfo)->collate = static_cast<SQLStringValue*>(value)->value;
			else
				return NOT_MATCH;
		}
		else
			return NOT_MATCH;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newTable(handle* h, SQLValue * value)
	{
		META::tableDdl* table = getTableInfo();
		table->usedDb = h->dbName;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  newTableName(handle * h, SQLValue * value)
	{
		META::tableDdl* table = getTableInfo();
		table->table.database = static_cast<SQLTableNameValue*>(value)->database;
		table->table.table = static_cast<SQLTableNameValue*>(value)->table;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue createTable(handle * h, SQLValue * value)
	{
		META::createTableDDL* table = new META::createTableDDL();
		copyTableHeadInfo(table);
		table->tableDef.m_tableName = table->table.table;
		table->tableDef.m_dbName = table->table.database;
		setDdl(h, table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createTableLike(handle* h, SQLValue * value)
	{
		META::createTableLike* table = new META::createTableLike;
		table->m_type = META::CREATE_TABLE_LIKE;
		copyTableHeadInfo(table);
		setDdl(h, table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  newLikedTableName(handle * h, SQLValue * value)
	{
		static_cast<META::createTableLike*>(h->userData)->likedTable.database = static_cast<SQLTableNameValue*>(value)->database;
		static_cast<META::createTableLike*>(h->userData)->likedTable.table = static_cast<SQLTableNameValue*>(value)->table;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableAddColumn(handle * h, SQLValue * value)
	{
		META::addColumn* ddl = new META::addColumn();
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableAddColumns(handle * h, SQLValue * value)
	{
		META::addColumns* ddl = new META::addColumns();
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAtFirst(handle* h, SQLValue * value)
	{
		static_cast<META::addColumn*>(GET_CURRENT_ALTER_TABLE_INFO())->first = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAfter(handle* h, SQLValue * value)
	{
		static_cast<META::addColumn*>(GET_CURRENT_ALTER_TABLE_INFO())->afterColumnName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterDropIndex(handle* h, SQLValue * value)
	{
		META::dropKey* ddl = new META::dropKey;
		ddl->type = META::ALTER_TABLE_DROP_INDEX;
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumn(handle* h, SQLValue * value)
	{
		META::changeColumn* ddl = new META::changeColumn;
		ddl->first = false;
		ddl->type = META::ALTER_TABLE_CHANGE_COLUMN;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumnName(handle* h, SQLValue * value)
	{
		static_cast<META::changeColumn*>(GET_CURRENT_ALTER_TABLE_INFO())->srcColumnName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropColumn(handle* h, SQLValue * value)
	{
		META::dropColumn* ddl = new META::dropColumn;
		ddl->type = META::ALTER_TABLE_DROP_COLUMN;
		ddl->columnName = static_cast<SQLNameValue*>(value)->name;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropPrimaryKey(handle* h, SQLValue * value)
	{
		META::dropKey * ddl = new META::dropKey;
		ddl->type = META::ALTER_TABLE_DROP_PRIMARY_KEY;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropForeignKey(handle* h, SQLValue * value)
	{
		META::dropKey* ddl = new META::dropKey;
		ddl->type = META::ALTER_TABLE_DROP_FOREIGN_KEY;
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		static_cast<META::alterTable*>(h->userData)->detail.push_back(ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTable(handle* h, SQLValue * value)
	{
		META::alterTable* table = new META::alterTable();
		table->usedDb = h->dbName;
		setDdl(h, table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createNewIndex(handle * h, SQLValue * value)
	{
		setDdl(h, new META::createIndex());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createNewIndexIsUk(handle * h, SQLValue * value)
	{
		static_cast<struct META::createIndex*>(h->userData)->index.type = META::ALTER_TABLE_ADD_UNIQUE_KEY;
		return OK;
	}
	/*now we only parse uk ,key/index not parse*/
	extern "C" DLL_EXPORT  parseValue  createIndexName(handle* h, SQLValue * value)
	{
		static_cast<struct META::createIndex*>(h->userData)->index.name = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  ddlTableName(handle * h, SQLValue * value)
	{
		static_cast<META::tableDdl*>(h->userData)->table.database = static_cast<SQLTableNameValue*>(value)->database;
		static_cast<META::tableDdl*>(h->userData)->table.table = static_cast<SQLTableNameValue*>(value)->table;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue createIndexColumn(handle * h, SQLValue * value)
	{
		static_cast<struct META::createIndex*>(h->userData)->index.columnNames.push_back(static_cast<SQLNameValue*>(value)->name);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndex(handle * h, SQLValue * value)
	{
		setDdl(h, new META::dropIndex());
		return OK;
	}
	
	extern "C" DLL_EXPORT  parseValue  dropIndexName(handle* h, SQLValue * value)
	{
		META::dropIndex* ddl = static_cast<META::dropIndex*>(h->userData);
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTable(handle* h, SQLValue * value)
	{
		setDdl(h, new META::dropTable());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTableName(handle* h, SQLValue * value)
	{
		META::tableName table;
		table.database = static_cast<SQLTableNameValue*>(value)->database;
		table.table = static_cast<SQLTableNameValue*>(value)->table;
		static_cast<META::dropTable*>(h->userData)->tables.push_back(table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTable(handle * h, SQLValue * value)
	{
		setDdl(h, new META::renameTable());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameSrcTable(handle* h, SQLValue * value)
	{
		META::tableName table;
		table.database = static_cast<SQLTableNameValue*>(value)->database;
		table.table = static_cast<SQLTableNameValue*>(value)->table;
		static_cast<META::renameTable*>(h->userData)->src.push_back(table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameDestTable(handle* h, SQLValue * value)
	{
		META::tableName table;
		table.database = static_cast<SQLTableNameValue*>(value)->database;
		table.table = static_cast<SQLTableNameValue*>(value)->table;
		static_cast<META::renameTable*>(h->userData)->dest.push_back(table);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterRenameKey(handle * h, SQLValue * value)
	{
		static_cast<META::alterTable*>(h->userData)->detail.push_back(new META::renameKey());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameOldKeyName(handle * h, SQLValue * value)
	{
		static_cast<struct META::renameKey*>(h->userData)->srcKeyName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameNewKeyName(handle * h, SQLValue * value)
	{
		static_cast<struct META::renameKey*>(h->userData)->destKeyName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterRenameColumn(handle * h, SQLValue * value)
	{
		static_cast<META::alterTable*>(h->userData)->detail.push_back(new META::renameColumn());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameOldColumn(handle * h, SQLValue * value)
	{
		static_cast<struct META::renameColumn*>(h->userData)->srcColumnName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameNewColumn(handle * h, SQLValue * value)
	{
		static_cast<struct META::renameColumn*>(h->userData)->destColumnName = static_cast<SQLNameValue*>(value)->name;
		return OK;
	}
	
	extern "C" DLL_EXPORT  parseValue  useDatabase(handle* h, SQLValue * value)
	{
		h->dbName = static_cast<SQLStringValue*>(value)->value;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterDatabase(handle* h, SQLValue * value)
	{
		META::dataBaseDDL* ddl = new META::dataBaseDDL();
		ddl->m_type = META::ALTER_DATABASE;
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		setDdl(h, ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createDatabase(handle* h, SQLValue * value)
	{
		META::dataBaseDDL * ddl = new META::dataBaseDDL();
		ddl->m_type = META::CREATE_DATABASE;
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		setDdl(h, ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropDatabase(handle* h, SQLValue * value)
	{
		META::dataBaseDDL* ddl = new META::dataBaseDDL();
		ddl->m_type = META::DROP_DATABASE;
		ddl->name = static_cast<SQLNameValue*>(value)->name;
		setDdl(h, ddl);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  databaseCharset(handle* h, SQLValue * value)
	{
		static_cast<META::dataBaseDDL*>(h->userData)->charset = getCharset(static_cast<SQLStringValue*>(value)->value);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  databaseCollate(handle* h, SQLValue* value)
	{
		static_cast<META::dataBaseDDL*>(h->userData)->collate = static_cast<SQLStringValue*>(value)->value;;
		return OK;
	}
}

