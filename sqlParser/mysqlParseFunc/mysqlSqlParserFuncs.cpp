/*
 * sqlParserFuncs.cpp
 *
 *  Created on: 2018年11月19日
 *      Author: liwei
 */
#include <assert.h>
#include "../../meta/metaChangeInfo.h"
#include "../../meta/charset.h"
#include "../sqlParserHandle.h"
#include  "../../util/winDll.h"
using namespace META;
namespace SQL_PARSER {
#define  NOT_FIXED_DEC 31
#define createMeta(h) (h)->userData = new metaChangeInfo();
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
	extern "C" DLL_EXPORT  parseValue bitType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_BIT;
		c->type = mysqlTypeMaps[c->rawType];
		c->size = 1;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bitTypeSize(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->size = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyIntType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TINY;
		c->type = mysqlTypeMaps[c->rawType];
		c->isSigned = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue BoolType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TINY;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue numertypeIsUnsigned(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		switch (c->rawType)
		{
		case MYSQL_TYPE_TINY:
			c->type = T_UINT8;
			break;
		case MYSQL_TYPE_SHORT:
			c->type = T_UINT16;
			break;
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
			c->type = T_UINT32;
			break;
		case MYSQL_TYPE_LONGLONG:
			c->type = T_UINT64;
			break;
		}
		c->isSigned = false;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue smallIntType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_SHORT;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}

	extern "C" DLL_EXPORT   parseValue mediumIntType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_INT24;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}

	extern "C" DLL_EXPORT  parseValue intType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_LONG;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue bigIntType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_LONGLONG;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue decimalType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_NEWDECIMAL;
		c->type = mysqlTypeMaps[c->rawType];
		c->size = 10;
		c->decimals = 0;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatSize(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->size = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatDigitsSize(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->decimals = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue floatType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_FLOAT;
		c->type = mysqlTypeMaps[c->rawType];
		c->size = 12;
		c->decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue doubleType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_DOUBLE;
		c->type = mysqlTypeMaps[c->rawType];
		c->size = 22;
		c->decimals = NOT_FIXED_DEC;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_DATETIME;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue datetimeTypePrec(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_DATETIME2;
		c->type = mysqlTypeMaps[c->rawType];
		c->precision = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TIMESTAMP;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue dateType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_DATE;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timestampTypePrec(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TIMESTAMP2;
		c->type = mysqlTypeMaps[c->rawType];
		c->precision = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TIME;//todo
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue timeTypePrec(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TIME2;
		c->type = mysqlTypeMaps[c->rawType];
		c->precision = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_YEAR;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue yearTypePrec(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->precision = atoi(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue charType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_STRING;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varcharType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_VARCHAR;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeCharSet(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->charset = getCharset(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue stringTypeSize(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->size = atol(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue binaryType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_STRING;
		c->type = T_BINARY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue varbinaryType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_VAR_STRING;
		c->type = T_BINARY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyBlobType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TINY_BLOB;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue blobType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_BLOB;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumBlobType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_MEDIUM_BLOB;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longBlobtype(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_LONG_BLOB;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tinyTextType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_TINY_BLOB;
		c->type = T_TEXT;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue textType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_BLOB;
		c->type = T_TEXT;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue mediumTextType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_MEDIUM_BLOB;
		c->type = T_TEXT;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue longTexttype(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_LONG_BLOB;
		c->type = T_TEXT;	
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue enumType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_ENUM;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue setType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_SET;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  enumOrSetValueList(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->setAndEnumValueList.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue geometryType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_GEOMETRY;
		c->type = mysqlTypeMaps[c->rawType];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue jsonType(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->rawType = MYSQL_TYPE_JSON;
		c->type = mysqlTypeMaps[c->rawType];
		c->charset = &charsets[utf8mb4];
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newColumn(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = new newColumnInfo;
		c->name = sql;
		t->newColumns.push_back(c);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue generatedColumn(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->generated = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsUK(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = getLastColumn(h);
		c->isUnique = true;
		newKeyInfo* k = new newKeyInfo();
		k->type = newKeyInfo::UNIQUE_KEY;
		k->name = c->name;
		k->columns.push_back(c->name);
		t->newKeys.push_back(k);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsPK(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = getLastColumn(h);
		c->isPrimary = true;
		newKeyInfo* k = new newKeyInfo();
		k->type = newKeyInfo::PRIMARY_KEY;
		k->columns.push_back(c->name);
		t->newKeys.push_back(k);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue columnIsKey(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = getLastColumn(h);
		newKeyInfo* k = new newKeyInfo();
		k->type = newKeyInfo::KEY;
		k->name = c->name;
		k->columns.push_back(c->name);
		t->newKeys.push_back(k);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue constraintName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newKeyInfo* k = new newKeyInfo();
		k->name = sql;
		t->newKeys.push_back(k);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue primaryKeys(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		list<newKeyInfo*>::reverse_iterator iter = t->newKeys.rbegin();
		newKeyInfo* k;
		if (iter != t->newKeys.rend() && (*iter)->type == newKeyInfo::MAX_KEY_TYPE)
			k = (*iter);
		else
		{
			k = new newKeyInfo();
			t->newKeys.push_back(k);
		}
		k->type = newKeyInfo::PRIMARY_KEY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue uniqueKeys(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		list<newKeyInfo*>::reverse_iterator iter = t->newKeys.rbegin();
		newKeyInfo* k;
		if (iter != t->newKeys.rend() && (*iter)->type == newKeyInfo::MAX_KEY_TYPE)
			k = (*iter);
		else
		{
			k = new newKeyInfo();
			t->newKeys.push_back(k);
		}
		k->type = newKeyInfo::UNIQUE_KEY;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue uniqueKeyName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newKeyInfo* k = *(t->newKeys.rbegin());
		if (k->name.empty())
			k->name = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue keyColumn(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newKeyInfo* k = *(t->newKeys.rbegin());
		k->columns.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue tableCharset(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->defaultCharset = getCharset(sql.c_str());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue newTable(handle* h, const string& sql)
	{
		newTableInfo* t = new newTableInfo();
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue NewTableDBName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue NewTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->type = META::newTableInfo::CREATE_TABLE;
		t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createTableLike(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->createLike = true;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  NewTableLikedDBName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->likedTable.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  NewTableLikedTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->likedTable.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAtFirst(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->after = false;
		c->index = 0;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  AlterNewColumnAfter(handle* h, const string& sql)
	{
		newColumnInfo* c = getLastColumn(h);
		c->after = true;
		c->afterColumnName = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndex(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumn(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		newColumnInfo* c = getLastColumn(h);
		c->after = true;
		c->afterColumnName = *t->oldColumns.rbegin();
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterChangeColumnName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->oldColumns.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropColumn(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->oldColumns.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropPrimaryKey(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back("PRIMARY");
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropForeignKey(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->oldKeys.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTable(handle* h, const string& sql)
	{
		newTableInfo* t = new newTableInfo;
		t->type = newTableInfo::ALTER_TABLE;
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableDbName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterTableTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUK(handle* h, const string& sql)
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
	extern "C" DLL_EXPORT  parseValue  createUKName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			(*(t->newKeys.begin()))->name = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUKONDatabaseName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			t->table.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUKONTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  tableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createUkByColumn(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		if (t != NULL && t->table.database.empty() && t->table.table.empty() && t->type == newTableInfo::ALTER_TABLE && t->newKeys.size() == 1 && (*(t->newKeys.begin()))->type == newKeyInfo::UNIQUE_KEY)
			(*(t->newKeys.begin()))->columns.push_back(sql);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndexName(handle* h, const string& sql)
	{
		newTableInfo* t = new newTableInfo;
		t->type = newTableInfo::ALTER_TABLE;
		t->oldColumns.push_back(sql);
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	/*now we only parse uk ,key/index not parse*/
	extern "C" DLL_EXPORT  parseValue  dropIndexOnDataBaseName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropIndexOnTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTable(handle* h, const string& sql)
	{
		getMeta(h)->oldTables.push_back(Table());
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTableDatabaseName(handle* h, const string& sql)
	{
		(*getMeta(h)->oldTables.rbegin()).database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropTableTableName(handle* h, const string& sql)
	{
		(*getMeta(h)->oldTables.rbegin()).table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableDatabaseName(handle* h, const string& sql)
	{
		getMeta(h)->oldTables.push_back(Table(sql, ""));
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableTableName(handle* h, const string& sql)
	{
		Table& t = *getMeta(h)->oldTables.rbegin();
		if (t.table.empty() && !t.database.empty())
			t.table = sql;
		else
			getMeta(h)->oldTables.push_back(Table("", sql));
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameNewTable(handle* h, const string& sql)
	{
		newTableInfo* t = new newTableInfo();
		t->createLike = true;
		t->likedTable = *getMeta(h)->oldTables.rbegin();
		getMeta(h)->newTables.push_back(t);
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableToDatabaseName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.database = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  renameTableToTableName(handle* h, const string& sql)
	{
		newTableInfo* t = getLastTable(h);
		t->table.table = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  useDatabase(handle* h, const string& sql)
	{
		h->dbName = sql;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  alterDatabase(handle* h, const string& sql)
	{
		getMeta(h)->database.name = sql;
		getMeta(h)->database.type = databaseInfo::ALTER_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  createDatabase(handle* h, const string& sql)
	{
		getMeta(h)->database.name = sql;
		getMeta(h)->database.type = databaseInfo::CREATE_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  dropDatabase(handle* h, const string& sql)
	{
		getMeta(h)->database.name = sql;
		getMeta(h)->database.type = databaseInfo::DROP_DATABASE;
		return OK;
	}
	extern "C" DLL_EXPORT  parseValue  databaseCharset(handle* h, const string& sql)
	{
		getMeta(h)->database.charset = getCharset(sql.c_str());
		return OK;
	}
}

