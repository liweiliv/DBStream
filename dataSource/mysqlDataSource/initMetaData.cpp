#include <map>
#include <string>
#include <string.h>
#include <thread>
#include"initMetaData.h"
#include "glog/logging.h"
#include "meta/metaData.h"
#include "util/winString.h"
#include "util/unorderMapUtil.h"
#include "meta/mysqlTypes.h"
namespace DATA_SOURCE {
	static constexpr auto SELECT_ALL_COLUMNS  = "select COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,TABLE_SCHEMA,TABLE_NAME,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME, COLUMN_TYPE,GENERATION_EXPRESSION,CHARACTER_OCTET_LENGTH FROM information_schema.columns where TABLE_SCHEMA in (%s)";
	static constexpr auto SELECT_ALL_CONSTRAINT = "select CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_TYPE from information_schema.TABLE_CONSTRAINTS where TABLE_SCHEMA in (%s)";
	static constexpr auto SELECT_ALL_KEY_COLUMN = "select CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA in (%s)";
	static constexpr auto SELECT_ALL_TABLE = "select TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,TABLE_COLLATION from information_schema.TABLES where TABLE_SCHEMA in (%s)";
	static constexpr auto SELECT_ALL_SCHEMA = "select SCHEMA_NAME ,DEFAULT_CHARACTER_SET_NAME from information_schema.SCHEMATA where SCHEMA_NAME in (%s) and SCHEMA_NAME not in('mysql','information_schema','performance_schema','sys')";
	static constexpr auto SELECT_ALL_USER_SCHEMA = "select SCHEMA_NAME  from information_schema.SCHEMATA where SCHEMA_NAME not in('mysql','information_schema','performance_schema','sys')";

	initMetaData::initMetaData(mysqlConnector* connector) :m_connector(connector), m_conn(nullptr)
	{
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("int", MYSQL_TYPE_LONG));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("tinyint", MYSQL_TYPE_TINY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("smallint", MYSQL_TYPE_SHORT));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("mediumint", MYSQL_TYPE_INT24));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("bigint", MYSQL_TYPE_LONGLONG));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("decimal", MYSQL_TYPE_NEWDECIMAL));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("float", MYSQL_TYPE_FLOAT));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("double",  MYSQL_TYPE_DOUBLE));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("bit", MYSQL_TYPE_BIT));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("datetime", MYSQL_TYPE_DATETIME2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("timestamp", MYSQL_TYPE_TIMESTAMP2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("time", MYSQL_TYPE_TIME2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("date",  MYSQL_TYPE_NEWDATE));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("year", MYSQL_TYPE_YEAR));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("char", MYSQL_TYPE_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("varchar", MYSQL_TYPE_VARCHAR));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("binary",  MYSQL_TYPE_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("varbinary", MYSQL_TYPE_VAR_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("tinytext", MYSQL_TYPE_TINY_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("mediumtext", MYSQL_TYPE_MEDIUM_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("text", MYSQL_TYPE_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("longtext",MYSQL_TYPE_LONG_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("tinyblob",  MYSQL_TYPE_TINY_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("mediumblob", MYSQL_TYPE_MEDIUM_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("blob", MYSQL_TYPE_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("longblob", MYSQL_TYPE_LONG_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("json",MYSQL_TYPE_JSON));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("enum",  MYSQL_TYPE_ENUM));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("set",MYSQL_TYPE_SET));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("geometry", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("point", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("linestring", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("polygon", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multipoint", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multilinestring", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multipolygon", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("geomcollection", MYSQL_TYPE_GEOMETRY));
	}
	static int parseEnumOrSetValueList(META::columnMeta* column, const char* values)
	{
		const char* pos = strchr(values, '\'') + 1;
		if (pos == nullptr)
		{
			LOG(ERROR) << "enum or set column:" << column->m_columnName << " value array is illegal:" << values;
			return -1;
		}
		std::vector<std::string> valueList;
		const char* valueBegin = pos;
		while (*pos != '\0')
		{
			if (*pos == '\'' && *(pos - 1) != '\\')
			{
				valueList.push_back(std::string(valueBegin, pos - valueBegin));
				pos++;
				while (*pos == ' ' || *pos == '\t')
					pos++;
				if (*pos == ')')
					break;
				if (*pos != ',')
				{
					LOG(ERROR) << "enum or set column:" << column->m_columnName << " value array is illegal:" << values;
					return -1;
				}
				pos++;
				while (*pos == ' ' || *pos == '\t')
					pos++;
				if (*pos != '\'')
				{
					LOG(ERROR) << "enum or set column:" << column->m_columnName << " value array is illegal:" << values;
					return -1;
				}
				valueBegin = pos + 1;
			}
			pos++;
		}
		column->m_setAndEnumValueList.m_count = 0;
		column->m_setAndEnumValueList.m_array = (char**)malloc(sizeof(char*)* valueList.size());
		for (std::vector<std::string>::const_iterator iter = valueList.begin(); iter != valueList.end(); iter++)
		{
			column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count] = (char*)malloc((*iter).size() + 1);
			memcpy(column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count], (*iter).c_str(), (*iter).size());
			column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count][(*iter).size()] = '\0';
			column->m_setAndEnumValueList.m_count++;
		}
		return 0;
	}
	int initMetaData::getColumnInfo(META::columnMeta* column, MYSQL_ROW row)
	{
		column->m_columnName = row[0];
		column->m_columnIndex = atoi(row[1])-1;
		MYSQL_TYPE_TREE::const_iterator iter = m_mysqlTyps.find(row[2]);
		if (iter == m_mysqlTyps.end())
		{
			LOG(ERROR) << "column:" << row[3] << "." << row[4] << "." << row[0] << " has unknown column type:" << row[2];
			return -1;
		}
		column->m_srcColumnType = iter->second;
		column->m_columnType = mysqlTypeMaps[column->m_srcColumnType];
		if (row[5] != nullptr)
			column->m_precision = atoi(row[5]);
		if (row[6] != nullptr)
			column->m_decimals = atoi(row[6]);
		if (row[7] != nullptr)
			column->m_decimals = atoi(row[7]);
		if (row[8] != nullptr)
			column->m_charset = getCharset(row[8]);
		if (column->m_srcColumnType == MYSQL_TYPE_ENUM || column->m_srcColumnType == MYSQL_TYPE_SET)
		{
			if (row[9] != nullptr && 0 != parseEnumOrSetValueList(column, row[9]))
			{
				LOG(ERROR) << "enum or set column:" << row[3] << "." << row[4] << "." << row[0] << " value array is illegal:" << row[9];
				return -1;
			}
		}
		else if (column->m_srcColumnType == MYSQL_TYPE_TINY || column->m_srcColumnType == MYSQL_TYPE_SHORT ||
			column->m_srcColumnType == MYSQL_TYPE_INT24 || column->m_srcColumnType == MYSQL_TYPE_LONG || column->m_srcColumnType == MYSQL_TYPE_LONGLONG)
		{
			if (row[9] != nullptr && strstr(row[9], "unsigned"))
				column->m_signed = false;
			else
				column->m_signed = true;
		}
		if (row[10] != nullptr && strlen(row[10]) > 0)
			column->m_generated = true;
		if (row[11] != nullptr && !META::columnInfos[TID(column->m_columnType)].fixed)
			column->m_size = (uint64_t)atol(row[11]);
		return 0;
	}
	int initMetaData::loadAllColumns(META::metaDataCollection* collection,const std::vector<std::string>& databases)
	{
		int ret = 0;
		MYSQL_RES  *rs = doQuery(SELECT_ALL_COLUMNS, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "load column failed";
			return -1;
		}
		std::map < std::string, std::map<std::string, std::map<int, META::columnMeta*>* >* > allColumns;
		MYSQL_ROW row;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			META::tableMeta* meta = collection->get(row[3], row[4], 1);
			if (meta == nullptr)
				continue;
			META::columnMeta* column = new META::columnMeta();
			if (0 != getColumnInfo(column, row))
			{
				LOG(ERROR) << "create column define failed";
				delete column;
				ret = -1;
				break;
			}
			std::map < std::string, std::map<std::string, std::map<int, META::columnMeta*>* >* >::iterator databaseIter = allColumns.find(row[3]);
			std::map < std::string, std::map<int, META::columnMeta*>*>* database;
			if (databaseIter == allColumns.end())
			{
				database = new std::map < std::string, std::map<int, META::columnMeta*>*>();
				allColumns.insert(std::pair<std::string, std::map<std::string, std::map<int, META::columnMeta*>* >*>(row[3], database));
			}
			else
				database = databaseIter->second;
			std::map< std::string, std::map<int, META::columnMeta*>*>::iterator tableIter = database->find(row[4]);
			std::map<int, META::columnMeta*> * table;
			if (tableIter == database->end())
			{
				table = new std::map<int, META::columnMeta*> ();
				database->insert(std::pair<std::string, std::map<int, META::columnMeta*>*>(row[4], table));
			}
			else
				table = tableIter->second;
			if (!table->insert(std::pair<int, META::columnMeta*>(column->m_columnIndex, column)).second)
			{
				LOG(ERROR) << "column:" << row[3] << "." << row[4] << "." << row[0] << " has the same column id :" << column->m_columnIndex <<
					" with column:" << table->find(column->m_columnIndex)->second->m_columnName;
				delete column;
				ret = -1;
				break;
			}
		}
		mysql_free_result(rs);
		for (std::map < std::string, std::map<std::string, std::map<int, META::columnMeta*>* >* >::iterator databaseIter = allColumns.begin();
			databaseIter != allColumns.end(); databaseIter++)
		{
			for (std::map< std::string, std::map<int, META::columnMeta*>*>::iterator tableIter = databaseIter->second->begin(); tableIter != databaseIter->second->end(); tableIter++)
			{
				if (ret == 0)
				{
					META::tableMeta* meta = collection->get(databaseIter->first.c_str(), tableIter->first.c_str(), 1);
					meta->m_columns = new META::columnMeta[tableIter->second->size()];
					for (std::map<int, META::columnMeta*>::iterator columnIter = tableIter->second->begin(); columnIter != tableIter->second->end(); columnIter++)
						meta->m_columns[meta->m_columnsCount++] = *columnIter->second;
					meta->buildColumnOffsetList();
				}
				for (std::map<int, META::columnMeta*>::iterator columnIter = tableIter->second->begin(); columnIter != tableIter->second->end(); columnIter++)
					delete columnIter->second;
				delete tableIter->second;
			}
			delete databaseIter->second;
		}
		return ret;
	}
	struct keyInfo {
		std::map<int, uint16_t> columns;
		META::KEY_TYPE type;
	};
	int initMetaData::loadAllKeyColumns(META::metaDataCollection* collection,const std::vector<std::string>& databases, std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* > &constraints)
	{
		MYSQL_RES  *rs = doQuery(SELECT_ALL_KEY_COLUMN, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "load key column failed";
			return -1;
		}
		MYSQL_ROW row;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			META::tableMeta* meta = collection->get(row[2], row[3], 1);
			META::columnMeta* column;
			if (meta == nullptr || (column = (META::columnMeta*)meta->getColumn(row[4])) == nullptr)
				continue;
			std::map < std::string, std::map<std::string, std::map<std::string, keyInfo* >* >* >::iterator databaseIter
				= constraints.find(row[2]);
			std::map<std::string, std::map<std::string, keyInfo* >* >* database;
			if (databaseIter == constraints.end())
				continue;
			else
				database = databaseIter->second;
			std::map<std::string, std::map<std::string, keyInfo* >* >::iterator tableIter = database->find(row[3]);
			std::map<std::string, keyInfo* >* table;
			if (tableIter == database->end())
				continue;
			else
				table = tableIter->second;
			std::map<std::string, keyInfo* >::iterator keyIter = table->find(row[1]);
			if (keyIter == table->end())
				continue;
			keyIter->second->columns.insert(std::pair<int, uint16_t>(atoi(row[5]), column->m_columnIndex));
		}
		mysql_free_result(rs);
		return 0;
	}
	MYSQL_RES* initMetaData::doQuery(const char* _sql, const std::vector<std::string>& databases)
	{
		if (m_conn == nullptr && (m_conn = m_connector->getConnect())==nullptr)
		{
			LOG(ERROR) << "can not create connect to mysql server";
			return nullptr;
		}
		std::string dbList;
		char escapeBuffer[512] = { 0 };
		for (std::vector<std::string>::const_iterator iter = databases.begin(); iter != databases.end(); iter++)
		{
			if (iter != databases.begin())
				dbList.append(",");
			dbList.append("'");
			uint32_t size = mysql_real_escape_string_quote(m_conn, escapeBuffer, (*iter).c_str(), (*iter).size(), '\'');
			dbList.append(escapeBuffer, size);
			dbList.append("'");
		}
		char* sql = new char[dbList.size() + strlen(_sql)+1];
		sprintf(sql, _sql, dbList.c_str());
		MYSQL_RES* rs = nullptr;
		for (int retry = 0; retry < 10; retry++)
		{
			if (0 != mysql_query(m_conn, sql))
			{
				LOG(ERROR) << "sql:" << sql << " query failed for:" << mysql_errno(m_conn) << "," << mysql_error(m_conn);
				goto CHECK_ERROR;
			}
			rs = mysql_store_result(m_conn);
			if (rs == nullptr)
			{
				LOG(ERROR) << "can not get any result from database";
				goto CHECK_ERROR;
			}
			delete []sql;
			return rs;
		CHECK_ERROR:
			if (mysql_errno(m_conn) == 2003 || mysql_errno(m_conn) == 2006 || mysql_errno(m_conn) == 2013)
			{
				std::this_thread::sleep_for(std::chrono::seconds(1));
				mysql_close(m_conn);
				if (nullptr == (m_conn = m_connector->getConnect()))
				{
					LOG(ERROR) << "can not create connect to mysql server";
					return nullptr;
				}
				continue;
			}
			else
				break;
		}
		delete []sql;
		return nullptr;
	}
	int initMetaData::loadAllConstraint(META::metaDataCollection* collection, const std::vector<std::string>& databases)
	{
		int ret = 0;
		std::map < std::string, std::map<std::string, std::map<std::string,  keyInfo*>* >* > constraints;
		MYSQL_RES  *rs = doQuery(SELECT_ALL_CONSTRAINT, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "load constraint failed";
			return -1;
		}
		MYSQL_ROW row;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			META::tableMeta* meta = collection->get(row[2], row[3], 1);
			if (meta == nullptr)
				continue;
			META::KEY_TYPE type = META::KEY_TYPE::PRIMARY_KEY;
			if (strncasecmp(row[4], "PRIMARY KEY", 11) == 0)
				type = META::KEY_TYPE::PRIMARY_KEY;
			else if (strncasecmp(row[4], "UNIQUE", 6) == 0)
			{
				type = META::KEY_TYPE::UNIQUE_KEY;
				meta->m_uniqueKeysCount++;
			}
			else if (strncasecmp(row[4], "KEY", 3) == 0)
			{
				type = META::KEY_TYPE::INDEX;
				meta->m_indexCount++;
			}
			else
				continue;
			std::map < std::string, std::map<std::string, std::map<std::string, keyInfo* >* >* >::iterator databaseIter
				= constraints.find(row[2]);
			std::map<std::string, std::map<std::string, keyInfo* >* >* database;
			if (databaseIter == constraints.end())
			{
				database = new std::map<std::string, std::map<std::string, keyInfo* >* >();
				constraints.insert(std::pair<std::string, std::map<std::string, std::map<std::string, keyInfo* >* >* >(row[2], database));
			}
			else
				database = databaseIter->second;
			std::map<std::string, std::map<std::string, keyInfo* >* >::iterator tableIter = database->find(row[3]);
			std::map<std::string, keyInfo* >* table;
			if (tableIter == database->end())
			{
				table = new  std::map<std::string, keyInfo* > ();
				database->insert(std::pair<std::string, std::map<std::string, keyInfo* >*  >(row[3], table));
			}
			else
				table = tableIter->second;
			keyInfo* key = new keyInfo();
			key->type = type;
			table->insert(std::pair<std::string, keyInfo*>(row[1], key));
		}
		mysql_free_result(rs);
		if (loadAllKeyColumns(collection, databases, constraints) != 0)
			ret = -1;
		uint16_t keyIdxs[256];
		for (std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >::iterator databaseIter = constraints.begin();
			databaseIter != constraints.end(); databaseIter++)
		{
			for (std::map<std::string, std::map<std::string, keyInfo*>* >::iterator tableIter = databaseIter->second->begin(); tableIter != databaseIter->second->end(); tableIter++)
			{
				if (ret == 0)
				{
					META::tableMeta* table = collection->get(databaseIter->first.c_str(), tableIter->first.c_str(), 1);
					if (table != nullptr)
					{
						if (table->m_uniqueKeysCount > 0)
							table->m_uniqueKeys = (META::unionKeyMeta**)malloc(sizeof(META::unionKeyMeta*) * table->m_uniqueKeysCount);
						table->m_uniqueKeyNames = new std::string[table->m_uniqueKeysCount];
						if (table->m_indexCount > 0)
							table->m_indexs = (META::unionKeyMeta**)malloc(sizeof(META::unionKeyMeta*) * table->m_indexCount);
						table->m_indexNames = new std::string[table->m_indexCount];
						uint16_t ukCount = 0, indexCount = 0;
						for (std::map<std::string, keyInfo*>::iterator keyIter = tableIter->second->begin(); keyIter != tableIter->second->end(); keyIter++)
						{
							int keyCount = 0;
							for (std::map<int, uint16_t>::const_iterator columnIter = keyIter->second->columns.begin(); columnIter != keyIter->second->columns.end(); columnIter++)
								keyIdxs[keyCount++] = columnIter->second;

							if (keyIter->second->type == META::KEY_TYPE::PRIMARY_KEY)
							{
								table->m_primaryKey = table->createUnionKey(0,META::KEY_TYPE::PRIMARY_KEY,keyIdxs,keyCount);
							}
							else if (keyIter->second->type == META::KEY_TYPE::UNIQUE_KEY)
							{
								table->m_uniqueKeys[ukCount] = table->createUnionKey(ukCount,META::KEY_TYPE::UNIQUE_KEY,keyIdxs,keyCount);
								table->m_uniqueKeyNames[ukCount++] = keyIter->first;
							}
							else
							{
								table->m_indexs[indexCount] = table->createUnionKey(ukCount,META::KEY_TYPE::INDEX,keyIdxs,keyCount);
								table->m_indexNames[indexCount++] = keyIter->first;
							}
						}
					}
				}
				for (std::map<std::string, keyInfo*>::iterator keyIter = tableIter->second->begin(); keyIter != tableIter->second->end(); keyIter++)
					delete keyIter->second;
				delete tableIter->second;
			}
			delete databaseIter->second;
		}
		return ret;
	}
	static std::string getCharsetFromCollation(const char* collation)
	{
		const char* ptr = strchr(collation, '_');
		if (ptr != nullptr)
			return std::string(collation, ptr - collation);
		else
			return collation;
	}
	int initMetaData::loadAllDataBases(META::metaDataCollection* collection, const std::vector<std::string>& databases)
	{
		MYSQL_RES  *rs = doQuery(SELECT_ALL_SCHEMA, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "load database failed";
			return -1;
		}
		MYSQL_ROW row;
		const charsetInfo* charset;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			if (row[1] != nullptr && strlen(row[1]) > 0)
			{
				if (nullptr == (charset = getCharset(row[1])))
				{
					LOG(ERROR) << "unkonw charset:" << row[1] << " of database " << row[0];
					mysql_free_result(rs);
					return -1;
				}
			}
			else
			{
				charset = collection->getDefaultCharset();
			}
			collection->put(row[0], charset, 0);
		}
		mysql_free_result(rs);
		return 0;
	}
	int initMetaData::loadAllTables(META::metaDataCollection* collection, const std::vector<std::string>& databases)
	{
		MYSQL_RES  *rs = doQuery(SELECT_ALL_TABLE, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "load table failed";
			return -1;
		}
		MYSQL_ROW row;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			META::tableMeta* meta = new META::tableMeta(true);//todo
			meta->m_dbName = row[0];
			meta->m_tableName = row[1];
			if (row[4] != nullptr && strlen(row[4]) > 0)
			{
				if (nullptr == (meta->m_charset = getCharset(getCharsetFromCollation(row[4]).c_str())))
				{
					LOG(ERROR) << "unkonw charset:" << row[1] << " of table " << row[0]<<"."<<row[1];
					mysql_free_result(rs);
					return -1;
				}
			}
			else
			{
				meta->m_charset = collection->getDataBaseCharset(row[0], 0);
			}
			collection->put(row[0], row[1], meta, 0);
		}
		mysql_free_result(rs);
		return 0;
	}
	int initMetaData::getAllUserDatabases(std::vector<std::string>& databases)
	{
		databases.clear();
		MYSQL_RES* rs = doQuery(SELECT_ALL_USER_SCHEMA, databases);
		if (rs == nullptr)
		{
			LOG(ERROR) << "get database list failed";
			return -1;
		}
		MYSQL_ROW row;
		while (nullptr!=(row = mysql_fetch_row(rs)))
		{
			databases.push_back(row[0]);
		}
		mysql_free_result(rs);
		return 0;
	}
	int initMetaData::loadMeta(META::metaDataCollection* collection,const std::vector<std::string>& databases)
	{
		std::string charset;
		if (!mysqlConnector::getVariables(m_conn, "character_set_server", charset))
		{
			LOG(ERROR) << "load meta failed for can not get variable :'character_set_server' from database";
			return -1;
		}
		const charsetInfo* defaultCharset = getCharset(charset.c_str());
		if (defaultCharset == nullptr)
		{
			LOG(ERROR) << "load meta failed for unspport charset:"<< charset;
			return -1;
		}
		collection->setDefaultCharset(defaultCharset);
		if (0 != loadAllDataBases(collection, databases))
		{
			LOG(ERROR) << "load meta failed for load database list failed:";
			return -1;
		}
		if (0 != loadAllTables(collection, databases))
		{
			LOG(ERROR) << "load meta failed for load table list failed:";
			return -1;
		}
		if (0 != loadAllColumns(collection, databases))
		{
			LOG(ERROR) << "load meta failed for load column failed:";
			return -1;
		}
		if (0 != loadAllConstraint(collection, databases))
		{
			LOG(ERROR) << "load meta failed for load constraint list failed:";
			return -1;
		}
		return 0;
	}
}
