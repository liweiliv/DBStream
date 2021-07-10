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
	static constexpr auto SELECT_ALL_COLUMNS = "select COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,TABLE_SCHEMA,TABLE_NAME,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME, COLUMN_TYPE,GENERATION_EXPRESSION,CHARACTER_OCTET_LENGTH FROM information_schema.columns where TABLE_SCHEMA in ";
	static constexpr auto SELECT_ALL_CONSTRAINT = "select CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_TYPE from information_schema.TABLE_CONSTRAINTS where TABLE_SCHEMA in ";
	static constexpr auto SELECT_ALL_KEY_COLUMN = "select CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA in ";
	static constexpr auto SELECT_ALL_TABLE = "select TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,ENGINE,TABLE_COLLATION from information_schema.TABLES where TABLE_SCHEMA in ";
	static constexpr auto SELECT_ALL_SCHEMA = "select SCHEMA_NAME ,DEFAULT_CHARACTER_SET_NAME from information_schema.SCHEMATA where SCHEMA_NAME in ";
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
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("double", MYSQL_TYPE_DOUBLE));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("bit", MYSQL_TYPE_BIT));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("datetime", MYSQL_TYPE_DATETIME2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("timestamp", MYSQL_TYPE_TIMESTAMP2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("time", MYSQL_TYPE_TIME2));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("date", MYSQL_TYPE_NEWDATE));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("year", MYSQL_TYPE_YEAR));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("char", MYSQL_TYPE_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("varchar", MYSQL_TYPE_VARCHAR));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("binary", MYSQL_TYPE_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("varbinary", MYSQL_TYPE_VAR_STRING));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("tinytext", MYSQL_TYPE_TINY_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("mediumtext", MYSQL_TYPE_MEDIUM_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("text", MYSQL_TYPE_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("longtext", MYSQL_TYPE_LONG_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("tinyblob", MYSQL_TYPE_TINY_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("mediumblob", MYSQL_TYPE_MEDIUM_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("blob", MYSQL_TYPE_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("longblob", MYSQL_TYPE_LONG_BLOB));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("json", MYSQL_TYPE_JSON));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("enum", MYSQL_TYPE_ENUM));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("set", MYSQL_TYPE_SET));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("geometry", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("point", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("linestring", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("polygon", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multipoint", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multilinestring", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("multipolygon", MYSQL_TYPE_GEOMETRY));
		m_mysqlTyps.insert(std::pair<const char*, enum_field_types>("geomcollection", MYSQL_TYPE_GEOMETRY));
	}
	static int parseEnumOrSetValueList(META::ColumnMeta* column, const char* values)
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
		column->m_setAndEnumValueList.m_array = (char**)malloc(sizeof(char*) * valueList.size());
		for (std::vector<std::string>::const_iterator iter = valueList.begin(); iter != valueList.end(); iter++)
		{
			column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count] = (char*)malloc((*iter).size() + 1);
			memcpy(column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count], (*iter).c_str(), (*iter).size());
			column->m_setAndEnumValueList.m_array[column->m_setAndEnumValueList.m_count][(*iter).size()] = '\0';
			column->m_setAndEnumValueList.m_count++;
		}
		return 0;
	}
	int initMetaData::getColumnInfo(META::ColumnMeta* column, MYSQL_ROW row)
	{
		column->m_columnName = row[0];
		column->m_columnIndex = atoi(row[1]) - 1;
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
			column->m_precision = atoi(row[7]);
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
				column->unsetFlag(COL_FLAG_SIGNED);
			else
				column->setFlag(COL_FLAG_SIGNED);
		}
		if (row[10] != nullptr && strlen(row[10]) > 0)
			column->setFlag(COL_FLAG_GENERATED);
		if (row[11] != nullptr && !META::columnInfos[TID(column->m_columnType)].fixed)
			column->m_size = (uint64_t)atol(row[11]);
		return 0;
	}

	static void clearAllColumns(std::map < std::string, std::map<std::string, std::map<int, META::ColumnMeta*>* >* >& allColumns)
	{
		for (auto& iter : allColumns)
		{
			for (auto tableIter : *iter.second)
			{
				for (auto& columnIter : *tableIter.second)
					delete columnIter.second;
				delete tableIter.second;
			}
			delete iter.second;
		}
	}

	DS initMetaData::loadAllColumns(META::MetaDataCollection* collection, const std::vector<std::string>& databases)
	{
		mysqlResWrap rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_COLUMNS, databases, rs));
		std::map < std::string, std::map<std::string, std::map<int, META::ColumnMeta*>* >* > allColumns;
		MYSQL_ROW row;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
		{
			META::TableMeta* meta = collection->get(row[3], row[4], 1);
			if (meta == nullptr)
				continue;
			META::ColumnMeta* column = new META::ColumnMeta();
			dsReturnIfFailedWithOp(getColumnInfo(column, row), do { delete column; clearAllColumns(allColumns); } while (0));

			std::map < std::string, std::map<std::string, std::map<int, META::ColumnMeta*>* >* >::iterator databaseIter = allColumns.find(row[3]);
			std::map < std::string, std::map<int, META::ColumnMeta*>*>* database;
			if (databaseIter == allColumns.end())
			{
				database = new std::map < std::string, std::map<int, META::ColumnMeta*>*>();
				allColumns.insert(std::pair<std::string, std::map<std::string, std::map<int, META::ColumnMeta*>* >*>(row[3], database));
			}
			else
				database = databaseIter->second;
			std::map< std::string, std::map<int, META::ColumnMeta*>*>::iterator tableIter = database->find(row[4]);
			std::map<int, META::ColumnMeta*>* table;
			if (tableIter == database->end())
			{
				table = new std::map<int, META::ColumnMeta*>();
				database->insert(std::pair<std::string, std::map<int, META::ColumnMeta*>*>(row[4], table));
			}
			else
				table = tableIter->second;
			if (!table->insert(std::pair<int, META::ColumnMeta*>(column->m_columnIndex, column)).second)
			{
				String errInfo;
				errInfo = errInfo << "column:" << row[3] << "." << row[4] << "." << row[0] << " has the same column id :" << column->m_columnIndex <<
					" with column:" << table->find(column->m_columnIndex)->second->m_columnName;
				delete column;
				clearAllColumns(allColumns);
				dsFailedAndLogIt(1, errInfo, ERROR);
			}
		}
		for (auto& iter : allColumns)
		{
			for (auto tableIter : *iter.second)
			{
				META::TableMeta* meta = collection->get(iter.first.c_str(), tableIter.first.c_str(), 1);
				meta->m_columns = new META::ColumnMeta[tableIter.second->size()];
				for (auto& columnIter : *tableIter.second)
					meta->m_columns[meta->m_columnsCount++] = *columnIter.second;
				meta->buildColumnOffsetList();
			}
		}
		clearAllColumns(allColumns);
		dsOk();
	}

	struct keyInfo {
		std::map<int, uint16_t> columns;
		META::KEY_TYPE type;
	};

	int initMetaData::loadAllKeyColumns(META::MetaDataCollection* collection, const std::vector<std::string>& databases, std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >& constraints)
	{
		mysqlResWrap  rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_KEY_COLUMN, databases, rs));
		MYSQL_ROW row;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
		{
			META::TableMeta* meta = collection->get(row[2], row[3], 1);
			META::ColumnMeta* column;
			if (meta == nullptr || (column = (META::ColumnMeta*)meta->getColumn(row[4])) == nullptr)
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
		dsOk();
	}

	DS initMetaData::doQuery(const char* _sql, const std::vector<std::string>& databases, mysqlResWrap& result)
	{
		if (m_conn == nullptr)
			dsReturnIfFailed(m_connector->getConnect(m_conn));
		std::string sql(_sql);
		sql.append("(");
		char escapeBuffer[512] = { 0 };
		for (std::vector<std::string>::const_iterator iter = databases.begin(); iter != databases.end(); iter++)
		{
			if (iter != databases.begin())
				sql.append(",");
			sql.append("'");
			uint32_t size = mysql_real_escape_string_quote(m_conn, escapeBuffer, (*iter).c_str(), (*iter).size(), '\'');
			sql.append(escapeBuffer, size);
			sql.append("'");
		}
		sql.append(")");
		MYSQL_RES* rs = nullptr;
		String errInfo;
		for (int retry = 0; retry < 10; retry++)
		{
			if (0 != mysql_query(m_conn, sql.c_str()))
			{
				errInfo.clear();
				errInfo = errInfo << "sql:" << sql << " query failed for:" << mysql_errno(m_conn) << "," << mysql_error(m_conn);
				LOG(ERROR) << errInfo;
				goto CHECK_ERROR;
			}
			rs = mysql_store_result(m_conn);
			if (rs == nullptr)
			{
				errInfo = "can not get any result from database";
				LOG(ERROR) << errInfo;
				goto CHECK_ERROR;
			}
			result.rs = rs;
			dsOk();
		CHECK_ERROR:
			if (!(mysql_errno(m_conn) == 2003 || mysql_errno(m_conn) == 2006 || mysql_errno(m_conn) == 2013))
				break;
			std::this_thread::sleep_for(std::chrono::seconds(1));
			mysql_close(m_conn);
			dsReturnIfFailed(m_connector->getConnect(m_conn));
		}
		dsFailedAndLogIt(1, errInfo, ERROR);
	}

	static void clearConstrains(std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >& constraints)
	{
		for (auto& databaseIter : constraints)
		{
			for (auto& tableIter : *databaseIter.second)
			{
				for (auto& keyIter : *tableIter.second)
					delete keyIter.second;
				delete tableIter.second;
			}
			delete databaseIter.second;
		}
	}

	int initMetaData::loadAllConstraint(META::MetaDataCollection* collection, const std::vector<std::string>& databases)
	{
		std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* > constraints;
		mysqlResWrap  rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_CONSTRAINT, databases, rs));
		MYSQL_ROW row;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
		{
			META::TableMeta* meta = collection->get(row[2], row[3], 1);
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
				table = new  std::map<std::string, keyInfo* >();
				database->insert(std::pair<std::string, std::map<std::string, keyInfo* >*  >(row[3], table));
			}
			else
				table = tableIter->second;
			keyInfo* key = new keyInfo();
			key->type = type;
			table->insert(std::pair<std::string, keyInfo*>(row[1], key));
		}
		dsReturnIfFailedWithOp(loadAllKeyColumns(collection, databases, constraints), clearConstrains(constraints));
		uint16_t keyIdxs[256];
		for (auto& databaseIter : constraints)
		{
			for (auto& tableIter : *databaseIter.second)
			{
				META::TableMeta* table = collection->get(databaseIter.first.c_str(), tableIter.first.c_str(), 1);
				if (table != nullptr)
				{
					if (table->m_uniqueKeysCount > 0)
						table->m_uniqueKeys = (META::UnionKeyMeta**)malloc(sizeof(META::UnionKeyMeta*) * table->m_uniqueKeysCount);
					table->m_uniqueKeyNames = new std::string[table->m_uniqueKeysCount];
					if (table->m_indexCount > 0)
						table->m_indexs = (META::UnionKeyMeta**)malloc(sizeof(META::UnionKeyMeta*) * table->m_indexCount);
					table->m_indexNames = new std::string[table->m_indexCount];
					uint16_t ukCount = 0, indexCount = 0;
					for (auto& keyIter: *tableIter.second)
					{
						int keyCount = 0;
						for (std::map<int, uint16_t>::const_iterator columnIter = keyIter.second->columns.begin(); columnIter != keyIter.second->columns.end(); columnIter++)
							keyIdxs[keyCount++] = columnIter->second;

						if (keyIter.second->type == META::KEY_TYPE::PRIMARY_KEY)
						{
							table->m_primaryKey = table->createUnionKey(0, META::KEY_TYPE::PRIMARY_KEY, keyIdxs, keyCount);
						}
						else if (keyIter.second->type == META::KEY_TYPE::UNIQUE_KEY)
						{
							table->m_uniqueKeys[ukCount] = table->createUnionKey(ukCount, META::KEY_TYPE::UNIQUE_KEY, keyIdxs, keyCount);
							table->m_uniqueKeyNames[ukCount++] = keyIter.first;
						}
						else
						{
							table->m_indexs[indexCount] = table->createUnionKey(ukCount, META::KEY_TYPE::INDEX, keyIdxs, keyCount);
							table->m_indexNames[indexCount++] = keyIter.first;
						}
					}
				}
			}
		}
		dsOk();
	}
	static std::string getCharsetFromCollation(const char* collation)
	{
		const char* ptr = strchr(collation, '_');
		if (ptr != nullptr)
			return std::string(collation, ptr - collation);
		else
			return collation;
	}

	DS initMetaData::loadAllDataBases(META::MetaDataCollection* collection, const std::vector<std::string>& databases)
	{
		mysqlResWrap rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_SCHEMA, databases, rs));
		MYSQL_ROW row;
		const charsetInfo* charset;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
		{
			if (row[1] != nullptr && strlen(row[1]) > 0)
			{
				if (nullptr == (charset = getCharset(row[1])))
					dsFailedAndLogIt(1, "unkonw charset:" << row[1] << " of database " << row[0], ERROR);
			}
			else
			{
				charset = collection->getDefaultCharset();
			}
			collection->put(row[0], charset, 0);
		}
		dsOk();
	}

	DS initMetaData::loadAllTables(META::MetaDataCollection* collection, const std::vector<std::string>& databases)
	{
		mysqlResWrap rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_TABLE, databases, rs));
		MYSQL_ROW row;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
		{
			META::TableMeta* meta = new META::TableMeta(true);//todo
			meta->m_dbName = row[0];
			meta->m_tableName = row[1];
			if (row[4] != nullptr && strlen(row[4]) > 0)
			{
				if (nullptr == (meta->m_charset = getCharset(getCharsetFromCollation(row[4]).c_str())))
					dsFailedAndLogIt(1, "unkonw charset:" << row[1] << " of table " << row[0] << "." << row[1], ERROR);
			}
			else
			{
				meta->m_charset = collection->getDataBaseCharset(row[0], 0);
			}
			collection->put(row[0], row[1], meta, 0);
		}
		dsOk();
	}

	DS initMetaData::getAllUserDatabases(std::vector<std::string>& databases)
	{
		databases.clear();
		mysqlResWrap rs;
		dsReturnIfFailed(doQuery(SELECT_ALL_USER_SCHEMA, databases, rs));
		MYSQL_ROW row;
		while (nullptr != (row = mysql_fetch_row(rs.rs)))
			databases.push_back(row[0]);
		dsOk();
	}

	DS initMetaData::loadMeta(META::MetaDataCollection* collection, const std::vector<std::string>& databases)
	{
		std::string charset;
		if (!mysqlConnector::getVariables(m_conn, "character_set_server", charset))
			dsFailedAndLogIt(1, "load meta failed for can not get variable :'character_set_server' from database", ERROR);

		const charsetInfo* defaultCharset = getCharset(charset.c_str());
		if (defaultCharset == nullptr)
			dsFailedAndLogIt(1, "load meta failed for unspport charset:" << charset, ERROR);

		collection->setDefaultCharset(defaultCharset);
		dsReturnIfFailed(loadAllDataBases(collection, databases));
		dsReturnIfFailed(loadAllTables(collection, databases));
		dsReturnIfFailed(loadAllColumns(collection, databases));
		dsReturnIfFailed(loadAllConstraint(collection, databases));
		dsOk();
	}
}
