#pragma once
#include "mysqlConnector.h"
#include "meta/metaDataCollection.h"
#include <vector>
namespace DATA_SOURCE {
	struct keyInfo;
	typedef std::unordered_map<const char*, enum_field_types, StrHash, StrCompare> MYSQL_TYPE_TREE;
	class initMetaData {
	private:
		MYSQL_TYPE_TREE m_mysqlTyps;
		mysqlConnector* m_connector;
		MYSQL* m_conn;
		DS doQuery(const char* sql, const std::vector<std::string>& databases, mysqlResWrap& result);
		DS getColumnInfo(META::ColumnMeta* column, MYSQL_ROW row);
		DS loadAllKeyColumns(META::MetaDataCollection* collection,const std::vector<std::string>& databases, std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >& constraints);
		DS loadAllDataBases(META::MetaDataCollection* collection, const std::vector<std::string>& databases);
		DS loadAllTables(META::MetaDataCollection* collection, const std::vector<std::string>& databases);
		DS loadAllColumns(META::MetaDataCollection* collection,const std::vector<std::string>& databases);
		DS loadAllConstraint(META::MetaDataCollection* collection, const std::vector<std::string>& databases);
	public:
		initMetaData(mysqlConnector* connector);
		~initMetaData()
		{
			if (m_conn!=nullptr)
				mysql_close(m_conn);
		}
		DS getAllUserDatabases(std::vector<std::string>& databases);
		DS loadMeta(META::MetaDataCollection* collection, const std::vector<std::string>& databases);
	};
}
