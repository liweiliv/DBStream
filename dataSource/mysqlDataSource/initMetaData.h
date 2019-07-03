#pragma once
#include "mysqlConnector.h"
#include "../../meta/metaDataCollection.h"
#include <vector>
namespace DATA_SOURCE {
	struct keyInfo;
	typedef std::unordered_map<const char*, enum_field_types, StrHash, StrCompare> MYSQL_TYPE_TREE;
	class initMetaData {
	private:
		MYSQL_TYPE_TREE m_mysqlTyps;
		mysqlConnector* m_connector;
		MYSQL* m_conn;
		MYSQL_RES* doQuery(const char* sql, const std::vector<std::string>& databases);
		int getColumnInfo(META::columnMeta* column, MYSQL_ROW row);
		int loadAllKeyColumns(META::metaDataCollection* collection,const std::vector<std::string>& databases, std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >& constraints);
		int loadAllDataBases(META::metaDataCollection* collection, const std::vector<std::string>& databases);
		int loadAllTables(META::metaDataCollection* collection, const std::vector<std::string>& databases);
		int loadAllColumns(META::metaDataCollection* collection,const std::vector<std::string>& databases);
		int loadAllConstraint(META::metaDataCollection* collection, const std::vector<std::string>& databases);
	public:
		initMetaData(mysqlConnector* connector);
		~initMetaData()
		{
			if (m_conn!=nullptr)
				mysql_close(m_conn);
		}
		int getAllUserDatabases(std::vector<std::string>& databases);
		int loadMeta(META::metaDataCollection* collection, const std::vector<std::string>& databases);
	};
}
