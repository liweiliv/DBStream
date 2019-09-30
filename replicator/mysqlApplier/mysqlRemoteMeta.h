#pragma once
#include "mysqlConnector.h"
#include "meta/metaDataCollection.h"
#include <vector>
#include "replicator/remoteMeta.h"
namespace REPLICATOR {
	struct keyInfo;
	typedef std::unordered_map<const char*, enum_field_types, StrHash, StrCompare> MYSQL_TYPE_TREE;
	class mysqlRemoteMeta:public remoteMeta {
	private:
		MYSQL_TYPE_TREE m_mysqlTyps;
		mysqlConnector* m_connector;
		MYSQL* m_conn;
		MYSQL_RES* doQueryByDataBases(const char* sql, const std::vector<std::string>& databases);
		MYSQL_RES* doQueryByString(const char* _sql, ...);
		int realQuery(const char* sql, MYSQL_RES*& rs);
		int getColumnInfo(META::columnMeta* column, MYSQL_ROW row);
		int loadAllKeyColumns(const std::vector<std::string>& databases, std::map < std::string, std::map<std::string, std::map<std::string, keyInfo*>* >* >& constraints);
		int loadAllDataBases(const std::vector<std::string>& databases);
		int loadAllTables(const std::vector<std::string>& databases);
		int loadAllColumns(const std::vector<std::string>& databases);
		int loadAllConstraint(const std::vector<std::string>& databases);
	public:
		mysqlRemoteMeta(mysqlConnector* connector);
		~mysqlRemoteMeta()
		{
			if (m_conn!=nullptr)
				mysql_close(m_conn);
		}
		int getAllUserDatabases(std::vector<std::string>& databases);
		int loadMeta( const std::vector<std::string>& databases);
		virtual const META::tableMeta* update(const char* database, const char* table);
		virtual int escapeTableName(const META::tableMeta* table, std::string& escaped);
		virtual int escapeColumnName(const META::columnMeta* column, std::string& escaped);
	};
}
