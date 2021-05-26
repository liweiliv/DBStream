#pragma once
#include <list>
#include <map>
#include <string>
#include <iostream>
#include "occiConnect.h"
#include "util/String.h"
#include "meta/charset.h"
#include "meta/metaData.h"
#include "meta/metaDataCollection.h"
#include "dataSource/tableList.h"
#include "oracleTypeDef.h"

namespace DATA_SOURCE
{
	struct indexInfo;
	typedef std::map<std::string, META::columnMeta*> tableColumnsMap;
	typedef std::map<uint64_t, tableColumnsMap*> userColumnsList;
	typedef std::map<uint64_t, userColumnsList*> allUserColumnsList;
	class oracleMetaDataCollection :public META::metaDataCollection
	{
	private:
		occiConnect* m_connector;
		oracle::occi::Connection* m_conn;
		const tableList* m_whiteList;
		const tableList* m_blackList;
		CHARSET m_ncharCharsetId;
		std::string m_ncharCharset;
		CHARSET m_charsetId;
		std::string m_charset;
		spp::sparse_hash_map<uint64_t, META::MetaTimeline<META::tableMeta>*> m_tables;
		DS createConnect();
	private:
		static DS connect(occiConnect* connector, oracle::occi::Connection*& conn);
		DS getUserList(std::list<std::pair<std::string, int32_t>>& userList);
		DS getUserObjectList(int ownerId, std::string& owner, std::map<uint64_t, std::string>& tableList,
			std::map<uint64_t, std::string>& indexList, std::map<std::string, std::list<uint64_t>*>& partitionsList);
		DS getSysTableInfo(int ownerId, std::map<uint64_t, std::string>& tableList);


		DS translateCharset(const std::string& src, CHARSET& dest);

		void setCharset(META::columnMeta* meta, int charsetForm);

		DS getDbCharset();

		META::columnMeta* readColumnInfo(oracle::occi::ResultSet* rs);

		DS getColumns(std::map<uint64_t, std::string>& tables, userColumnsList* userColumns);

		DS initUserTableMeta(std::string& ownerName, int32_t ownerId, std::map<uint64_t, std::string>& tables);

		DS initPartitons(std::string& owner, std::map<std::string, std::list<uint64_t>*>& partitionList);

		DS getAllNeedIndexInfo(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList,
			std::map<uint64_t, indexInfo*>& neededIndexs, std::map<uint64_t, std::list<indexInfo*>>& tableIndexInfo);

		DS getColumnInfoOfIndexs(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList,
			std::map<uint64_t, indexInfo*>& neededIndexs);

		DS initIndexs(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList);

	public:
		DLL_EXPORT oracleMetaDataCollection(occiConnect* connector, const tableList* whiteList, const tableList* blackList);
		DLL_EXPORT ~oracleMetaDataCollection();
		DLL_EXPORT inline META::tableMeta* getMetaByObjectId(uint64_t objectId, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		DLL_EXPORT DS init(META::metaDataCollection* collection);
	};
}