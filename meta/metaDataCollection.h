/*
 * metaDataCollection.h
 *
 *  Created on: 2018年12月3日
 *      Author: liwei
 */

#ifndef METADATACOLLECTION_H_
#define METADATACOLLECTION_H_

#include <string>
#include "util/trieTree.h"
#include "util/unorderMapUtil.h"
#include "util/sparsepp/spp.h"
#include "tableIdTree.h"
#include "util/winDll.h"
#include "metaTimeline.h"
#include "metaDataBaseCollection.h"
namespace CLIENT {
	class client;
}
namespace SQL_PARSER
{
	class sqlParser;
	struct handle;
};
namespace DATABASE {
	class blockManager;
}
class bufferPool;
class bufferBaseAllocer;
class Config;
typedef std::unordered_map<const char*, const  charsetInfo*, StrHash, StrCompare> CharsetTree;
namespace META {
	struct TableMeta;
	struct ColumnMeta;
	struct DbInfo;
	typedef spp::sparse_hash_map<const char*, MetaTimeline<DbInfo>*, UTIL::nameCompare, UTIL::nameCompare> dbTree;
	class newColumnInfo;
	class newTableInfo;
	struct Table;
	struct ddl;
	struct databaseInfo;
	class  MetaDataCollection :public MetaDataBaseCollection
	{
	private:
		dbTree m_dbs;
		CharsetTree m_charsetSizeList;
		TableIdTree m_allTables;
		SQL_PARSER::sqlParser* m_sqlParser;
		CLIENT::client* m_client;
		uint64_t m_maxTableId;
		uint64_t m_maxDatabaseId;

		DATABASE::blockManager* m_metaFile;
		bufferPool* m_bufferPool;
		bufferBaseAllocer *m_allocer;
		Config* m_virtualConf;
	public:
		DLL_EXPORT MetaDataCollection(const char* defaultCharset, bool caseSensitive = true, CLIENT::client* client = nullptr, const char* savePath = nullptr);
		DLL_EXPORT ~MetaDataCollection();
		DLL_EXPORT int initSqlParser(const char* sqlParserTreeFile, const char* sqlParserFunclibFile);
		DLL_EXPORT TableMeta* get(uint64_t tableID);
		DLL_EXPORT TableMeta* getPrevVersion(uint64_t tableID);
		DLL_EXPORT TableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		DLL_EXPORT bool isDataBaseExist(const char* database, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		DLL_EXPORT TableMeta* getTableMetaFromRemote(uint64_t tableID);
		DLL_EXPORT TableMeta* getTableMetaFromRemote(const char* database, const char* table, uint64_t originCheckPoint);
		DLL_EXPORT const charsetInfo* getDataBaseCharset(const char* database, uint64_t originCheckPoint);
		DLL_EXPORT int put(const char* database, const char* table, TableMeta* meta, uint64_t originCheckPoint);
		DLL_EXPORT int put(const char* database, const charsetInfo* charset, uint64_t originCheckPoint);
		DLL_EXPORT int purge(uint64_t originCheckPoint);
		DLL_EXPORT int processDDL(const char* ddl, const char* database, uint64_t originCheckPoint);
		DLL_EXPORT int setDefaultCharset(const charsetInfo* defaultCharset);
		DLL_EXPORT const charsetInfo* getDefaultCharset();
	protected:
		DLL_EXPORT MetaTimeline<TableMeta>* _getTableInfo(const char* database, const char* table);
	private:
		int put(const char* database, uint64_t offset, DbInfo* db);
		DbInfo* getDatabaseMetaFromRemote(uint64_t databaseID);
		DbInfo* getDatabaseMetaFromRemote(const char* databaseName, uint64_t offset);
		DbInfo* getDatabase(const char* database, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		int processDDL(const struct ddl* ddl, uint64_t originCheckPoint);

		int createDatabase(const ddl* database, uint64_t originCheckPoint);
		int alterDatabase(const ddl* database, uint64_t originCheckPoint);
		int dropDatabase(const ddl* database, uint64_t originCheckPoint);
		int createTable(const ddl* tableDDL, uint64_t originCheckPoint);
		int createTableLike(const ddl* tableDDL, uint64_t originCheckPoint);
		int createIndex(const ddl* tableDDL, uint64_t originCheckPoint);
		int dropIndex(const ddl* tableDDL, uint64_t originCheckPoint);

		int dropTable(const ddl* tableDDL, uint64_t originCheckPoint);

		int dropTable(const char* database, const char* table, uint64_t originCheckPoint);
		int renameTable(const ddl* tableDDL, uint64_t originCheckPoint);
		int renameTable(const char* srcDatabase, const char* srcTable, const char* destDatabase, const char* destTable, uint64_t originCheckPoint);
		int alterTable(const ddl* tableDDL, uint64_t originCheckPoint);
	public:
		DLL_EXPORT void print();
	};
}
#endif /* METADATACOLLECTION_H_ */

