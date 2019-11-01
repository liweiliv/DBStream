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
#include "metadataBaseCollection.h"
namespace CLIENT{
class client;
}
namespace SQL_PARSER
{
class sqlParser;
struct handle;
};
namespace DATABASE{
	class blockManager;
}
class bufferPool;
class config;
typedef std::unordered_map<const char *,const  charsetInfo*,StrHash,StrCompare> CharsetTree ;
namespace META {
	struct tableMeta;
	struct columnMeta;
	struct dbInfo;
	typedef spp::sparse_hash_map<const char*,MetaTimeline<dbInfo>*, nameCompare, nameCompare> dbTree;
	class newColumnInfo;
	class newTableInfo;
	struct Table;
	struct ddl;
	struct databaseInfo;
	class DLL_EXPORT metaDataCollection :public metaDataBaseCollection
	{
	private:
		dbTree m_dbs;
		CharsetTree m_charsetSizeList;
		tableIdTree m_allTables;
		SQL_PARSER::sqlParser * m_sqlParser;
		CLIENT::client * m_client;
		uint64_t m_maxTableId;
		uint64_t m_maxDatabaseId;

		DATABASE::blockManager* m_metaFile;
		bufferPool* m_bufferPool;
		config* m_virtualConf;
	public:
		metaDataCollection(const char * defaultCharset,bool caseSensitive = true,CLIENT::client *client = nullptr, const char* savePath=nullptr);
		~metaDataCollection();
		int initSqlParser(const char * sqlParserTreeFile,const char * sqlParserFunclibFile);
		tableMeta * get(uint64_t tableID);
		tableMeta * getPrevVersion(uint64_t tableID);
		tableMeta * get(const char * database, const char * table, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		bool isDataBaseExist(const char * database, uint64_t originCheckPoint = 0xffffffffffffffffULL);
		tableMeta * getTableMetaFromRemote(uint64_t tableID);
		tableMeta * getTableMetaFromRemote(const char * database, const char * table, uint64_t originCheckPoint);
		const charsetInfo* getDataBaseCharset(const char* database, uint64_t originCheckPoint);
		int put(const char * database, const char * table, tableMeta * meta, uint64_t originCheckPoint);
		int put(const char* database, const charsetInfo* charset, uint64_t originCheckPoint);
		int purge(uint64_t originCheckPoint);
		int processDDL(const char * ddl, const char * database,uint64_t originCheckPoint);
		int processDDL(const struct ddl* ddl, uint64_t originCheckPoint);
		int setDefaultCharset(const charsetInfo* defaultCharset);
		const charsetInfo* getDefaultCharset();
	private:
		int put(const char* database, uint64_t offset, dbInfo* db);
		dbInfo * getDatabaseMetaFromRemote(uint64_t databaseID);
		dbInfo * getDatabaseMetaFromRemote(const char * databaseName, uint64_t offset);
		dbInfo* getDatabase(const char* database, uint64_t originCheckPoint = 0xffffffffffffffffULL);

		int createDatabase(const ddl* database, uint64_t originCheckPoint);
		int alterDatabase(const ddl* database, uint64_t originCheckPoint);
		int dropDatabase(const ddl* database,uint64_t originCheckPoint);
		int createTable(const ddl* tableDDL, uint64_t originCheckPoint);
		int createTableLike(const ddl* tableDDL, uint64_t originCheckPoint);
		int createIndex(const ddl* tableDDL, uint64_t originCheckPoint);
		int dropIndex(const ddl* tableDDL, uint64_t originCheckPoint);

		int dropTable(const ddl* tableDDL, uint64_t originCheckPoint);

		int dropTable(const char * database,const char * table, uint64_t originCheckPoint);
		int renameTable(const ddl* tableDDL, uint64_t originCheckPoint);
		int renameTable(const char * srcDatabase,const char * srcTable,const char * destDatabase,const char * destTable, uint64_t originCheckPoint);
		int alterTable(const ddl* tableDDL, uint64_t originCheckPoint);
	public:
		void print();
	};
}
#endif /* METADATACOLLECTION_H_ */

