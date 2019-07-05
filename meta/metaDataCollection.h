/*
 * metaDataCollection.h
 *
 *  Created on: 2018年12月3日
 *      Author: liwei
 */

#ifndef METADATACOLLECTION_H_
#define METADATACOLLECTION_H_

#include <string>
#include "../util/trieTree.h"
#include "../util/unorderMapUtil.h"
#include "tableIdTree.h"
#include "../util/winDll.h"
struct charsetInfo;
namespace STORE{
class client;
}
namespace SQL_PARSER
{
class sqlParser;
struct handle;
};
typedef std::unordered_map<const char *,const  charsetInfo*,StrHash,StrCompare> CharsetTree ;
namespace META {
	struct tableMeta;
	struct columnMeta;

	struct dbInfo;
	class newColumnInfo;
	class newTableInfo;
	struct Table;
	struct databaseInfo;

	class DLL_EXPORT metaDataCollection
	{
	private:
		trieTree m_dbs;
		CharsetTree m_charsetSizeList;
		const charsetInfo * m_defaultCharset;
		tableIdTree m_allTables;
		SQL_PARSER::sqlParser * m_sqlParser;
		STORE::client * m_client;
		uint64_t m_maxTableId;
		uint64_t m_maxDatabaseId;
	public:
		metaDataCollection(const char * defaultCharset,STORE::client *client = nullptr);
		~metaDataCollection();
		int initSqlParser(const char * sqlParserTreeFile,const char * sqlParserFunclibFile);
		tableMeta * get(uint64_t tableID);
		tableMeta * get(const char * database, const char * table, uint64_t originCheckPoint);

		tableMeta * getTableMetaFromRemote(uint64_t tableID);
		tableMeta * getTableMetaFromRemote(const char * database, const char * table, uint64_t originCheckPoint);
		const charsetInfo* getDataBaseCharset(const char* database, uint64_t originCheckPoint);
		int put(const char * database, const char * table, tableMeta * meta, uint64_t originCheckPoint);
		int put(const char* database, const charsetInfo* charset, uint64_t originCheckPoint);
		int purge(uint64_t originCheckPoint);
		int processDDL(const char * ddl, uint64_t originCheckPoint);
		int setDefaultCharset(const charsetInfo* defaultCharset);
		const charsetInfo* getDefaultCharset();
	private:
		int put(const char* database, uint64_t offset, dbInfo* db);
		dbInfo * getDatabaseMetaFromRemote(uint64_t databaseID);
		dbInfo * getDatabaseMetaFromRemote(const char * databaseName, uint64_t offset);
		int createTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int createTableLike(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int alterTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int processNewTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int processOldTable(SQL_PARSER::handle * h, const Table *table, uint64_t originCheckPoint);
		int processDatabase(const databaseInfo * database, uint64_t originCheckPoint);
	public:
		void print();
	};
}
#endif /* METADATACOLLECTION_H_ */
