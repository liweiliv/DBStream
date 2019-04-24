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
#include "../util/skiplist.h"
struct charsetInfo;
namespace STORE{
class client;
}
namespace SQL_PARSER
{
class sqlParser;
struct handle;
};
namespace META {
	struct tableMeta;
	struct columnMeta;

	struct dbInfo;
	class newColumnInfo;
	class newTableInfo;
	struct Table;
	struct databaseInfo;
	struct tableMetaWrap
	{
		uint64_t tableID;
		tableMeta * meta;
	};
	struct tableIDComparator
	{
		int operator()( META::tableMetaWrap* const&a, META::tableMetaWrap* const& b) const
		{
			if (a->tableID < b->tableID)
				return -1;
			else if (a->tableID > b->tableID)
				return +1;
			else
				return 0;
		}
	};
	class metaDataCollection
	{
	private:
		trieTree m_dbs;
		const charsetInfo * m_defaultCharset;
		leveldb::Arena m_arena;
		tableIDComparator m_cmp;
		leveldb::SkipList<tableMetaWrap*, tableIDComparator> m_allTables;
		SQL_PARSER::sqlParser * m_SqlParser;
		STORE::client * m_client;
		uint64_t m_maxTableId;

	public:
		metaDataCollection(const char * defaultCharset,STORE::client *client = nullptr);
		~metaDataCollection();
		tableMeta * get(uint64_t tableID);
		tableMeta * get(const char * database, const char * table, uint64_t originCheckPoint);

		tableMeta * getTableMetaFromRemote(uint64_t tableID);
		tableMeta * getTableMetaFromRemote(const char * databaseName,const char * tableName, uint64_t offset);

		int put(const char * database, uint64_t offset, dbInfo *db);
		int put(const char * database, const char * table, tableMeta * meta, uint64_t originCheckPoint);
		int purge(uint64_t originCheckPoint);
		int processDDL(const char * ddl, uint64_t originCheckPoint);
	private:
		dbInfo * getDatabaseMetaFromRemote(uint64_t databaseID);
		dbInfo * getDatabaseMetaFromRemote(const char * databaseName, uint64_t offset);
		int createTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int createTableLike(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int alterTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int processNewTable(SQL_PARSER::handle * h, const newTableInfo *t, uint64_t originCheckPoint);
		int processOldTable(SQL_PARSER::handle * h, const Table *table, uint64_t originCheckPoint);
		int processDatabase(const databaseInfo * database, uint64_t originCheckPoint);
	public:
	};
}
#endif /* METADATACOLLECTION_H_ */
