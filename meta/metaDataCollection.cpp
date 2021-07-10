/*
 * metaDataCollection.cpp
 *
 *  Created on: 2018年11月5日
 *      Author: liwei
 */
#include <thread>
#include <string.h>
#include "metaDataCollection.h"
#include "metaData.h"
#include "glog/logging.h"
#include "sqlParser/sqlParser.h"
#include "client/client.h"
#include "charset.h"
#include "metaTimeline.h"
#include "message/record.h"
#include "thread/barrier.h"
#include "util/likely.h"
#include "util/config.h"
#include "memory/bufferPool.h"
#include "ddl.h"
#include "database/database.h"
using namespace SQL_PARSER;
using namespace RPC;
namespace META {
	typedef spp::sparse_hash_map<const char*, MetaTimeline<TableMeta>*, UTIL::nameCompare, UTIL::nameCompare> tbTree;
#define getDbInfo(database,di) do{		\
	dbTree::iterator DBIter = m_dbs.find(database);\
	if (unlikely(DBIter == m_dbs.end()))\
		(di) = nullptr;\
	else\
		(di) = DBIter->second; \
	}while(0);
#define getTableInfo(tableName,meta,db) do{		\
	tbTree::iterator TBIter = (db)->tables.find(tableName);\
	if (unlikely(TBIter == (db)->tables.end()))\
		(meta) = nullptr;\
	else\
		(meta) = TBIter->second; \
	}while(0);

	struct DbInfo
	{
		tbTree tables;
		uint64_t m_id;
		std::string name;
		const charsetInfo* charset;
		std::string collate;
		DbInfo(UTIL::nameCompare& comp) :tables(0, comp, comp), m_id(0), charset(nullptr) {}
		DbInfo& operator = (const DbInfo& db)
		{
			for (tbTree::iterator iter = db.tables.begin(); iter != db.tables.end(); iter++)
				tables.insert(std::pair<const char*, MetaTimeline<TableMeta>*>(iter->first, iter->second));
			m_id = db.m_id;
			name = db.name;
			charset = db.charset;
			collate = db.collate;
			return *this;
		}
		~DbInfo()
		{
			for (tbTree::iterator iter = tables.begin(); iter != tables.end(); iter++)
				delete iter->second;
			tables.clear();
		}
	};
	void initVirtualConf(Config* conf)
	{

	}
	DLL_EXPORT MetaDataCollection::MetaDataCollection(const char* defaultCharset, bool caseSensitive, CLIENT::client* client, const char* savePath) :MetaDataBaseCollection(caseSensitive, getCharset(defaultCharset)), m_dbs(0, m_nameCompare, m_nameCompare), m_sqlParser(nullptr), m_client(client),
		m_maxTableId(1), m_maxDatabaseId(0), m_metaFile(nullptr), m_bufferPool(nullptr)
	{
		if (savePath != nullptr)
		{
			m_virtualConf = new Config(nullptr);
			initVirtualConf(m_virtualConf);
			m_allocer = new defaultBufferBaseAllocer();
			m_bufferPool = new bufferPool(m_allocer);
		}
		for (uint16_t i = 0; i < MAX_CHARSET; i++)
			m_charsetSizeList.insert(std::pair<const char*, const charsetInfo*>(charsets[i].name, &charsets[i]));
	}
	DLL_EXPORT int MetaDataCollection::initSqlParser(const char* sqlParserTreeFile, const char* sqlParserFunclibFile)
	{
		m_sqlParser = new sqlParser();
		if (0 != m_sqlParser->LoadFuncs(sqlParserFunclibFile))
		{
			delete m_sqlParser;
			m_sqlParser = nullptr;
			return -1;
		}
		if (0 != m_sqlParser->LoadParseTreeFromFile(sqlParserTreeFile))
		{
			delete m_sqlParser;
			m_sqlParser = nullptr;
			return -1;
		}
		return 0;
	}
	DLL_EXPORT MetaDataCollection::~MetaDataCollection()
	{
		if (m_sqlParser != nullptr)
			delete m_sqlParser;
		for (dbTree::iterator iter = m_dbs.begin(); iter != m_dbs.end(); iter++)
			delete iter->second;
		m_dbs.clear();
	}
	DLL_EXPORT TableMeta* MetaDataCollection::get(const char* database, const char* table,
		uint64_t originCheckPoint)
	{
		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		if (db == nullptr)
			return nullptr;
		DbInfo* currentDB = db->get(originCheckPoint);
		if (currentDB == nullptr)
			return nullptr;
		MetaTimeline<TableMeta>* metas;
		getTableInfo(table, metas, currentDB);
		if (metas == nullptr)
			return nullptr;
		return metas->get(originCheckPoint);
	}
	DLL_EXPORT const charsetInfo* MetaDataCollection::getDataBaseCharset(const char* database, uint64_t originCheckPoint)
	{

		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		if (db == nullptr)
			return nullptr;
		DbInfo* currentDB = db->get(originCheckPoint);
		if (currentDB == nullptr)
			return nullptr;
		return currentDB->charset;
	}

	DLL_EXPORT TableMeta* MetaDataCollection::getTableMetaFromRemote(uint64_t tableID) {
		const char* metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askTableMeta(tableID)); i++)
		{
			if (m_client->getStatus() == CLIENT::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == CLIENT::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		RPC::TableMetaMessage msg(metaRecord);
		TableMeta* meta = new TableMeta(&msg);
		put(meta->m_dbName.c_str(), meta->m_tableName.c_str(), meta, msg.head->checkpoint.logOffset);
		return meta;
	}
	DLL_EXPORT TableMeta* MetaDataCollection::getTableMetaFromRemote(const char* database, const char* table, uint64_t originCheckPoint) {
		const char* metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askTableMeta(database, table, originCheckPoint)); i++)
		{
			if (m_client->getStatus() == CLIENT::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == CLIENT::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		TableMetaMessage msg(metaRecord);
		TableMeta* meta = new TableMeta(&msg);
		put(meta->m_dbName.c_str(), meta->m_tableName.c_str(), meta, msg.head->checkpoint.logOffset);
		return meta;
	}
	DbInfo* MetaDataCollection::getDatabaseMetaFromRemote(uint64_t databaseID) {
		const char* metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askDatabaseMeta(databaseID)); i++)
		{
			if (m_client->getStatus() == CLIENT::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == CLIENT::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		RPC::DatabaseMetaMessage msg(metaRecord);
		DbInfo* meta = new DbInfo(m_nameCompare);
		meta->charset = &charsets[msg.charsetID];
		meta->name = msg.dbName;
		meta->m_id = msg.id;
		put(meta->name.c_str(), msg.head->checkpoint.logOffset, meta);
		return meta;
	}
	DbInfo* MetaDataCollection::getDatabaseMetaFromRemote(const char* dbName, uint64_t offset) {
		const char* metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askDatabaseMeta(dbName, offset)); i++)
		{
			if (m_client->getStatus() == CLIENT::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == CLIENT::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		RPC::DatabaseMetaMessage msg(metaRecord);
		DbInfo* meta = new DbInfo(m_nameCompare);
		meta->charset = &charsets[msg.charsetID];
		meta->name = msg.dbName;
		meta->m_id = msg.id;
		put(meta->name.c_str(), msg.head->checkpoint.logOffset, meta);
		return meta;
	}
	DLL_EXPORT bool MetaDataCollection::isDataBaseExist(const char* database, uint64_t originCheckPoint)
	{
		return getDatabase(database, originCheckPoint) != nullptr;
	}
	DbInfo* MetaDataCollection::getDatabase(const char* database, uint64_t originCheckPoint)
	{
		DbInfo* currentDB = nullptr;
		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		if (db == nullptr)
		{
			if (m_client)
			{
				if (nullptr == (currentDB = this->getDatabaseMetaFromRemote(database, originCheckPoint)))
					return nullptr;
				else
				{
					put(database, originCheckPoint, currentDB);
					return currentDB;
				}
			}
			return nullptr;
		}
		else
		{
			if (nullptr == (currentDB = db->get(originCheckPoint)))
			{
				if (m_client)
				{
					if (nullptr == (currentDB = this->getDatabaseMetaFromRemote(database, originCheckPoint)))
						return nullptr;
					else
					{
						put(database, originCheckPoint, currentDB);
						return currentDB;
					}
				}
				return nullptr;
			}
			else
				return currentDB;
		}
	}
	DLL_EXPORT TableMeta* MetaDataCollection::get(uint64_t tableID) {
		TableMeta* meta = m_allTables.get(tableID);
		if (meta != nullptr)
			return meta;
		if (m_client)
		{
			if ((meta = getTableMetaFromRemote(tableID)) == nullptr)
				return nullptr;
			else
				return meta;
		}
		else
			return nullptr;
	}
	DLL_EXPORT TableMeta* MetaDataCollection::getPrevVersion(uint64_t tableID) {
		return m_allTables.getPrevVersion(tableID);
	}
	int MetaDataCollection::put(const char* database, uint64_t offset, DbInfo* dbmeta)
	{
		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		if (db == nullptr)
		{
			db = new MetaTimeline<DbInfo>(offset, database);
			db->put(dbmeta, offset);
			barrier;
			if (!m_dbs.insert(std::pair<const char*, MetaTimeline<DbInfo>*>(db->getName().c_str(), db)).second)
			{
				delete db;
				return -1;
			}
			else
				return 0;
		}
		else
		{
			if (0 > db->put(dbmeta, offset))
				return -1;
			else
				return 0;
		}
	}
	DLL_EXPORT int MetaDataCollection::put(const char* database, const charsetInfo* charset, uint64_t originCheckPoint)
	{
		DbInfo* db = new DbInfo(m_nameCompare);
		db->charset = charset;
		db->name = database;
		if (0 != put(database, originCheckPoint, db))
		{
			delete db;
			return -1;
		}
		return 0;
	}

	DLL_EXPORT int MetaDataCollection::put(const char* database, const char* table,
		TableMeta* meta, uint64_t originCheckPoint)
	{
		DbInfo* currentDB = nullptr;
		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		bool newMeta = false;
		if (db == nullptr)
		{
			if (m_client)
			{
				if (nullptr == (currentDB = this->getDatabaseMetaFromRemote(database, originCheckPoint)))
				{
					LOG(ERROR) << "can not get database meta from reomte server";
					return -1;
				}
			}
			else
			{
				LOG(ERROR) << "unknown database:" << database;
				return -1;
			}
		}
		else
			currentDB = db->get(originCheckPoint);
		MetaTimeline<TableMeta>* metas;
		getTableInfo(table, metas, currentDB);
		if (metas == nullptr)
		{
			newMeta = true;
			metas = new MetaTimeline<TableMeta>(m_maxTableId++, table);
		}
		if (meta == nullptr)
		{
			metas->disableCurrent(originCheckPoint);
		}
		else
		{
			if (0 != metas->put(meta, originCheckPoint))//here meta id will be set
			{
				if (newMeta)
					delete metas;
				LOG(ERROR) << "put new table meta to MetaTimeline failed";
				return -1;
			}
			if (newMeta)
			{
				barrier;
				currentDB->tables.insert(std::pair<const char*, MetaTimeline<TableMeta>* >(meta->m_tableName.c_str(), metas));
			}
			m_allTables.put(meta);
		}
		return 0;
	}

	DLL_EXPORT MetaTimeline<TableMeta>* MetaDataCollection::_getTableInfo(const char* database, const char* table)
	{
		DbInfo* currentDB = nullptr;
		MetaTimeline<DbInfo>* db;
		getDbInfo(database, db);
		if (db == nullptr || (currentDB = db->get()) == nullptr)
			return nullptr;
		MetaTimeline<TableMeta>* metas;
		getTableInfo(table, metas, currentDB);
		return metas;
	}

	int MetaDataCollection::createDatabase(const ddl* database, uint64_t originCheckPoint)
	{
		if (getDatabase(static_cast<const dataBaseDDL*>(database)->name.c_str(), originCheckPoint) != nullptr)
		{
			LOG(ERROR) << "create database failed for database:" << static_cast<const dataBaseDDL*>(database)->name << " exist";
			return -1;
		}
		else
		{
			DbInfo* current = new DbInfo(m_nameCompare);
			current->name = static_cast<const dataBaseDDL*>(database)->name;
			current->charset = static_cast<const dataBaseDDL*>(database)->charset;
			if (current->charset == nullptr)
				current->charset = m_defaultCharset;
			return put(current->name.c_str(), originCheckPoint, current);
		}
	}
	int MetaDataCollection::alterDatabase(const ddl* database,
		uint64_t originCheckPoint)
	{
		DbInfo* current = getDatabase(static_cast<const dataBaseDDL*>(database)->name.c_str(), originCheckPoint);
		if (current == nullptr)
		{
			LOG(ERROR) << "alter database failed for database" << static_cast<const dataBaseDDL*>(database)->name << " not exist";
			return -1;
		}
		else
		{
			if (static_cast<const dataBaseDDL*>(database)->charset != nullptr)
				current->charset = static_cast<const dataBaseDDL*>(database)->charset;
			if (!static_cast<const dataBaseDDL*>(database)->collate.empty())
				current->collate = static_cast<const dataBaseDDL*>(database)->collate;//todo 
			return 0;
		}
	}
	int MetaDataCollection::dropDatabase(const ddl* database, uint64_t originCheckPoint)
	{
		MetaTimeline<DbInfo>* db;
		getDbInfo(static_cast<const dataBaseDDL*>(database)->name.c_str(), db);
		if (db == nullptr)
		{
			LOG(ERROR) << "drop database failed for database: " << static_cast<const dataBaseDDL*>(database)->name << " not exist";
			return -1;
		}
		else
		{
			return db->disableCurrent(originCheckPoint);
		}
	}
	int MetaDataCollection::createTable(const ddl* tableDDL,
		uint64_t originCheckPoint)
	{
		const createTableDDL* table = static_cast<const createTableDDL*>(tableDDL);
		TableMeta* meta = new TableMeta(m_nameCompare.caseSensitive);
		*meta = table->tableDef;
		if (meta->m_dbName.empty())
		{
			if (table->usedDb.empty())
			{
				LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
				delete meta;
				return -1;
			}
			meta->m_dbName = table->usedDb;
		}
		DbInfo* currentDb = getDatabase(meta->m_dbName.c_str(), originCheckPoint);
		if (currentDb == nullptr)
		{
			LOG(ERROR) << "unknown database :" << meta->m_dbName;
			delete meta;
			return -1;
		}
		if (meta->m_charset == nullptr)
			meta->m_charset = currentDb->charset;
		for (int idx = 0; idx < meta->m_columnsCount; idx++)
		{
			if (columnInfos[TID(meta->m_columns[idx].m_columnType)].stringType && meta->m_columns[idx].m_charset == nullptr)
			{
				meta->m_columns[idx].m_charset = meta->m_charset;
				if (meta->m_columns[idx].m_size != 0)
					meta->m_columns[idx].m_size *= meta->m_charset->byteSizePerChar;
			}
			if (meta->m_columns[idx].testFlag(COL_FLAG_PRIMARY_KEY))
			{
				std::list<std::string> pk;
				pk.push_back(meta->m_columns[idx].m_columnName);
				if (0 != meta->createPrimaryKey(pk))
				{
					delete meta;
					LOG(ERROR) << "create table failed for create primary key failed";
					return -1;
				}
			}
			if (meta->m_columns[idx].testFlag(COL_FLAG_UNIQUE_KEY))
			{
				std::list<std::string> uk;
				uk.push_back(meta->m_columns[idx].m_columnName);
				if (0 != meta->addIndex(meta->m_columns[idx].m_columnName.c_str(), uk, KEY_TYPE::UNIQUE_KEY))
				{
					delete meta;
					LOG(ERROR) << "create table failed for add unique key failed";
					return -1;
				}
			}
			if (meta->m_columns[idx].testFlag(COL_FLAG_INDEX))
			{
				std::list<std::string> index;
				index.push_back(meta->m_columns[idx].m_columnName);
				if (0 != meta->addIndex(meta->m_columns[idx].m_columnName.c_str(), index, KEY_TYPE::INDEX))
				{
					delete meta;
					LOG(ERROR) << "create table failed for add index failed";
					return -1;
				}
			}
			if (columnInfos[static_cast<int>(meta->m_columns[idx].m_columnType)].stringType && meta->m_columns[idx].m_charset == nullptr)
				meta->m_columns[idx].m_charset = meta->m_charset;
		}
		if (!table->primaryKey.columnNames.empty())
		{
			if (0 != meta->createPrimaryKey(table->primaryKey.columnNames))
			{
				delete meta;
				LOG(ERROR) << "create table failed for create primary key failed";
				return -1;
			}
		}
		for (std::list<addKey>::const_iterator iter = table->uniqueKeys.begin(); iter != table->uniqueKeys.end(); iter++)
		{
			if (0 != meta->addIndex((*iter).name.c_str(), (*iter).columnNames, KEY_TYPE::UNIQUE_KEY))
			{
				delete meta;
				LOG(ERROR) << "create table failed for add unique key failed";
				return -1;
			}
		}
		for (std::list<addKey>::const_iterator iter = table->indexs.begin(); iter != table->indexs.end(); iter++)
		{
			if (0 != meta->addIndex((*iter).name.c_str(), (*iter).columnNames, KEY_TYPE::INDEX))
			{
				delete meta;
				LOG(ERROR) << "create table failed for add index failed";
				return -1;
			}
		}
		meta->buildColumnOffsetList();
		if (0
			!= put(meta->m_dbName.c_str(), meta->m_tableName.c_str(), meta, originCheckPoint))
		{
			LOG(ERROR) << "insert new meta of table " << meta->m_dbName << "." << meta->m_tableName << " failed";
			delete meta;
			return -1;
		}
		return 0;
	}
	int MetaDataCollection::createTableLike(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct createTableLike* table = static_cast<const struct createTableLike*>(tableDDL);
		const char* db = table->likedTable.database.empty() ? (table->usedDb.empty() ? nullptr : table->usedDb.c_str()) : table->likedTable.database.c_str();
		if (db == nullptr)
		{
			LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
			return -1;
		}

		const TableMeta* srcTable = get(db, table->likedTable.table.c_str(), originCheckPoint);
		if (srcTable == nullptr)
		{
			LOG(ERROR) << "create table like " << db << "." << table->likedTable.table << " failed for liked table not exist";
			return -1;
		}
		if ((db = table->getDatabase()) == nullptr)
		{
			LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
			return -1;
		}
		if (nullptr != get(db, table->table.table.c_str(), originCheckPoint))
		{
			LOG(ERROR) << "create table " << db << "." << table->table.table << " failed for table exist";
			return -1;
		}

		TableMeta* meta = new TableMeta(srcTable->m_nameCompare.caseSensitive);
		*meta = *srcTable;
		meta->m_dbName.assign(db);
		meta->m_tableName.assign(table->table.table);
		if (put(db, table->table.table.c_str(), meta, originCheckPoint) != 0)
		{
			delete meta;
			LOG(ERROR) << "create table " << db << "." << table->table.table << " failed";
			return -1;
		}
		return 0;
	}
	int MetaDataCollection::dropTable(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct dropTable* tables = static_cast<const struct dropTable*>(tableDDL);
		for (std::list<tableName>::const_iterator iter = tables->tables.begin(); iter != tables->tables.end(); iter++)
		{
			const char* database = (*iter).database.empty() ? (tableDDL->usedDb.empty() ? nullptr : tableDDL->usedDb.c_str()) : (*iter).database.c_str();
			if (database == nullptr)
			{
				LOG(ERROR) << "drop table failed for no database selected:";
				continue;
			}
			dropTable(database, (*iter).table.c_str(), originCheckPoint);
		}
		return 0;
	}

	int MetaDataCollection::dropTable(const char* database, const char* table, uint64_t originCheckPoint)
	{
		if (database == nullptr)
		{
			LOG(ERROR) << "drop table failed for no database selected:";
			return -1;
		}
		if (get(database, table, originCheckPoint) == nullptr)
		{
			LOG(ERROR) << "drop table failed for :" << database << "." << table << " not exists";
			return -1;
		}
		if (put(database, table, nullptr, originCheckPoint) != 0)
		{
			LOG(ERROR) << "drop table " << database << "." << table << " failed";
			return -1;
		}
		return 0;
	}
	int MetaDataCollection::renameTable(const char* srcDatabase, const char* srcTable, const char* destDatabase, const char* destTable, uint64_t originCheckPoint)
	{
		const TableMeta* src = get(srcDatabase, srcTable);
		if (src == nullptr)
		{
			LOG(ERROR) << "rename table failed for src table not exist";
			return -1;
		}
		if (m_nameCompare.compare(srcDatabase, destDatabase) == 0 &&
			m_nameCompare.compare(srcTable, destTable) != 0 &&
			get(destDatabase, destTable) != nullptr)
		{
			LOG(ERROR) << "rename table failed for dest table exist";
			return -1;
		}
		TableMeta* newTable = new TableMeta(m_nameCompare.caseSensitive);
		*newTable = *get(srcDatabase, srcTable);
		newTable->m_tableName = destTable;
		newTable->m_dbName = destDatabase;

		if (0 != put(destDatabase, destTable, newTable, originCheckPoint))
		{
			LOG(ERROR) << "rename table failed";
			delete newTable;
			return -1;
		}
		put(srcDatabase, srcTable, nullptr, originCheckPoint);
		return 0;
	}
	int MetaDataCollection::renameTable(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct renameTable* tables = static_cast<const struct renameTable*>(tableDDL);
		assert(tables->src.size() == tables->dest.size());
		std::list<tableName>::const_iterator src = tables->src.begin(), dest = tables->dest.begin();

		for (; src != tables->src.end(); src++)
		{
			const char* db, * destDb;
			if ((*src).database.empty())
			{
				if (tables->usedDb.empty())
				{
					LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
					return -1;
				}
				db = tables->usedDb.c_str();
			}
			else
				db = (*src).database.c_str();

			if ((*dest).database.empty())
			{
				if (tables->usedDb.empty())
				{
					LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
					return -1;
				}
				destDb = tables->usedDb.c_str();
			}
			else
				destDb = (*dest).database.c_str();

			if (get(db, (*src).table.c_str()) == nullptr)
			{
				LOG(ERROR) << "rename table failed for " << (*src).table << " not exist,ddl:" << tableDDL->rawDdl;
				return -1;
			}
			if (getDatabase(destDb) == nullptr)
			{
				LOG(ERROR) << "rename table failed for database " << destDb << " not exist,ddl:" << tableDDL->rawDdl;
				return -1;
			}
			if (get(destDb, (*dest).table.c_str()) != nullptr)
			{
				LOG(ERROR) << "rename table failed for " << (*dest).table << " exist,ddl:" << tableDDL->rawDdl;
				return -1;
			}
			dest++;
		}
		src = tables->src.begin();
		dest = tables->dest.begin();
		for (; src != tables->src.end(); src++)
		{
			const char* db, * destDb;
			if ((*src).database.empty())
				db = tables->usedDb.c_str();
			else
				db = (*src).database.c_str();
			if ((*dest).database.empty())
				destDb = tables->usedDb.c_str();
			else
				destDb = (*dest).database.c_str();
			renameTable(db, (*src).table.c_str(), destDb, (*dest).table.c_str(), originCheckPoint);
			dest++;
		}
		return 0;
	}
	int MetaDataCollection::createIndex(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct createIndex* table = static_cast<const struct createIndex*>(tableDDL);
		const TableMeta* meta;
		const char* database = table->getDatabase();
		if (database == nullptr)
		{
			LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
			return -1;
		}
		if ((meta = get(database, table->table.table.c_str(), originCheckPoint)) == nullptr)
		{
			LOG(ERROR) << "unknown table :" << table->table.table;
			return -1;
		}
		TableMeta* newVersion = new TableMeta(meta->m_nameCompare.caseSensitive);
		int ret = 0;
		if (table->index.type == ALTER_TABLE_ADD_UNIQUE_KEY)
			ret = newVersion->addIndex(table->index.name.c_str(), table->index.columnNames, KEY_TYPE::UNIQUE_KEY);
		else
			ret = newVersion->addIndex(table->index.name.c_str(), table->index.columnNames, KEY_TYPE::INDEX);
		if (ret != 0)
		{
			delete newVersion;
			LOG(ERROR) << "create index failed";
			return -1;
		}
		if (0 != put(newVersion->m_dbName.c_str(), newVersion->m_tableName.c_str(), newVersion, originCheckPoint))
		{
			delete newVersion;
			LOG(ERROR) << "process create index failed for put new meta to collection failed";
			return -1;
		}
		return 0;
	}
	int MetaDataCollection::dropIndex(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct dropIndex* table = static_cast<const struct dropIndex*>(tableDDL);
		const TableMeta* meta;
		const char* database = table->getDatabase();
		if (database == nullptr)
		{
			LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
			return -1;
		}
		if ((meta = get(database, table->table.table.c_str(), originCheckPoint)) == nullptr)
		{
			LOG(ERROR) << "unknown table :" << table->table.table;
			return -1;
		}
		TableMeta* newVersion = new TableMeta(meta->m_nameCompare.caseSensitive);
		int ret = 0;
		if (newVersion->getIndex(table->name.c_str()) != nullptr)
			ret = newVersion->dropIndex(table->name.c_str());
		else if (newVersion->getUniqueKey(table->name.c_str()) != nullptr)
			ret = newVersion->dropUniqueKey(table->name.c_str());
		else
		{
			LOG(ERROR) << "drop index failed for index not exist";
			delete newVersion;
			return -1;
		}
		if (ret != 0)
		{
			delete newVersion;
			LOG(ERROR) << "drop index failed";
			return -1;
		}
		if (0 != put(newVersion->m_dbName.c_str(), newVersion->m_tableName.c_str(), newVersion, originCheckPoint))
		{
			delete newVersion;
			LOG(ERROR) << "process drop index failed for put new meta to collection failed";
			return -1;
		}
		return 0;
	}
	int MetaDataCollection::alterTable(const ddl* tableDDL, uint64_t originCheckPoint)
	{
		const struct alterTable* table = static_cast<const struct alterTable*>(tableDDL);
		const TableMeta* meta;
		const char* database = table->getDatabase();
		if (database == nullptr)
		{
			LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
			return -1;
		}
		if ((meta = get(database, table->table.table.c_str(), originCheckPoint)) == nullptr)
		{
			LOG(ERROR) << "unknown table :" << table->table.table;
			return -1;
		}
		TableMeta* newVersion = new TableMeta(meta->m_nameCompare.caseSensitive);
		*newVersion = *meta;
		int ret = 0;
		bool renameTable = false;
		for (std::list<alterTableHead*>::const_iterator iter = table->detail.begin(); iter != table->detail.end(); iter++)
		{
			switch ((*iter)->type)
			{
			case ALTER_TABLE_RENAME:
			{
				const alterRenameTable* dest = static_cast<const alterRenameTable*>(*iter);
				const char* destDatabase = dest->destTable.database.empty() ? (table->usedDb.empty() ? nullptr : table->usedDb.c_str()) : dest->destTable.database.c_str();
				if (destDatabase == nullptr)
				{
					LOG(ERROR) << "no database selected for ddl:" << tableDDL->rawDdl;
					ret = -1;
					break;
				}
				if (m_nameCompare.compare(database, destDatabase) != 0 || m_nameCompare.compare(dest->destTable.table.c_str(), table->table.table.c_str()) != 0)
				{
					renameTable = true;
					if (get(destDatabase, dest->destTable.table.c_str(), originCheckPoint) != nullptr)
					{
						LOG(ERROR) << "rename table failed for dest table exist";
						ret = -1;
						break;
					}
				}
				newVersion->m_dbName = destDatabase;
				newVersion->m_tableName = dest->destTable.table;
				break;
			}
			case ALTER_TABLE_ADD_COLUMN:
				ret = newVersion->addColumn(&static_cast<const struct addColumn*>((*iter))->column, static_cast<const struct addColumn*>(*iter)->afterColumnName.empty() ? nullptr : static_cast<const struct addColumn*>(*iter)->afterColumnName.c_str(), static_cast<const struct addColumn*>(*iter)->first);
				break;
			case ALTER_TABLE_ADD_COLUMNS:
			{
				for (std::list<ColumnMeta*>::const_iterator citer = static_cast<const struct addColumns*>(*iter)->columns.begin(); citer != static_cast<const struct addColumns*>(*iter)->columns.end(); citer++)
				{
					if (0 != (ret = newVersion->addColumn(*citer)))
						break;
				}
				break;
			}
			case ALTER_TABLE_RENAME_COLUMN:
				ret = newVersion->renameColumn(static_cast<const struct renameColumn*>(*iter)->srcColumnName.c_str(), static_cast<const struct renameColumn*>(*iter)->destColumnName.c_str());
				break;
			case ALTER_TABLE_MODIFY_COLUMN:
				ret = newVersion->modifyColumn(&static_cast<const struct addColumn*>(*iter)->column, static_cast<const struct addColumn*>(*iter)->first, static_cast<const struct addColumn*>(*iter)->afterColumnName.empty() ? nullptr : static_cast<const struct addColumn*>(*iter)->afterColumnName.c_str());
				break;
			case ALTER_TABLE_CHANGE_COLUMN:
				ret = newVersion->changeColumn(&static_cast<const struct changeColumn*>(*iter)->column, static_cast<const struct changeColumn*>(*iter)->srcColumnName.c_str(), static_cast<const struct changeColumn*>(*iter)->first,
					static_cast<const struct changeColumn*>(*iter)->afterColumnName.empty() ? nullptr : static_cast<const struct changeColumn*>(*iter)->afterColumnName.c_str());
				break;
			case ALTER_TABLE_DROP_COLUMN:
				ret = newVersion->dropColumn(static_cast<const struct dropColumn*>(*iter)->columnName.c_str());
				break;
			case ALTER_TABLE_ADD_INDEX:
				ret = newVersion->addIndex(static_cast<const struct addKey*>(*iter)->name.c_str(), static_cast<const struct addKey*>(*iter)->columnNames, KEY_TYPE::INDEX);
				break;
			case ALTER_TABLE_DROP_INDEX:
				ret = newVersion->dropIndex(static_cast<const struct dropKey*>(*iter)->name.c_str());
				break;
			case ALTER_TABLE_RENAME_INDEX:
				ret = newVersion->renameIndex(static_cast<const struct renameKey*>(*iter)->srcKeyName.c_str(), static_cast<const struct renameKey*>(*iter)->destKeyName.c_str());
				break;
			case ALTER_TABLE_ADD_UNIQUE_KEY:
				ret = newVersion->addIndex(static_cast<const struct addKey*>(*iter)->name.c_str(), static_cast<const struct addKey*>(*iter)->columnNames, KEY_TYPE::UNIQUE_KEY);
				break;
			case ALTER_TABLE_DROP_UNIQUE_KEY:
				ret = newVersion->dropUniqueKey(static_cast<const struct dropKey*>(*iter)->name.c_str());
				break;
			case ALTER_TABLE_ADD_PRIMARY_KEY:
				ret = newVersion->createPrimaryKey(static_cast<const struct addKey*>(*iter)->columnNames);
				break;
			case ALTER_TABLE_DROP_PRIMARY_KEY:
				ret = newVersion->dropPrimaryKey();
				break;
			case ALTER_TABLE_DEFAULT_CHARSET:
				ret = newVersion->defaultCharset(static_cast<const struct defaultCharset*>(*iter)->charset, static_cast<const struct defaultCharset*>(*iter)->collate.empty() ? nullptr : static_cast<const struct defaultCharset*>(*iter)->collate.c_str());
				break;
			case ALTER_TABLE_CONVERT_DEFAULT_CHARSET:
				ret = newVersion->convertDefaultCharset(static_cast<const struct defaultCharset*>(*iter)->charset, static_cast<const struct defaultCharset*>(*iter)->collate.empty() ? nullptr : static_cast<const struct defaultCharset*>(*iter)->collate.c_str());
				break;
			default:
				ret = 0;
				break;
			}
			if (ret != 0)
				break;
		}

		if (ret == 0)
		{
			if (0 != put(newVersion->m_dbName.c_str(), newVersion->m_tableName.c_str(), newVersion, originCheckPoint))
			{
				delete newVersion;
				LOG(ERROR) << "process alter table ddl failed for put new meta to collection failed";
				return -1;
			}
			if (renameTable)
				dropTable(database, table->table.table.c_str(), originCheckPoint);
			return 0;
		}
		else
			return ret;
	}


	int MetaDataCollection::processDDL(const struct ddl* ddl, uint64_t originCheckPoint)
	{
		switch (ddl->m_type)
		{
		case CREATE_DATABASE:
			return createDatabase(ddl, originCheckPoint);
		case DROP_DATABASE:
			return dropDatabase(ddl, originCheckPoint);
		case ALTER_DATABASE:
			return alterDatabase(ddl, originCheckPoint);
		case CREATE_TABLE:
			return createTable(ddl, originCheckPoint);
		case CREATE_TABLE_LIKE:
			return createTableLike(ddl, originCheckPoint);
		case CREATE_TABLE_FROM_SELECT:
		{
			LOG(ERROR) << "not support create table from select now";
			return -1;
		}
		case DROP_TABLE:
			return dropTable(ddl, originCheckPoint);
		case RENAME_TABLE:
			return renameTable(ddl, originCheckPoint);
		case CREATE_INDEX:
			return createIndex(ddl, originCheckPoint);
		case DROP_INDEX:
			return dropIndex(ddl, originCheckPoint);
		case ALTER_TABLE:
			return alterTable(ddl, originCheckPoint);
		case CREATE_VIEW:
		{
			LOG(ERROR) << "not support create view now,but we do not need it here,ignore";
			return -1;
		}
		case CREATE_TRIGGER:
		{
			LOG(ERROR) << "not support create trigger now,but we do not need it here,ignore";
			return -1;
		}
		default:
		{
			LOG(ERROR) << "unknown ddl type:" << ddl->m_type << ",query is:" << ddl->rawDdl;
			return -1;
		}
		}
	}

	DLL_EXPORT int MetaDataCollection::processDDL(const char* ddl, const char* database, uint64_t originCheckPoint)
	{
		handle* h = nullptr;
		if (OK != m_sqlParser->parse(h, database, ddl))
		{
			LOG(ERROR) << "parse ddl :" << ddl << " failed";
			return -1;
		}
		handle* currentHandle = h;
		while (currentHandle != nullptr)
		{
			const struct ddl* ddlInfo = static_cast<const struct ddl*>(currentHandle->userData);
			if (ddlInfo != nullptr)
				processDDL(ddlInfo, originCheckPoint);
			currentHandle = currentHandle->next;
		}
		delete h;
		return 0;
	}
	DLL_EXPORT int MetaDataCollection::purge(uint64_t originCheckPoint)
	{
		for (dbTree::iterator iter = m_dbs.begin(); iter != m_dbs.end(); iter++)
		{
			MetaTimeline<DbInfo>* db = iter->second;
			if (db == nullptr)
				continue;
			db->purge(originCheckPoint);
			DbInfo* dbMeta = db->get(0xffffffffffffffffULL);
			if (dbMeta == nullptr)
				continue;
			for (tbTree::iterator titer = dbMeta->tables.begin(); titer != dbMeta->tables.begin(); titer++)
			{
				MetaTimeline<TableMeta>* table = titer->second;
				if (table == nullptr)
					continue;
				table->purge(originCheckPoint);
			}
		}
		return 0;
	}
	DLL_EXPORT int MetaDataCollection::setDefaultCharset(const charsetInfo* defaultCharset)
	{
		m_defaultCharset = defaultCharset;
		return 0;
	}
	DLL_EXPORT const charsetInfo* MetaDataCollection::getDefaultCharset()
	{
		return m_defaultCharset;
	}
	DLL_EXPORT void MetaDataCollection::print()
	{
		for (dbTree::iterator iter = m_dbs.begin(); iter != m_dbs.end(); iter++)
		{
			MetaTimeline<DbInfo>* db = iter->second;
			DbInfo* currentDB = db->get(0xffffffffffffffffULL);
			for (tbTree::iterator titer = currentDB->tables.begin(); titer != currentDB->tables.begin(); titer++)
			{
				MetaTimeline<TableMeta>* metas = titer->second;
				TableMeta* currentTable = metas->get(0xffffffffffffffffULL);
				printf("%s\n", currentTable->toString().c_str());
			}
		}
	}
}

