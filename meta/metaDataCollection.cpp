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
#include "../sqlParser/sqlParser.h"
#include "../store/client/client.h"
#include "charset.h"
#include "metaTimeline.h"
#include "../message/record.h"
#include "metaChangeInfo.h"
#include "../util/barrier.h"
#include "../util/likely.h"
using namespace SQL_PARSER;
using namespace DATABASE_INCREASE;
namespace META {
	typedef spp::sparse_hash_map<const char*, MetaTimeline<tableMeta>*, StrHash, StrCompare> tbTree;
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
	struct dbInfo
	{
		tbTree tables;
		uint64_t m_id;
		std::string name;
		const charsetInfo* charset;
	};
	metaDataCollection::metaDataCollection(const char * defaultCharset,STORE::client *client) :m_dbs(),m_sqlParser(nullptr),m_client(client),
		 m_maxTableId(1),m_maxDatabaseId(0)
	{
		m_defaultCharset = getCharset(defaultCharset);
		for (uint16_t i = 0; i < MAX_CHARSET; i++)
			m_charsetSizeList.insert(std::pair<const char*, const charsetInfo*>(charsets[i].name, &charsets[i]));
	}
	int metaDataCollection::initSqlParser(const char * sqlParserTreeFile,const char * sqlParserFunclibFile)
	{
		m_sqlParser = new sqlParser();
		if(0!=m_sqlParser->LoadFuncs(sqlParserFunclibFile))
		{
			delete m_sqlParser;
			m_sqlParser = nullptr;
			return -1;
		}
		if(0!=m_sqlParser->LoadParseTreeFromFile(sqlParserTreeFile))
		{
			delete m_sqlParser;
			m_sqlParser = nullptr;
			return -1;
		}
		return 0;
	}
	metaDataCollection::~metaDataCollection()
	{
		if (m_sqlParser != NULL)
			delete m_sqlParser;
	}
	tableMeta * metaDataCollection::get(const char * database, const char * table,
		uint64_t originCheckPoint)
	{
		MetaTimeline<dbInfo>* db;
		getDbInfo(database, db);
		if (db == NULL)
			return NULL;
		dbInfo * currentDB = db->get(originCheckPoint);
		if (currentDB == NULL)
			return NULL;
		MetaTimeline<tableMeta>* metas;
		getTableInfo(table, metas, currentDB);
		if (metas == NULL)
			return NULL;
		return metas->get(originCheckPoint);
	}
	const charsetInfo* metaDataCollection::getDataBaseCharset(const char* database, uint64_t originCheckPoint)
	{

		MetaTimeline<dbInfo>* db;
		getDbInfo(database, db);
		if (db == NULL)
			return NULL;
		dbInfo* currentDB = db->get(originCheckPoint);
		if (currentDB == NULL)
			return NULL;
		return currentDB->charset;
	}

	tableMeta *metaDataCollection::getTableMetaFromRemote(uint64_t tableID) {
		const char * metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askTableMeta(tableID)); i++)
		{
			if (m_client->getStatus() == STORE::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == STORE::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		DATABASE_INCREASE::TableMetaMessage msg(metaRecord);
		tableMeta * meta = new tableMeta(&msg);
		put(meta->m_dbName.c_str(), meta->m_tableName.c_str(), meta, msg.head->logOffset);
		return meta;
	}
	tableMeta *metaDataCollection::getTableMetaFromRemote(const char * database, const char * table, uint64_t originCheckPoint) {
		const char * metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askTableMeta(database, table,originCheckPoint)); i++)
		{
			if (m_client->getStatus() == STORE::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == STORE::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		TableMetaMessage msg(metaRecord);
		tableMeta * meta = new tableMeta(&msg);
		put(meta->m_dbName.c_str(), meta->m_tableName.c_str(), meta, msg.head->logOffset);
		return meta;
	}
	dbInfo *metaDataCollection::getDatabaseMetaFromRemote(uint64_t databaseID) {
		const char * metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askDatabaseMeta(databaseID)); i++)
		{
			if (m_client->getStatus() == STORE::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == STORE::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		DATABASE_INCREASE::DatabaseMetaMessage msg(metaRecord);
		dbInfo * meta = new dbInfo();
		meta->charset = &charsets[msg.charsetID];
		meta->name = msg.dbName;
		meta->m_id = msg.id;
		put(meta->name.c_str(), msg.head->logOffset, meta);
		return meta;
	}
	dbInfo *metaDataCollection::getDatabaseMetaFromRemote(const char * dbName, uint64_t offset) {
		const char * metaRecord = nullptr;
		for (int i = 0; i < 10 && nullptr == (metaRecord = m_client->askDatabaseMeta(dbName, offset)); i++)
		{
			if (m_client->getStatus() == STORE::client::IDLE)
				return nullptr;
			else if (m_client->getStatus() == STORE::client::DISCONNECTED)
				m_client->connect();
			std::this_thread::yield();
		}
		if (metaRecord == nullptr)
			return nullptr;
		DATABASE_INCREASE::DatabaseMetaMessage msg(metaRecord);
		dbInfo * meta = new dbInfo();
		meta->charset = &charsets[msg.charsetID];
		meta->name = msg.dbName;
		meta->m_id = msg.id;
		put(meta->name.c_str(), msg.head->logOffset, meta);
		return meta;
	}
	tableMeta *metaDataCollection::get(uint64_t tableID) {
		tableMeta * meta = m_allTables.get(tableID);
		if (meta!=nullptr)
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
	tableMeta* metaDataCollection::getPrevVersion(uint64_t tableID) {
		return m_allTables.getPrevVersion(tableID);
	}
	int metaDataCollection::put(const char * database, uint64_t offset, dbInfo *dbmeta)
	{
		MetaTimeline<dbInfo>* db;
		getDbInfo(database, db);
		if (db == NULL)
		{
			db = new MetaTimeline<dbInfo>(offset,database);
			db->put(dbmeta,offset);
			barrier;
			if (!m_dbs.insert(std::pair<const char*, MetaTimeline<dbInfo>*>(db->getName().c_str(), db)).second)
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
	int metaDataCollection::put(const char* database, const charsetInfo* charset, uint64_t originCheckPoint)
	{
		dbInfo* db = new dbInfo();
		db->charset = charset;
		db->name = database;
		if (0 != put(database, originCheckPoint, db))
		{
			delete db;
			return -1;
		}
		return 0;
	}

	int metaDataCollection::put(const char * database, const char * table,
		tableMeta * meta, uint64_t originCheckPoint)
	{
		dbInfo * currentDB = nullptr;
		MetaTimeline<dbInfo>* db;
		getDbInfo(database, db);
		bool newMeta = false;
		if (db == NULL)
		{
			if (m_client)
			{
				if (nullptr == (currentDB = this->getDatabaseMetaFromRemote(database, originCheckPoint)))
					return -1;
			}
			return -1;
		}
		else
			currentDB = db->get(originCheckPoint);
		MetaTimeline<tableMeta>* metas;
		getTableInfo(table, metas, currentDB);
		if (metas == NULL)
		{
			newMeta = true;
			metas = new MetaTimeline<tableMeta>(m_maxTableId++,table);
		}
		metas->put(meta, originCheckPoint);//here meta id will be set

		if (newMeta)
		{
			barrier;
			currentDB->tables.insert(std::pair<const char*, MetaTimeline<tableMeta>* >(meta->m_tableName.c_str(), metas));
		}
		m_allTables.put(meta);
		return 0;
	}
	static void copyColumn(columnMeta & column, const newColumnInfo* src)
	{
		column.m_srcColumnType = src->rawType;//todo
		column.m_columnType = src->type;
		column.m_columnIndex = src->index;
		column.m_columnName = src->name;
		column.m_decimals = src->decimals;
		column.m_generated = src->generated;
		column.m_isPrimary = src->isPrimary;
		column.m_isUnique = src->isUnique;
		column.m_precision = src->precision;
		column.m_setAndEnumValueList.m_Count = 0;
		column.m_setAndEnumValueList.m_array = (char**)malloc(
			sizeof(char*) * src->setAndEnumValueList.size());
		for (std::list<std::string>::const_iterator iter = src->setAndEnumValueList.begin();
			iter != src->setAndEnumValueList.end(); iter++)
		{
			column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count] =
				(char*)malloc((*iter).size() + 1);
			memcpy(
				column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count],
				(*iter).c_str(), (*iter).size());
			column.m_setAndEnumValueList.m_array[column.m_setAndEnumValueList.m_Count][(*iter).size()] =
				'\0';
			column.m_setAndEnumValueList.m_Count++;
		}
		column.m_signed = src->isSigned;
		column.m_size = src->size;
	}
	int metaDataCollection::createTable(handle * h, const newTableInfo *t,
		uint64_t originCheckPoint)
	{
		Table newTable = t->table;
		if (!h->dbName.empty())
			newTable.database = h->dbName;
		if (newTable.database.empty())
		{
			printf("no database\n");
			return -1;
		}
		MetaTimeline<dbInfo>* db;
		getDbInfo(newTable.database.c_str(), db);
		if (db == NULL)
		{
			printf("unknown database :%s\n", newTable.database.c_str());
			return -1;
		}

		dbInfo * currentDb = db->get(originCheckPoint);
		if (currentDb == NULL)
		{
			printf("unknown database :%s\n", newTable.database.c_str());
			return -1;
		}

		tableMeta * meta = new tableMeta;
		meta->m_columns = new columnMeta[t->newColumns.size()];
		meta->m_tableName = newTable.table;
		meta->m_dbName = currentDb->name;
		meta->m_charset = t->defaultCharset;
		if (meta->m_charset == nullptr)
			meta->m_charset = currentDb->charset;

		for (std::list<newColumnInfo*>::const_iterator iter = t->newColumns.begin();
			iter != t->newColumns.end(); iter++)
		{
			newColumnInfo * c = *iter;
			columnMeta & column = meta->m_columns[meta->m_columnsCount];
			copyColumn(column, c);
			if (columnInfos[c->type].stringType)
			{
				if (c->charset == nullptr)
					column.m_charset = meta->m_charset;
				column.m_size *= column.m_charset->byteSizePerChar;
			}
			column.m_columnIndex = meta->m_columnsCount++;
		}
		uint32_t ukCount = 0;
		for (list<newKeyInfo*>::const_iterator iter = t->newKeys.begin();
			iter != t->newKeys.end(); iter++)
			if ((*iter)->type == newKeyInfo::UNIQUE_KEY)
				ukCount++;
		if (ukCount > 0)
			meta->m_uniqueKeys = new keyInfo[ukCount];
		for (auto k : t->newKeys)
		{
			keyInfo * key = nullptr;
			if (k->type == newKeyInfo::PRIMARY_KEY)
			{
				meta->m_primaryKey.name = "primary key";
				key = &meta->m_primaryKey;
			}
			else if (k->type == newKeyInfo::UNIQUE_KEY)
			{
				key = &meta->m_uniqueKeys[meta->m_uniqueKeysCount++];
				key->name = k->name;
			}
			else if (k->type == newKeyInfo::KEY)
				continue;//todo
			if(k->columns.size()>0)
				key->keyIndexs = new uint16_t[k->columns.size()];
			for (auto name : k->columns)
			{
				columnMeta * _c = (columnMeta*)meta->getColumn(
					name.c_str());
				if (_c == NULL)
				{
					printf("can not find column %s in columns\n",
						name.c_str());
					delete meta;
					return -1;
				}
				if (k->type == newKeyInfo::PRIMARY_KEY)
					_c->m_isPrimary = true;
				else if (k->type == newKeyInfo::UNIQUE_KEY)
					_c->m_isUnique = true;
				else if (k->type == newKeyInfo::KEY)
					continue;//todo
				key->keyIndexs[key->count++] = _c->m_columnIndex;
			}
		}
		meta->buildColumnOffsetList();
		if (0
			!= put(newTable.database.c_str(), newTable.table.c_str(), meta, originCheckPoint))
		{
			printf("insert new meta of table %s.%s failed",
				newTable.database.c_str(), newTable.table.c_str());
			delete meta;
			return -1;
		}
		printf("%s\n",meta->toString().c_str());
		return 0;
	}
	int metaDataCollection::createTableLike(handle * h, const newTableInfo *t,
		uint64_t originCheckPoint)
	{
		Table newTable = t->table;
		if (!h->dbName.empty())
			newTable.database = h->dbName;
		if (newTable.database.empty())
		{
			printf("no database\n");
			return -1;
		}
		MetaTimeline<dbInfo>* db;
		getDbInfo(newTable.database.c_str(), db);
		if (db == NULL)
		{
			printf("unknown database :%s\n", newTable.database.c_str());
			return -1;
		}
		Table likedTable = t->likedTable;
		if (!h->dbName.empty())
			likedTable.database = h->dbName;
		if (likedTable.database.empty())
		{
			printf("no database\n");
			return -1;
		}
		tableMeta * likedMeta = get(likedTable.database.c_str(),
			likedTable.table.c_str(), originCheckPoint);
		if (likedMeta == NULL)
		{
			printf("create liked table %s.%s is not exist",
				likedTable.database.c_str(), likedTable.table.c_str());
			return -1;
		}
		tableMeta * meta = new tableMeta;
		*meta = *likedMeta;
		meta->m_tableName = newTable.table;
		if (0
			!= put(newTable.database.c_str(), newTable.table.c_str(), meta,
				originCheckPoint))
		{
			printf("insert new meta of table %s.%s failed",
				newTable.database.c_str(), newTable.table.c_str());
			delete meta;
			return -1;
		}
		return 0;
	}
	int metaDataCollection::alterTable(handle * h, const newTableInfo *t,
		uint64_t originCheckPoint)
	{
		Table newTable = t->table;
		if (!h->dbName.empty())
			newTable.database = h->dbName;
		if (newTable.database.empty())
		{
			printf("no database\n");
			return -1;
		}
		tableMeta * meta = get(newTable.database.c_str(), newTable.table.c_str(),
				originCheckPoint);
		if (meta == NULL)
		{
			printf("unknown table %s.%s\n", newTable.database.c_str(),
				newTable.table.c_str());
			return -1;
		}
		tableMeta * newMeta = new tableMeta;
		*newMeta = *meta;
		/*update charset*/
		if (t->defaultCharset != nullptr)
			newMeta->m_charset = t->defaultCharset;
		/*update new column*/
		for (list<newColumnInfo*>::const_iterator iter = t->newColumns.begin();
			iter != t->newColumns.end(); iter++)
		{
			newColumnInfo * c = *iter;
			columnMeta column;
			copyColumn(column, c);
			/*update default charset and string size*/
			if (columnInfos[c->type].stringType)
			{
				if (c->charset == nullptr)
					column.m_charset = meta->m_charset;
				column.m_size *= column.m_charset->byteSizePerChar;
			}
			columnMeta * modifiedColumn = (columnMeta*)meta->getColumn(c->name.c_str());
			if (c->after)
			{
				/*不能alter table modify column_a after column_a，先执行drop是安全的*/
				if (modifiedColumn != NULL)
				{
					if (0 != newMeta->dropColumn(modifiedColumn->m_columnIndex))
					{
						printf("drop column %s in %s.%s failed\n",
							column.m_columnName.c_str(),
							newTable.database.c_str(), newTable.table.c_str());
						delete newMeta;
						return -1;
					}
				}
				if (0 != newMeta->addColumn(&column, c->afterColumnName.c_str()))
				{
					printf("add column %s after %s in %s.%s failed\n",
						column.m_columnName.c_str(), c->afterColumnName.c_str(),
						newTable.database.c_str(), newTable.table.c_str());
					delete newMeta;
					return -1;
				}

			}
			else
			{
				if (modifiedColumn != NULL) //modify column,only update column
				{
					column.m_columnIndex = modifiedColumn->m_columnIndex;
					*modifiedColumn = column;
				}
				else
				{
					if (0 != newMeta->addColumn(&column))
					{
						printf("add column %s in %s.%s failed\n",
							column.m_columnName.c_str(),
							newTable.database.c_str(), newTable.table.c_str());
						delete newMeta;
						return -1;
					}
				}
			}
		}
		/*drop old column*/
		for (list<string>::const_iterator iter = t->oldColumns.begin();
			iter != t->oldColumns.end(); iter++)
		{
			if (0 != newMeta->dropColumn((*iter).c_str()))
			{
				printf("alter table drop column %s ,but it is not exist in %s.%s\n",
					(*iter).c_str(), newTable.database.c_str(),
					newTable.table.c_str());
				delete newMeta;
				return -1;
			}
		}
		/*update new key*/
		for (list<newKeyInfo*>::const_iterator iter = t->newKeys.begin();
			iter != t->newKeys.end(); iter++)
		{
			const newKeyInfo * key = *iter;
			if (key->type == newKeyInfo::PRIMARY_KEY)
			{
				if (newMeta->createPrimaryKey(key->columns) != 0)
				{
					printf("primary key is exits in %s.%s\n",
						newTable.database.c_str(), newTable.table.c_str());
					delete newMeta;
					return -1;
				}
			}
			else if (key->type == newKeyInfo::UNIQUE_KEY)
			{
				if (newMeta->addUniqueKey(key->name.c_str(), key->columns) != 0)
				{
					printf("unique key %s is exits in %s.%s\n", key->name.c_str(),
						newTable.database.c_str(), newTable.table.c_str());
					delete newMeta;
					return -1;
				}
			}
			else
				continue;
		}
		/*drop old key*/
		for (list<string>::const_iterator iter = t->oldKeys.begin();
			iter != t->oldKeys.end(); iter++)
		{
			if ((*iter) == "PRIMARY")
			{
				newMeta->dropPrimaryKey();
			}
			else
			{
				newMeta->dropUniqueKey((*iter).c_str());
			}
		}
		if (0
			!= put(newTable.database.c_str(), newTable.table.c_str(), newMeta,
				originCheckPoint))
		{
			printf("insert new meta of table %s.%s failed",
				newTable.database.c_str(), newTable.table.c_str());
			delete meta;
			return -1;
		}
		return 0;
	}

	int metaDataCollection::processNewTable(handle * h, const newTableInfo *t,
		uint64_t originCheckPoint)
	{
		if (t->type == newTableInfo::CREATE_TABLE)
		{
			if (t->createLike)
			{
				return createTableLike(h, t, originCheckPoint);
			}
			else
			{
				return createTable(h, t, originCheckPoint);
			}
		}
		else if (t->type == newTableInfo::ALTER_TABLE)
		{
			return alterTable(h, t, originCheckPoint);
		}
		else
			return -1;
	}
	int metaDataCollection::processOldTable(handle * h, const Table *table,
		uint64_t originCheckPoint)
	{
		MetaTimeline<dbInfo> * db = NULL;
		if (table->database.empty())
			return -1;
		getDbInfo(table->database.c_str(), db);
		if (db == NULL)
			return -1;
		dbInfo * currentDB = db->get(originCheckPoint);
		if (currentDB == NULL)
			return -1;

		MetaTimeline<tableMeta>* metas;
		getTableInfo(table->table.c_str(), metas, currentDB);

		if (metas == NULL)
			return -1;
		return metas->disableCurrent(originCheckPoint);
	}

	int metaDataCollection::processDatabase(const databaseInfo * database,
		uint64_t originCheckPoint)
	{
		MetaTimeline<dbInfo>* db;
		getDbInfo(database->name.c_str(), db);
		dbInfo * current = NULL;
		if (db == NULL)
		{
			if (database->type == databaseInfo::CREATE_DATABASE)
			{
				current = new dbInfo;
				current->name = database->name;
				current->charset = database->charset;
				if (current->charset == nullptr)
					current->charset = m_defaultCharset;
				db = new MetaTimeline<dbInfo>(m_maxDatabaseId++,database->name.c_str());
				db->put(current,originCheckPoint);
				barrier;
				if (!m_dbs.insert(std::pair<const char *, MetaTimeline<dbInfo>*>(database->name.c_str(), db)).second)
				{
					delete db;
					return -1;
				}
				else
					return 0;
			}
			else
				return -1;
		}

		if (NULL == (current = db->get(originCheckPoint)))
		{
			if (database->type == databaseInfo::CREATE_DATABASE)
			{
				current = new dbInfo;
				current->name = database->name;
				current->charset = database->charset;
				if (current->charset == nullptr)
					current->charset = m_defaultCharset;
				barrier;
				if (0 != db->put(current, originCheckPoint))
				{
					delete current;
					return -1;
				}
				else
					return 0;
			}
			else
				return -1;
		}

		if (database->type == databaseInfo::CREATE_DATABASE)
			return -1;
		else if (database->type == databaseInfo::ALTER_DATABASE)
		{
			if (database->charset != nullptr)
				current->charset = database->charset;
			return 0;
		}
		else if (database->type == databaseInfo::DROP_DATABASE)
		{
			return db->disableCurrent(originCheckPoint);
		}
		else
			return -1;
	}

	int metaDataCollection::processDDL(const char * ddl, const char * database,uint64_t originCheckPoint)
	{
		handle * h = NULL;
		if (OK != m_sqlParser->parse(h, database,ddl))
		{
			LOG(ERROR)<<"parse ddl :"<<ddl<<" failed";
			return -1;
		}
		handle * currentHandle = h;
		while (currentHandle != NULL)
		{
			metaChangeInfo * meta = static_cast<metaChangeInfo*>(currentHandle->userData);
			meta->print();
			if (meta->database.type
				!= databaseInfo::MAX_DATABASEDDL_TYPE)
			{
				processDatabase(&meta->database, originCheckPoint);
			}
			for (list<newTableInfo*>::const_iterator iter =
				meta->newTables.begin();
				iter != meta->newTables.end(); iter++)
			{
				processNewTable(currentHandle, *iter, originCheckPoint);
			}
			for (list<Table>::const_iterator iter = meta->oldTables.begin();
				iter != meta->oldTables.end(); iter++)
			{
				processOldTable(currentHandle, &(*iter), originCheckPoint);
			}
			currentHandle = currentHandle->next;
		}
		delete h;
		return 0;
	}
	int metaDataCollection::purge(uint64_t originCheckPoint)
	{
		for (dbTree::iterator iter = m_dbs.begin(); iter!=m_dbs.end(); iter++)
		{
			MetaTimeline<dbInfo>* db = iter->second;
			if (db == NULL)
				continue;
			db->purge(originCheckPoint);
			dbInfo * dbMeta = db->get(0xffffffffffffffffUL);
			if (dbMeta == NULL)
				continue;
			for (tbTree::iterator titer = dbMeta->tables.begin(); titer!=dbMeta->tables.begin(); titer++)
			{
				MetaTimeline<tableMeta>* table = titer->second;
				if (table == NULL)
					continue;
				table->purge(originCheckPoint);
			}
		}
		return 0;
	}
	int metaDataCollection::setDefaultCharset(const charsetInfo* defaultCharset)
	{
		m_defaultCharset = defaultCharset;
		return 0;
	}
	const charsetInfo* metaDataCollection::getDefaultCharset()
	{
		return m_defaultCharset;
	}
	void metaDataCollection::print()
	{
		for (dbTree::iterator iter = m_dbs.begin(); iter != m_dbs.end(); iter++)
		{
			MetaTimeline<dbInfo>* db = iter->second;
			dbInfo* currentDB = db->get(0xffffffffffffffffUL);
			for (tbTree::iterator titer = currentDB->tables.begin(); titer != currentDB->tables.begin(); titer++)
			{
				MetaTimeline<tableMeta>* metas = titer->second;
				tableMeta* currentTable = metas->get(0xffffffffffffffffUL);
				printf("%s\n", currentTable->toString().c_str());
			}
		}
	}

}

