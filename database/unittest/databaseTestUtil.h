/*
 * databaseTestUtil.h
 *
 *  Created on: 2019年11月13日
 *      Author: liwei
 */

#ifndef DATABASETESTUTIL_H_
#define DATABASETESTUTIL_H_
#include "database/appendingIndex.h"
#include "meta/metaDataCollection.h"
#include "database/database.h"
#include "message/record.h"
#include "meta/columnType.h"
#include "sqlParser/sqlParser.h"
#include "util/config.h"
#include "util/file.h"
#ifdef OS_WIN
#include <windows.h>
#define mysqlFuncLib "mysqlParserFuncs.dll"
#define mysqlParserTree "D:\\git\\DBStream\\sqlParser\\ParseTree"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
#define mysqlParserTree "sqlParser/ParseTree"
#endif
enum tableType
{
	INT_PRIMARY_KEY,
	CHAR_PRIMARY_KEY,
	FIXED_PRIMARY_KEY,
	VAR_PRIMARY_KEY,
	INT_UK,
	CHAR_UK,
	FIXED_UK,
	VAR_UK,
	MUTIL_UK,
	PK_AND_UK,
	PK_AND_MULTI_UK,
	NO_PK,
	MAX_TYPE
};
static config conf(nullptr);
static DATABASE::database* db;
static META::metaDataCollection* dbs;
static bufferPool pool;
static char mod1RecordBuf[1024];
static uint64_t ckp = 1;
META::tableMeta* createTable_INT_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30),d int,f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_CHAR_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30) primary key,d int,f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_FIXED_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int ,f timestamp, primary key(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_VAR_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int,f timestamp, primary key(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_INT_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int unique key,b int,c char(30),d int,f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_CHAR_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30) unique key,d int ,f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_FIXED_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int,f timestamp, unique key uk1(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_VAR_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int,f timestamp, unique key uk1(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_MUTIL_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int,f timestamp, unique key uk1(a,c),unique key uk2(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_PK_AND_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30),d int,f timestamp, unique key uk1(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_PK_AND_MULTI_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30),d int,f timestamp, unique key uk1(a,c),unique key uk1(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_NO_PK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30),d int,f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable(const char * db,const char * table,tableType type,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create database if not exists `%s`",db);
	dbs->processDDL(sql,nullptr,checkpoint);
	snprintf(sql,511,"drop table if exists `%s`",table);
	dbs->processDDL(sql,db,checkpoint);
	switch(type)
	{
	case INT_PRIMARY_KEY:
		return createTable_INT_PRIMARY_KEY(db,table,checkpoint,dbs);
	case CHAR_PRIMARY_KEY:
		return createTable_CHAR_PRIMARY_KEY(db,table,checkpoint,dbs);
	case FIXED_PRIMARY_KEY:
		return createTable_FIXED_PRIMARY_KEY(db,table,checkpoint,dbs);
	case VAR_PRIMARY_KEY:
		return createTable_VAR_PRIMARY_KEY(db,table,checkpoint,dbs);
	case INT_UK:
		return createTable_INT_UK(db,table,checkpoint,dbs);
	case CHAR_UK:
		return createTable_CHAR_UK(db,table,checkpoint,dbs);
	case FIXED_UK:
		return createTable_FIXED_UK(db,table,checkpoint,dbs);
	case VAR_UK:
		return createTable_VAR_UK(db,table,checkpoint,dbs);
	case MUTIL_UK:
		return createTable_MUTIL_UK(db,table,checkpoint,dbs);
	case PK_AND_UK:
		return createTable_PK_AND_UK(db,table,checkpoint,dbs);
	case PK_AND_MULTI_UK:
		return createTable_PK_AND_MULTI_UK(db,table,checkpoint,dbs);
	case NO_PK:
		return createTable_NO_PK(db,table,checkpoint,dbs);
	default:
		abort();
	}
}
DATABASE_INCREASE::DMLRecord* createInsertRecord_mod1(META::tableMeta* table,int pk)
{
	char sbuf[31] = {0};
	DATABASE_INCREASE::DMLRecord *r = new DATABASE_INCREASE::DMLRecord(mod1RecordBuf, table, DATABASE_INCREASE::RecordType::R_INSERT);
	r->head->logOffset = ckp++;
	r->head->timestamp = META::timestamp::create(time(nullptr),0);
	r->setFixedColumn(0, pk);
	r->setFixedColumn(1, pk*pk);
	snprintf(sbuf,30,"%dwad%d_sw", pk, pk + 1);
	r->setVarColumn(2, sbuf, strlen(sbuf));
	r->setFixedColumn(3, pk>>2);
	r->setFixedColumn(4, META::timestamp::create(time(nullptr), 0));

	r->finishedSet();
	return r;
}
DATABASE_INCREASE::DMLRecord* createUpdateRecord_mod1(META::tableMeta* table,int pk,bool updatePk)
{
	char sbuf[31] = {0};
	DATABASE_INCREASE::DMLRecord *r = new DATABASE_INCREASE::DMLRecord(mod1RecordBuf, table, DATABASE_INCREASE::RecordType::R_UPDATE);
	r->head->logOffset = ckp++;
	r->head->timestamp = META::timestamp::create(time(nullptr),0);
	r->setFixedColumn(0, pk);
	r->setFixedColumn(1, pk*pk);
	snprintf(sbuf,30,"%dwad%d_sw",pk,pk+1);
	r->setVarColumn(2, sbuf, strlen(sbuf));
	r->setFixedColumn(3, pk>>2);
	r->setFixedColumn(4, META::timestamp::create(time(nullptr), 0));
	r->startSetUpdateOldValue();
	if (updatePk)
	{
		for (int i = 0; i < table->m_columnsCount; i++)
		{
			if (table->m_columns[0].m_isPrimary || table->m_columns[0].m_isUnique)
			{
				switch(i)
				{
				case 0:
					r->setFixedUpdatedColumn(0, pk-1);
					break;
				case 1:
					r->setFixedUpdatedColumn(1, (pk - 1) * (pk - 1));
					break;
				case 2:
					snprintf(sbuf, 30, "%dwad%d_sw", pk - 1, pk);
					r->setVardUpdatedColumn(2, sbuf, strlen(sbuf));
					break;
				case 3:
					r->setFixedUpdatedColumn(3, (pk-1)>>1);
					break;
				default:
					break;
				}
			}
		}
	}
	r->setFixedUpdatedColumn(4, META::timestamp::create(time(nullptr)-10, 0));
	r->finishedSet();
	return r;
}
DATABASE_INCREASE::DMLRecord* createDeleteRecord_mod1(META::tableMeta* table, int pk)
{
	char sbuf[31] = { 0 };
	DATABASE_INCREASE::DMLRecord* r = new DATABASE_INCREASE::DMLRecord(mod1RecordBuf, table, DATABASE_INCREASE::RecordType::R_DELETE);
	r->head->logOffset = ckp++;
	r->head->timestamp = META::timestamp::create(time(nullptr), 0);
	r->setFixedColumn(0, pk);
	r->setFixedColumn(1, pk * pk);
	snprintf(sbuf, 30, "%dwad%d_sw", pk, pk + 1);
	r->setVarColumn(2, sbuf, strlen(sbuf));
	r->setFixedColumn(3, pk >> 2);
	r->setFixedColumn(4, META::timestamp::create(time(nullptr), abs(rand())));
	r->finishedSet();
	return r;
}
bool mod1ValueCheck(DATABASE_INCREASE::DMLRecord * dml,int id)
{
	char sbuf[31] = { 0 };
	if (*(int*)dml->column(0) != id)
		return false;
	if (*(int*)dml->column(1) != id * id)
		return false;
	snprintf(sbuf, 30, "%dwad%d_sw", id, id + 1);
	if (dml->varColumnSize(2) != strlen(sbuf))
		return false;
	if (memcmp(sbuf, dml->column(2), strlen(sbuf)) != 0)
		return false;
	if (*(int*)dml->column(3) != id >> 2)
		return false;
	return true;
}
int init()
{
	initKeyWords();
	dbs = new META::metaDataCollection("utf8");
	if (0 != dbs->initSqlParser(mysqlParserTree, mysqlFuncLib))
	{
		printf("load sqlparser failed");
		return -1;
	}
	removeDir("data");
	db = new DATABASE::database("test", &conf, &pool, dbs);
	if (0 != db->load())
		return -1;
	if (0 != db->start())
		return -1;
	return 0;
}
#endif /* DATABASETESTUTIL_H_ */
