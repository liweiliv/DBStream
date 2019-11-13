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
	NO_PK
};
static DATABASE::database* db;
static META::metaDataCollection* dbs;
static char mod1RecordBuf[1024];
static uint64_t ckp = 1;
META::tableMeta* createTable_INT_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30),f timestamp)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_CHAR_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30) primary key)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_FIXED_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30), primary key(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_VAR_PRIMARY_KEY(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30), primary key(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_INT_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int unique key,b int,c char(30))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_CHAR_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30) unique key)",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_FIXED_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30), unique key uk1(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_VAR_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30), unique key uk1(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_MUTIL_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30), unique key uk1(a,c),unique key uk2(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_PK_AND_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30), unique key uk1(a,c))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_PK_AND_MULTI_UK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int primary key,b int,c char(30), unique key uk1(a,c),unique key uk1(a,b))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable_NO_PK(const char * db,const char * table,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create table `%s` (a int ,b int,c char(30))",table);
	dbs->processDDL(sql,db,checkpoint);
	return dbs->get(db,table);
}
META::tableMeta* createTable(const char * db,const char * table,tableType type,uint64_t checkpoint,META::metaDataCollection *dbs)
{
	char sql[512] = {0};
	snprintf(sql,511,"create database if not exist `%s`",db);
	dbs->processDDL(sql,nullptr,checkpoint);
	snprintf(sql,511,"drop table if exist `%s`",table);
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
int createInsertRecord_mod1(META::tableMeta* table,int pk)
{
	char sbuf[31] = {0};
	DATABASE_INCREASE::DMLRecord *r = new DATABASE_INCREASE::DMLRecord(mod1RecordBuf, table, DATABASE_INCREASE::RecordType::R_INSERT);
	r->head->logOffset = ckp++;
	r->head->timestamp = META::timestamp::create(time(nullptr),0);
	r->setFixedColumn(0, pk);
	r->setFixedColumn(1, pk*pk);
	snprintf(sbuf,30,"%dwad%d_sw",pk);
	r->setVarColumn(2, sbuf, strlen(sbuf));
	r->setFixedColumn(3, pk>>2);
	r->finishedSet();
	return r;
}
int createUpdateRecord_mod1(META::tableMeta* table,int pk,bool updatePk)
{
	int keyid =0;
	META::KEY_TYPE type;
	char * oldRecord;
	if(table->m_primaryKey!=nullptr)
	{
		keyid = 0;
		type = META::KEY_TYPE::PRIMARY_KEY;
	}
	else if(table->m_uniqueKeysCount>0)
	{
		keyid = 0;
		type = META::KEY_TYPE::UNIQUE_KEY;
	}
	else
	{
			//todo
	}

	DATABASE_INCREASE::DMLRecord *r = db->
	char sbuf[31] = {0};
	DATABASE_INCREASE::DMLRecord *r = new DATABASE_INCREASE::DMLRecord(mod1RecordBuf, table, DATABASE_INCREASE::RecordType::R_INSERT);
	r->head->logOffset = ckp++;
	r->head->timestamp = META::timestamp::create(time(nullptr),0);
	r->setFixedColumn(0, pk);
	r->setFixedColumn(1, pk*pk);
	snprintf(sbuf,30,"%dwad%d_sw",pk);
	r->setVarColumn(2, sbuf, strlen(sbuf));
	r->setFixedColumn(3, pk>>2);
	r->finishedSet();
	return r;
}

#endif /* DATABASETESTUTIL_H_ */
