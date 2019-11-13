#include "database/appendingBlock.h"
DATABASE::database* db;
META::metaDataCollection* dbs;
uint64_t ckp =1;
int testAppend()
{
	DATABASE::appendingBlock block(1,0,1024*1024*32,0,0,1,db,dbs);
	dbs->processDDL("drop database if exists test ",nullptr, ckp++);
	dbs->processDDL("create database test ", nullptr, ckp++);
	dbs->processDDL("create table test1(a int primary key,b char(20),c int)", "test", ckp++);
	dbs->processDDL("create table test2(a int ,b char(20),c timestamp,d int,unique key bb(b))", "test", ckp++);
	dbs->processDDL("create table test3(a int primary key,b char(20),c int,d int,unique key bb(b,c))", "test", ckp++);
	dbs->processDDL("create table test4(a int primary key,b char(20),c int,d int,unique key bb(b,c),unique key dd(d))", "test", ckp++);

	META::tableMeta* t1 = dbs->get("test", "test1");
	META::tableMeta* t2 = dbs->get("test", "test2");
	META::tableMeta* t3 = dbs->get("test", "test3");
	META::tableMeta* t4 = dbs->get("test", "test4");

	assert(t1 != nullptr);
	assert(t2 != nullptr);
	assert(t3 != nullptr);
	assert(t4 != nullptr);

	DATABASE::appendingIndex idx(t1->m_primaryKey, t1);
	std::map<int, int> t1Kv;
	char rbuf[1024] = { 0 };
	for (int i = 1; i < 20000; i++)
	{
		int k = rand();
		if (t1Kv.find(k) == t1Kv.end())
		{
			DATABASE_INCREASE::DMLRecord r(rbuf, t1, DATABASE_INCREASE::RecordType::R_INSERT);
			r.head->logOffset = ckp++;
			r.head->recordId = i;
			r.head->timestamp = 1573438210 + i;
			r.head->txnId = i;
			r.setFixedColumn(0, k);
			r.setVarColumn(1, "dwadfw", 6);
			r.setFixedColumn(2, i);
			r.finishedSet();
			t1Kv.insert(std::pair<int, int>(k, i));
			idx.append(&r, i);
		}
	}
}




