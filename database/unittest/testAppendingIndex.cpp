#include "database/appendingIndex.h"
#include "meta/metaDataCollection.h"
#include "database/database.h"
#include "message/record.h"
#include "meta/columnType.h"
#include "sqlParser/sqlParser.h"
#include <map>
#include <string>
#ifdef OS_WIN
#include <windows.h>
#define mysqlFuncLib "mysqlParserFuncs.dll"
#define mysqlParserTree "ParseTree"
#endif
#ifdef OS_LINUX
#define mysqlFuncLib "lib/libmysqlParserFuncs.so"
#define mysqlParserTree "sqlParser/ParseTree"
#endif

	META::metaDataCollection* dbs;
	DATABASE::database* db;
	uint64_t ckp = 1;
	uint64_t rid = 1;
	int testAppendingIndex()
	{
		dbs->processDDL("drop database if exists test ",nullptr, ckp++);
		dbs->processDDL("create database test ", nullptr, ckp++);
		dbs->processDDL("create table test1(a int primary key,b char(20),c int)", "test", ckp++);
		META::tableMeta* t1 = dbs->get("test", "test1");
		assert(t1 != nullptr);
		DATABASE::appendingIndex idx(t1->m_primaryKey, t1);
		std::map<int, int> t1Kv;
		char rbuf[1024] = { 0 };
		for (int i = 1; i < 20000; i++)
		{
			int k = rand()%200000;
			DATABASE_INCREASE::DMLRecord r(rbuf, t1, DATABASE_INCREASE::RecordType::R_INSERT);
			r.head->logOffset = ckp++;
			r.head->recordId = i;
			r.head->timestamp = 1573438210 + i;
			r.head->txnId = i;
			r.setFixedColumn(0, k);
			r.setVarColumn(1, "dwadfw", 6);
			r.setFixedColumn(2, i);
			r.finishedSet();
			if(!t1Kv.insert(std::pair<int, int>(k, i)).second)
				t1Kv.find(k)->second = i;
			idx.append(&r, i);
		}
		DATABASE::appendingIndex::iterator<int> aiter(0, &idx);
		assert(aiter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			assert(idx.find<int>(&k)==iter->second);
			assert(*(int*)aiter.key() == k);
			uint32_t v = aiter.value();
			assert(v == (uint32_t)iter->second);
			aiter.nextKey();
		}
		const char * solidIndx = idx.toString<int>();
		DATABASE::page p;
		p.pageId = 1;
		p.pageData= (char*)solidIndx;
		DATABASE::fixedSolidIndex fsi(&p);
		DATABASE::solidIndexIterator<int , DATABASE::fixedSolidIndex> siter(0, &fsi);
		assert(siter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			int ks = *(int*)fsi.getKey(fsi.find<int>(k, true));
			assert(ks == iter->first);
			assert(*(int*)siter.key() == k);
			uint32_t v = siter.value();
			assert(v == (uint32_t)iter->second);
			siter.nextKey();
		}
		return 0;
	}
	int testAppendingIndexUnion()
	{
		dbs->processDDL("drop database if exists test ", nullptr, ckp++);
		dbs->processDDL("create database test ", nullptr, ckp++);
		dbs->processDDL("create table test2(a int,b char(20),c varchar(30),d int ,primary key(a,b,c))", "test", ckp++);
		META::tableMeta* t1 = dbs->get("test", "test2");
		assert(t1 != nullptr);
		DATABASE::appendingIndex idx(t1->m_primaryKey, t1);
		std::map<int, int> t1Kv;
		char rbuf[1024] = { 0 };
		for (int i = 1; i < 20000; i++)
		{
			int k = rand()%200000;
			char sbuf[30] = { 0 };
			DATABASE_INCREASE::DMLRecord r(rbuf, t1, DATABASE_INCREASE::RecordType::R_INSERT);
			r.head->logOffset = ckp++;
			r.head->recordId = i;
			r.head->timestamp = 1573438210 + i;
			r.head->txnId = i;
			r.setFixedColumn(0, k);
			sprintf(sbuf, "%d__AS_sdf_%dw", k, k * 123);
			r.setVarColumn(1, sbuf, strlen(sbuf));
			sprintf(sbuf, "dwad%d_&sdf_%dw", k, k * 123);
			r.setVarColumn(2, sbuf, strlen(sbuf));
			r.setFixedColumn(3, i);
			r.finishedSet();
			if(!t1Kv.insert(std::pair<int, int>(k, i)).second)
				t1Kv.find(k)->second = i;
			idx.append(&r, i);
		}
		DATABASE::appendingIndex::iterator<META::unionKey> aiter(0, &idx);
		assert(aiter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			char ubuf[60] = { 0 };
			char sbuf[30] = { 0 };
			META::unionKey uk(ubuf, t1->m_primaryKey);
			int off = uk.appendValue(&k, 4, 0, 2);
			sprintf(sbuf, "%d__AS_sdf_%dw", k, k * 123);
			off = uk.appendValue(sbuf, strlen(sbuf), 1, off);
			sprintf(sbuf, "dwad%d_&sdf_%dw", k, k * 123);
			off = uk.appendValue(sbuf, strlen(sbuf), 1, off);
			*(uint16_t*)&ubuf[0] = off - 2;
			assert(idx.find<META::unionKey>(&uk) == iter->second);
			uint32_t v = aiter.value();
			assert(v == (uint32_t)iter->second);
			aiter.nextKey();
		}
		const char* solidIndx = idx.toString<META::unionKey>();
		DATABASE::page p;
		p.pageId = 1;
		p.pageData= (char*)solidIndx;
		DATABASE::varSolidIndex fsi(&p);
		DATABASE::solidIndexIterator<META::unionKey, DATABASE::varSolidIndex> siter(0, &fsi);
		assert(siter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			char ubuf[60] = { 0 };
			char sbuf[30] = { 0 };
			META::unionKey uk(ubuf, t1->m_primaryKey);
			int off = uk.appendValue(&k, 4, 0, 2);
			sprintf(sbuf, "%d__AS_sdf_%dw", k, k * 123);
			off = uk.appendValue(sbuf, strlen(sbuf), 1, off);
			sprintf(sbuf, "dwad%d_&sdf_%dw", k, k * 123);
			off = uk.appendValue(sbuf, strlen(sbuf), 1, off);
			*(uint16_t*)&ubuf[0] = off - 2;
			META::unionKey ukf((char*)fsi.getKey(fsi.find<META::unionKey>(uk, true)), t1->m_primaryKey);
			META::unionKey uk1((char *)siter.key(), t1->m_primaryKey);
			assert(uk1 == uk);
			assert(ukf == uk);

			uint32_t v = siter.value();
			assert(v == (uint32_t)iter->second);
			siter.nextKey();
		}
		return 0;
	}
	int testAppendingIndexUnionFixed()
	{
		dbs->processDDL("drop database if exists test ", nullptr, ckp++);
		dbs->processDDL("create database test ", nullptr, ckp++);
		dbs->processDDL("create table test2(a int,b bigint,c smallint,d int ,primary key(a,b,c))", "test", ckp++);
		META::tableMeta* t1 = dbs->get("test", "test2");
		assert(t1 != nullptr);
		DATABASE::appendingIndex idx(t1->m_primaryKey, t1);
		std::map<int, int> t1Kv;
		char rbuf[1024] = { 0 };
		for (int i = 1; i < 20000; i++)
		{
			int k = rand()%200000;
			int64_t b = k * k;
			short c = k;
			DATABASE_INCREASE::DMLRecord r(rbuf, t1, DATABASE_INCREASE::RecordType::R_INSERT);
			r.head->logOffset = ckp++;
			r.head->recordId = i;
			r.head->timestamp = 1573438210 + i;
			r.head->txnId = i;
			r.setFixedColumn(0, k);
			r.setFixedColumn(1, b);
			r.setFixedColumn(2, c);
			r.setFixedColumn(3, k);
			r.finishedSet();
			if(!t1Kv.insert(std::pair<int, int>(k, i)).second)
				t1Kv.find(k)->second = i;
			idx.append(&r, i);
		}
		DATABASE::appendingIndex::iterator<META::unionKey> aiter(0, &idx);
		assert(aiter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			int64_t b = k * k; 
			short c = k;
			char ubuf[60] = { 0 };
			META::unionKey uk(ubuf, t1->m_primaryKey);
			int off = uk.appendValue(&k, 4, 0, 0);
			off = uk.appendValue(&b,8, 1, off);
			off = uk.appendValue(&c,2, 2, off);
			assert(idx.find<META::unionKey>(&uk) == iter->second);
			uint32_t v = aiter.value();
			assert(v == (uint32_t)iter->second);
			aiter.nextKey();
		}
		const char* solidIndx = idx.toString<META::unionKey>();
		DATABASE::page p;
		p.pageId = 1;
		p.pageData= (char*)solidIndx;
		DATABASE::fixedSolidIndex fsi(&p);
		DATABASE::solidIndexIterator<META::unionKey, DATABASE::fixedSolidIndex> siter(0, &fsi);
		assert(siter.begin());
		for (std::map<int, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			int k = iter->first;
			int64_t b = k * k;
			short c = k;
			char ubuf[60] = { 0 };
			META::unionKey uk(ubuf, t1->m_primaryKey);
			int off = uk.appendValue(&k, 4, 0, 0);
			off = uk.appendValue(&b, 8, 1, off);
			off = uk.appendValue(&c, 2, 2, off);
			META::unionKey ukf((char*)fsi.getKey(fsi.find<META::unionKey>(uk, true)), t1->m_primaryKey);
			META::unionKey uk1((char*)siter.key(), t1->m_primaryKey);
			assert(uk1 == uk);
			assert(ukf == uk);

			uint32_t v = siter.value();
			assert(v == (uint32_t)iter->second);
			siter.nextKey();
		}
		return 0;
	}
	int testAppendingIndexBinary()
	{
		dbs->processDDL("drop database if exists test ", nullptr, ckp++);
		dbs->processDDL("create database test ", nullptr, ckp++);
		dbs->processDDL("create table test3(a char(30),b int ,primary key(a))", "test", ckp++);
		META::tableMeta* t1 = dbs->get("test", "test3");
		assert(t1 != nullptr);
		DATABASE::appendingIndex idx(t1->m_primaryKey, t1);
		std::map<std::string, int> t1Kv;
		std::list<char*> buflist;

		for (int i = 1; i < 20000; i++)
		{
			int k = rand()%200000;
			char sbuf[30] = { 0 };
			sprintf(sbuf, "%d__AS_sdf_%dw", k, k * 123);
			if (t1Kv.find(sbuf) == t1Kv.end())
			{
				char* rbuf = new char[256];
				buflist.push_back(rbuf);
				DATABASE_INCREASE::DMLRecord r(rbuf, t1, DATABASE_INCREASE::RecordType::R_INSERT);
				r.head->logOffset = ckp++;
				r.head->recordId = i;
				r.head->timestamp = 1573438210 + i;
				r.head->txnId = i;
				r.setVarColumn(0, sbuf, strlen(sbuf));
				r.setFixedColumn(1, i);
				r.finishedSet();
				t1Kv.insert(std::pair<std::string, int>(sbuf, i));
				idx.append(&r, i);
			}
		}
		DATABASE::appendingIndex::iterator<META::binaryType> aiter(0, &idx);
		assert(aiter.begin());
		for (std::map<std::string, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			std::string k = iter->first;
			META::binaryType b(k.c_str(),k.size());
			assert(idx.find<META::binaryType>(&b) == iter->second);
			uint32_t v = aiter.value();
			assert(v == (uint32_t)iter->second);
			aiter.nextKey();
		}
		const char* solidIndx = idx.toString<META::binaryType>();
		DATABASE::page p;
		p.pageId = 1;
		p.pageData= (char*)solidIndx;
		DATABASE::varSolidIndex fsi(&p);
		DATABASE::solidIndexIterator<META::binaryType, DATABASE::varSolidIndex> siter(0, &fsi);
		assert(siter.begin());
		for (std::map<std::string, int>::iterator iter = t1Kv.begin(); iter != t1Kv.end(); iter++)
		{
			std::string k = iter->first;
			META::binaryType b(k.c_str(), k.size());
			const char* ffk = (const char*)fsi.getKey(fsi.find<META::binaryType>(b, true));
			META::binaryType ukf(ffk+sizeof(uint16_t) ,*(uint16_t*)ffk);
			META::binaryType uk1((char*)siter.key()+sizeof(uint16_t), *(uint16_t*)siter.key());
			assert(uk1 == b);
			assert(ukf == b);

			uint32_t v = siter.value();
			assert(v == (uint32_t)iter->second);
			siter.nextKey();
		}
		for (std::list<char*>::iterator iter = buflist.begin(); iter != buflist.end(); iter++)
			delete[] * iter;
		return 0;
	}
int main()
{
	initKeyWords();
	dbs = new META::metaDataCollection("utf8");
	if (0 != dbs->initSqlParser(mysqlParserTree, mysqlFuncLib))
	{
		printf("load sqlparser failed");
		return -1;
	}
	testAppendingIndex();
	testAppendingIndexUnion();
	testAppendingIndexUnionFixed();
	testAppendingIndexBinary();
	LOG(INFO)<<"finished";
	delete dbs;
	return 0;
}
