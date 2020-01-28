#include "databaseTestUtil.h"
int testAppend()
{
	META::tableMeta* tl[tableType::MAX_TYPE];
	std::list<int> allrecords;
	std::map<int,uint64_t> rmap[tableType::MAX_TYPE];
	for (int i = 0; i < tableType::MAX_TYPE; i++)
	{
		char buf[20] = { 0 };
		sprintf(buf,"table_%d", i);
		tl[i] = createTable("test", buf, static_cast<tableType>(i), ckp++, dbs);
	}
	uint64_t first = ckp;
	LOG(ERROR)<<"insert start";

	for (int i = 0; i < 40; i++)
	{
		int transReconrdCount = abs(rand()) % 50;
		db->begin();
		for (int t = 0; t < transReconrdCount; t++)
		{
			int tid = abs(rand()) % tableType::MAX_TYPE;
			int op = abs(rand()) % 3;
			DATABASE_INCREASE::DMLRecord* r;
			int pk = i + (rand()%100);
			//LOG(INFO)<<pk<<" "<<tid;
			allrecords.push_back(pk);
			switch (op)
			{
			case 0:
				r = createInsertRecord_mod1(tl[tid], pk);
				break;
			case 1:
				r = createUpdateRecord_mod1(tl[tid], pk,(abs(rand())%100)>95);
				break;
			case 2:
				r = createDeleteRecord_mod1(tl[tid], pk);
				break;
			}
			std::map<int, uint64_t>::iterator iiter = rmap[tid].insert(std::pair<int, uint64_t>(pk, r->head->logOffset)).first;
			if (iiter != rmap[tid].end())
				iiter->second = r->head->logOffset;
			if(r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_UPDATE)||
					r->head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_REPLACE))
			{
				iiter = rmap[tid].insert(std::pair<int, uint64_t>(*(const int*)r->oldColumnOfUpdateType(0), r->head->logOffset)).first;
				if (iiter != rmap[tid].end())
					iiter->second = r->head->logOffset;
			}
			db->insert(r);
			delete r;
		}
		db->commit();
	}
	LOG(ERROR)<<"insert finished";
	db->flushLogs();
	std::this_thread::sleep_for(std::chrono::seconds(1));
	DATABASE::databaseCheckpointIterator *iter = new DATABASE::databaseCheckpointIterator(0,nullptr,db);
	assert(iter->seek(&first));
	int c=0;
	for (std::list<int>::iterator riter = allrecords.begin(); riter != allrecords.end(); riter++)
	{
		const char* iv = (const char*)iter->value();
		DATABASE_INCREASE::DMLRecord ir(iv, dbs);
		assert(mod1ValueCheck(&ir, *riter));
		iter->next();
		c++;
	}
	for(std::map<int, uint64_t>::iterator iiter =  rmap[INT_PRIMARY_KEY].begin();iiter!=rmap[INT_PRIMARY_KEY].end();iiter++)
	{
		int k = iiter->first;
		const char* iv = db->getRecord(tl[INT_PRIMARY_KEY],META::KEY_TYPE::PRIMARY_KEY,0,&k);
		DATABASE_INCREASE::DMLRecord ir(iv, dbs);
		if(*(const int*)ir.column(0)==k)
			assert(mod1ValueCheck(&ir, k));
		else
			assert(ir.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_UPDATE)&&*(const int*)ir.oldColumnOfUpdateType(0)==k);
		assert(ir.head->logOffset==iiter->second);
	}
	LOG(INFO)<<"test count:"<<c;
	delete iter;
	db->stop();
	delete db;

	db = new DATABASE::database("test", &conf, &pool, dbs);
	if (0 != db->load())
		return -1;
	if (0 != db->start())
		return -1;
	DATABASE::databaseCheckpointIterator iter1(0,nullptr,db);
	assert(iter1.seek(&first));
	c=0;
	for (std::list<int>::iterator riter = allrecords.begin(); riter != allrecords.end(); riter++)
	{
		const char* iv = (const char*)iter1.value();
		DATABASE_INCREASE::DMLRecord ir(iv, dbs);
		assert(mod1ValueCheck(&ir, *riter));
		iter1.next();
		c++;
	}
	for(std::map<int, uint64_t>::iterator iiter =  rmap[INT_PRIMARY_KEY].begin();iiter!=rmap[INT_PRIMARY_KEY].end();iiter++)
	{
		int k = iiter->first;
		const char* iv = db->getRecord(tl[INT_PRIMARY_KEY],META::KEY_TYPE::PRIMARY_KEY,0,&k);
		DATABASE_INCREASE::DMLRecord ir(iv, dbs);
		if(*(const int*)ir.column(0)==k)
			assert(mod1ValueCheck(&ir, k));
		else
			assert(ir.head->minHead.type == static_cast<uint8_t>(DATABASE_INCREASE::RecordType::R_UPDATE)&&*(const int*)ir.oldColumnOfUpdateType(0)==k);
		assert(ir.head->logOffset==iiter->second);
	}

	return 0;
}
int main()
{
	if (0 != init())
	{
		LOG(ERROR) << "start failed";
		return -1;
	}
	if (0 != testAppend())
	{
		LOG(ERROR) << "test failed";
		return -1;
	}
	db->stop();
	delete db;
	delete dbs;
	return 0;
}



