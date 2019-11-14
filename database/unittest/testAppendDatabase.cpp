#include "databaseTestUtil.h"
int testAppend()
{
	META::tableMeta* tl[tableType::MAX_TYPE];
	std::list<int> allrecords;
	std::map<int,uint64_t> rmap;
	for (int i = 0; i < tableType::MAX_TYPE; i++)
	{
		char buf[20] = { 0 };
		sprintf(buf,"table_%d", i);
		tl[i] = createTable("test", buf, static_cast<tableType>(i), ckp++, dbs);
	}
	uint64_t first = ckp + 1;
	for (int i = 0; i < 1000000; i++)
	{
		int transReconrdCount = abs(rand()) % 50;
		db->begin();
		for (int t = 0; t < transReconrdCount; t++)
		{
			int tid = abs(rand()) % tableType::MAX_TYPE;
			int op = abs(rand()) % 3;
			DATABASE_INCREASE::DMLRecord* r;
			int pk = i + (rand()%100);
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
			std::map<int, uint64_t>::iterator iiter = rmap.insert(std::pair<int, uint64_t>(pk, *(uint64_t*)r->column(4))).first;
			if (iiter != rmap.end())
				iiter->second = *(uint64_t*)r->column(4);
			db->insert(r);
		}
		db->commit();
	}
	DATABASE::databaseCheckpointIterator iter(0,nullptr,db);
	assert(iter.seek(&first));
	for (std::list<int>::iterator riter = allrecords.begin(); riter != allrecords.end(); riter++)
	{
		const char* iv = (const char*)iter.value();
		DATABASE_INCREASE::DMLRecord ir(iv, dbs);
		assert(mod1ValueCheck(&ir, *riter));
		iter.next();
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



