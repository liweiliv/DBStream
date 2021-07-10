#include "database/appendingIndex.h"
#include "meta/metaDataCollection.h"
#include <assert.h>
#include <map>
bool testIntIndex()
{
	META::metaDataCollection metas("utf8",nuulptr);
	assert(0==metas.processDDL("create database test"),1);
	assert(0 == metas.processDDL("create table test.testint (id int,c1 varchar(20))"),2);
	META::tableMeta* meta = metas.get("test", "testint", 2);
	DB_INSTANCE::appendingIndex idx(meta->m_primaryKey.keyIndexs, meta->m_primaryKey.count,meta,nullptr);
	std::map<int, RPC::DMLRecord*> m;
	for (int i = 0; i < 1024; )
	{
		char * data = new char[256];
		RPC::DMLRecord *r = new RPC::DMLRecord(data, meta, RPC::R_INSERT);
		r->setFixedColumn(0, rand());
		r->setVarColumn(1, "1234567890", 10);
		r->head->logOffset = 3;
		r->head->recordId = 1;
		r->head->headSize = sizeof(r1.head);
		r->head->version = 1;
		if (m.insert(std::pair<int, RPC::DMLRecord*>(*(int*)r->column(0), r)).second)
		{
			idx.append(r, i);
			i++;
		}
		else
		{
			delete r;
			delete data;
			continue;
		}
	}
	DB_INSTANCE::a
	for (std::map<int, RPC::DMLRecord*>::iterator iter = m.begin(); iter != m.end(); iter++)
	{

	}


}
