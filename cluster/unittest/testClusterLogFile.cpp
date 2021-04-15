#include "cluster/clusterLogFile.h"
using namespace CLUSTER;
DS wFunc(clusterLogFile *logFile)
{
	logFile->use();
	for (int i = 1; i < 100000; i++)
	{
		char buf[sizeof(nodeInfoLogEntry) + 256];
		nodeInfoLogEntry* logEntry = (nodeInfoLogEntry*)buf;
		sprintf(logEntry->host, "%d.%d.%d.%d", i, i, i, i);
		logEntry->size = sizeof(nodeInfoLogEntry) + strlen(logEntry->host);
		logEntry->version = VERSION;
		logEntry->recordType = static_cast<uint8_t>(rpcType::nodeInfoLogEntry);
		logEntry->prevRecordLogIndex.logIndex = i -1;
		logEntry->prevRecordLogIndex.term = 1;
		logEntry->logIndex.logIndex = i;
		logEntry->logIndex.term = 1;
		logEntry->nodeId = i;
		logEntry->port = i % 65535;
		logEntry->opt = 1;
		dsTest(logFile->append(logEntry));
	}
	logFile->finish();
	logFile->unUse();
	dsOk();
}
DS rFunc(clusterLogFile* logFile)
{
	clusterLogFile::iterator iter;
	dsTest(iter.setLogFile(logFile));
	dsTest(iter.search(logIndexInfo(1, 1)));
	const logEntryRpcBase* entry;
	for (int i = 1; i < 100000; i++)
	{
		dsTest(iter.next(entry));
		const nodeInfoLogEntry * logEntry = static_cast<const nodeInfoLogEntry*>(entry);
		assert(logEntry->version == VERSION);
		assert(logEntry->recordType == static_cast<uint8_t>(rpcType::nodeInfoLogEntry));
		assert(logEntry->prevRecordLogIndex.logIndex == (uint64_t)(i -1)) ;
		assert(logEntry->prevRecordLogIndex.term == 1L) ;
		assert(logEntry->logIndex.logIndex == (uint64_t)i);
		assert(logEntry->logIndex.term == 1L);
		assert(logEntry->nodeId == (uint64_t)i);
		assert(logEntry->port == i % 65535);
		assert(logEntry->opt == 1);
	}
	assert(iter.next(entry).code == errorCode::endOfFile);
	printf("finished\n");
	dsOk();
}
void test()
{
	logConfig config;
	remove("./cluster_1.log");
	remove("./cluster_1.log.index");
	clusterLogFile logFile("cluster_1.log", 1, config);
	logIndexInfo index = { 1,1 }, prev = {1,0};
	dsTest(logFile.create(prev, index));
	std::thread w(wFunc, &logFile);
	w.join();
	std::thread r0(rFunc, &logFile);
	std::thread r1(rFunc, &logFile);
	std::thread r2(rFunc, &logFile);
	std::thread r3(rFunc, &logFile);
	r0.join();
	r1.join();
	r2.join();
	r3.join();
	remove("./cluster_1.log");
	remove("./cluster_1.log.index");
}
void testRollback()
{
	logConfig config;
	remove("./cluster_1.log");
	remove("./cluster_1.log.index");
	clusterLogFile logFile("cluster_1.log", 1, config);
	logIndexInfo index = { 1,1 }, prev = { 1,0 };
	dsTest(logFile.create(prev, index));
	logFile.use();
	for (int i = 1; i < 100000; i++)
	{
		char buf[sizeof(nodeInfoLogEntry) + 256];
		nodeInfoLogEntry* logEntry = (nodeInfoLogEntry*)buf;
		sprintf(logEntry->host, "%d.%d.%d.%d", i, i, i, i);
		logEntry->size = sizeof(nodeInfoLogEntry) + strlen(logEntry->host);
		logEntry->version = VERSION;
		logEntry->recordType = static_cast<uint8_t>(rpcType::nodeInfoLogEntry);
		logEntry->prevRecordLogIndex.logIndex = i - 1;
		logEntry->prevRecordLogIndex.term = 1;
		logEntry->logIndex.logIndex = i;
		logEntry->logIndex.term = 1;
		logEntry->nodeId = i;
		logEntry->port = i % 65535;
		logEntry->opt = 1;
		dsTest(logFile.append(logEntry));
	}
	logIndexInfo rollback = { 1,95000 };
	logFile.rollback(rollback);

	clusterLogFile::iterator iter;
	dsTest(iter.setLogFile(&logFile));
	dsTest(iter.search(logIndexInfo(1, 1)));
	const logEntryRpcBase* entry;
	for (int i = 1; i < 95000; i++)
	{
		dsTest(iter.next(entry));
		const nodeInfoLogEntry* logEntry = static_cast<const nodeInfoLogEntry*>(entry);
		assert(logEntry->version == VERSION);
		assert(logEntry->recordType == static_cast<uint8_t>(rpcType::nodeInfoLogEntry));
		assert(logEntry->prevRecordLogIndex.logIndex == (uint64_t)(i - 1));
		assert(logEntry->prevRecordLogIndex.term == 1L);
		assert(logEntry->logIndex.logIndex == (uint64_t)i);
		assert(logEntry->logIndex.term == 1L);
		assert(logEntry->nodeId == (uint64_t)i);
		assert(logEntry->port == i % 65535);
		assert(logEntry->opt == 1);
	}
	iter.next(entry);
	assert(entry == nullptr);
	logFile.unUse();
	logFile.unUse();
	remove("./cluster_1.log");
	remove("./cluster_1.log.index");
}
int main()
{
	test();
	//testRollback();
}
