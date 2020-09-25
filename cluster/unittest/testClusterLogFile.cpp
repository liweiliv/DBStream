#include "cluster/clusterLogFile.h"
using namespace CLUSTER;
dsStatus& wFunc(clusterLogFile *logFile)
{
	logFile->use();
	for (int i = 1; i < 100000; i++)
	{
		char buf[sizeof(nodeInfoLogEntry) + 256];
		nodeInfoLogEntry* logEntry = (nodeInfoLogEntry*)buf;
		sprintf(logEntry->host, "%d.%d.%d.%d", i, i, i, i);
		logEntry->size = sizeof(logEntry) + strlen(logEntry->host);
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
	logFile->unUse();
	dsOk();
}
dsStatus& rFunc(clusterLogFile* logFile)
{
	clusterLogFile::iterator iter;
	dsTest(iter.setLogFile(logFile));
	dsTest(iter.search(logIndexInfo(1, 1)));
	const logEntryRpcBase* logEntry;
	for (int i = 1; i < 100000; i++)
	{
		dsTest(iter.next(logEntry));
	}
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
	sleep(1);
	std::thread r(rFunc, &logFile);
	w.join();
	r.join();
}
int main()
{
	test();
}
