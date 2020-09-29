#include "cluster/clusterLog.h"
#include "util/fileList.h"
using namespace CLUSTER;
void clear(const std::string logDir)
{
	std::vector<std::string> files;
	fileList::getFileList(logDir, files);
	for (std::vector<std::string>::iterator iter = files.begin(); iter != files.end(); iter++)
		remove((logDir + "/" + *iter).c_str());
}
dsStatus& wFunc(clusterLog* log)
{
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
		dsTest(log->append(logEntry));
	}
	dsOk();
}
dsStatus& rFunc(clusterLog* logFile)
{
	clusterLog::iterator iter(logFile, {1,1});
	dsTest(iter.seek(logIndexInfo(1, 1)));
	const logEntryRpcBase* entry;
	for (int i = 1; i < 100000; i++)
	{
		dsStatus & rtv = iter.next(entry);
		if(!dsCheck(rtv))
			printf("xxx\n");
		dsTest(rtv);
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
	dsOk();
}
dsStatus& test()
{
	clusterLogConfig logManagerConfig;
	logConfig logConfig;
	logConfig.update("defaultLogFileSize", "1M", false);
	clear(logManagerConfig.getLogDir());
	clusterLog log(nullptr, logManagerConfig, logConfig);
	dsReturnIfFailed(log.init({ 0,0 }));
	std::thread w(wFunc, &log);
	w.join();
	std::thread r0(rFunc, &log);
	r0.join();
	dsOk();
}
int main()
{
	dsCheck(test());
}
