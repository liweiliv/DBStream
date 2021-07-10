#include "cluster/clusterLog.h"
#include "util/fileList.h"
using namespace CLUSTER;
void clear(const std::string logDir)
{
	std::vector<String> files;
	fileList::getFileList(logDir, files);
	for (auto &iter: files)
		remove((logDir + "/" + iter).c_str());
}
DS wFunc(clusterLog* log)
{
	for (int i = 1; i < 100000; i++)
	{
		char buf[sizeof(nodeInfoLogEntry) + 256];
		nodeInfoLogEntry* LogEntry = (nodeInfoLogEntry*)buf;
		sprintf(LogEntry->host, "%d.%d.%d.%d", i, i, i, i);
		LogEntry->size = sizeof(nodeInfoLogEntry) + strlen(LogEntry->host);
		LogEntry->version = VERSION;
		LogEntry->recordType = static_cast<uint8_t>(rpcType::nodeInfoLogEntry);
		LogEntry->prevRecordLogIndex.logIndex = i - 1;
		LogEntry->prevRecordLogIndex.term = 1;
		LogEntry->logIndex.logIndex = i;
		LogEntry->logIndex.term = 1;
		LogEntry->nodeId = i;
		LogEntry->port = i % 65535;
		LogEntry->opt = 1;
		dsTest(log->append(LogEntry));
	}
	dsOk();
}
DS rFunc(clusterLog* logFile)
{
	clusterLog::iterator iter(logFile, {1,1});
	dsTest(iter.seek(logIndexInfo(1, 1)));
	const logEntryRpcBase* entry;
	for (int i = 1; i < 100000; i++)
	{
		dsTest(iter.next(entry));
		const nodeInfoLogEntry* LogEntry = static_cast<const nodeInfoLogEntry*>(entry);
		assert(LogEntry->version == VERSION);
		assert(LogEntry->recordType == static_cast<uint8_t>(rpcType::nodeInfoLogEntry));
		assert(LogEntry->prevRecordLogIndex.logIndex == (uint64_t)(i - 1));
		assert(LogEntry->prevRecordLogIndex.term == 1L);
		assert(LogEntry->logIndex.logIndex == (uint64_t)i);
		assert(LogEntry->logIndex.term == 1L);
		assert(LogEntry->nodeId == (uint64_t)i);
		assert(LogEntry->port == i % 65535);
		assert(LogEntry->opt == 1);
	}
	dsOk();
}
DS test()
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
	std::thread r1(rFunc, &log);
	std::thread r2(rFunc, &log);
	std::thread r3(rFunc, &log);
	r0.join();
	r1.join();
	r2.join();
	r3.join();
	dsOk();
}
int main()
{
	dsCheck(test());
}
