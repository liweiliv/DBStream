/*
 * BinlogRead.h
 *
 *  Created on: 2018年5月16日
 *      Author: liwei
 */

#ifndef BINLOG2FILE_BINLOGREAD_H_
#define BINLOG2FILE_BINLOGREAD_H_
#include <string.h>
#include <string>
#include <queue>
#include <vector>
#include <unistd.h>
#include <stdint.h>
#include "block_file_manager.h"
#include "MySqlBinlogReaderMod.h"
using namespace std;
struct st_mysql;

class ReadBinlogFile;
class formatEvent;
struct BinlogEvent;
struct MySQLLogWrapper;
struct _memp_ring;
class BinlogRead : public MySqlBinlogReader
{
private:
	st_mysql * m_conn;
	uint32_t m_serverId;
	uint32_t m_remoteServerID;
	std::queue<long> m_connFrequency;
	bool m_readLocalBinlog;
	block_file_manager * m_localBinlogFiles;
	block_file_manager::iterator * m_localBinlogItor;
	ReadBinlogFile * m_localFile;
	std::string m_localBinlogIndex;
	std::string m_localBinlogPath;
	string m_serverVersion;

	uint32_t  m_currFileID;

	int connectDB(uint32_t retryTimes = 5,bool ctrlFreq = false);
	int setConnectToGetBinlog(const char * file, uint64_t pos);
	int initFmtDescEvent();
	int checkMasterVesion();
	int connectAndDumpBinlog();
	int dumpLocalBinlog();
	int readLocalBinlog(BinlogEvent *logEvent);
	int readRemoteBinlog(BinlogEvent *logEvent);
	struct binlogFileInfo
	{
		std::string file;
		uint64_t size;
		binlogFileInfo(const char *_file,uint64_t _size):file(_file),size(_size){}
		binlogFileInfo(const binlogFileInfo &info):file(info.file),size(info.size){}
	};
	int seekBinlogInRemote(uint32_t fileID,uint64_t position);
	int seekBinlogInLocal(uint32_t fileID,uint64_t position);
	int getFirstLogeventTimestamp(const char * file,uint64_t &timestamp,bool localORRemote = false);
	int seekBinlogFileInRemote(uint64_t timestamp,bool strick = false);
	int seekBinlogFileInLocal(uint64_t timestamp,bool strick = false);
	int seekBinlogInFile(uint64_t timestamp,const char * fileName,bool localORRemmote = false,bool strick = false);
	int dumpBinlog(const char * file,uint64_t offset,bool localORRemote = false);
	static int showBinaryLogs(st_mysql * conn, std::vector<binlogFileInfo> & binaryLogs);
public:
	BinlogRead(struct _memp_ring * mp,const std::map<std::string,std::string> & modConfig);
	~BinlogRead();
	static uint32_t genSrvierId(uint32_t seed);

	int seekBinlogByCheckpoint(uint32_t fileID,uint64_t position);
	int seekBinlogByTimestamp(uint64_t timestamp,bool strick = true);
	int seekBinlogFile(uint64_t timestamp,bool strick = true);
	int startDump();
	int init();
	static bool getVariables(st_mysql *mysqld,const char * variableName,std::string &v);
	int initRemoteServerID(uint32_t serverID);
	formatEvent * getFmtDescEvent();
	int readBinlog(MySQLLogWrapper *wrapper);

};





#endif /* BINLOG2FILE_BINLOGREAD_H_ */
