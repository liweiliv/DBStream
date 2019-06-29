#pragma once
#include <stdint.h>
#include <string>
#include "mysqlConnector.h"
#include "..//..//glog/logging.h"
class ringBuffer;
class fileList;
namespace DATA_SOURCE
{
	class formatEvent;
	class BinlogFile;
	class mysqlBinlogReader {
	public:
		enum READ_CODE
		{
			READ_OK,
			READ_TIMEOUT,
			READ_FAILED,
			READ_FAILED_NEED_RETRY,
			READ_END_OF_LOCAL_FILE,
			READ_FILE_NOT_EXIST
		};
	private:
		mysqlConnector* m_mysqlConnector;
		MYSQL * m_conn;
		uint32_t m_serverId;
		uint32_t m_remoteServerID;

		bool m_readLocalBinlog;
		std::string m_localBinlogPath;
		std::string m_localBinlogPrefix;
		fileList* m_localBinlogList;
		ReadBinlogFile* m_localFile;

		std::string m_serverVersion;
		formatEvent* m_descriptionEvent;
		uint32_t  m_currFileID;
		uint64_t m_currentPos;
		std::string m_currFile;
		ringBuffer* m_pool;

		bool m_isTypeNeedParse[ENUM_END_EVENT];
		int initFmtDescEvent();
		int checkMasterVesion();
		int connectAndDumpBinlog(const char* fileName, uint64_t offset);
		int dumpLocalBinlog(const char* fileName, uint64_t offset);
		int readLocalBinlog(const char *& binlogEvent,size_t &size);
		int readRemoteBinlog(const char * &binlogEvent,size_t &size);
		int readBinlogWrap(const char*& logEvent, size_t& size);
		int seekBinlogInRemote(uint32_t fileID, uint64_t position);
		int seekBinlogInLocal(uint32_t fileID, uint64_t position);
		int getBinlogFileList(std::map<uint64_t, fileInfo>& files, bool localORRemote);
		int getFirstLogeventTimestamp(const char* file, uint64_t& timestamp, bool localORRemote = false);
		int seekBinlogFile(const std::map<uint64_t, fileInfo>& binaryLogs, uint64_t timestamp, bool strick, bool localORRemmote);
		int seekBinlogInFile(uint64_t timestamp, const char* fileName, bool localORRemmote = false, bool strick = false);
		int dumpBinlog(const char* file, uint64_t offset, bool localORRemote = false);
	public:
		mysqlBinlogReader(ringBuffer *pool);
		~mysqlBinlogReader();
		int seekBinlogByCheckpoint(uint32_t fileID, uint64_t position);
		int seekBinlogByTimestamp(uint64_t timestamp, bool strick = true);
		int seekBinlogFile(uint64_t timestamp, bool strick = true);
		int startDump();
		int init();
		int initRemoteServerID(uint32_t serverID);
		formatEvent* getFmtDescEvent();
		int readBinlog(const char*& data);
	};
}
