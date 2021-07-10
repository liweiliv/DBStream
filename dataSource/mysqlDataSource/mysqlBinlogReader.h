#pragma once
#include <stdint.h>
#include <string>
#include "mysqlConnector.h"
#include "glog/logging.h"
#include "util/file.h"
#include "util/status.h"
#include "util/String.h"
#include "BinaryLogEvent.h"
#include "../dataSourceReader.h"
class ringBuffer;
class fileList;
class ReadBinlogFile;
namespace DATA_SOURCE
{
	class formatEvent;
	class BinlogFile;
	class ReadBinlogFile;
	class mysqlBinlogReader :public DataSourceReader {
	public:
		enum READ_CODE
		{
			READ_OK,
			READ_TIMEOUT,
			READ_FAILED,
			READ_FAILED_NEED_RETRY,
			READ_END_OF_LOCAL_FILE,
			READ_FILE_NOT_EXIST,
			LOCAL_FILE_DAMAGED
		};
	private:
		mysqlConnector* m_mysqlConnector;
		MYSQL* m_conn;
		uint32_t m_serverId;
		uint32_t m_remoteServerID;

		bool m_readLocalBinlog;
		std::string m_localBinlogPath;
		std::string m_localBinlogPrefix;
		fileList* m_localBinlogList;
		ReadBinlogFile* m_localFile;

		String m_serverVersion;
		formatEvent* m_descriptionEvent;
		uint32_t  m_currFileID;
		uint64_t m_currentPos;
		std::string m_currFile;
		int m_currentFileRotateCount;

		bool m_isTypeNeedParse[ENUM_END_EVENT];
		DS initFmtDescEvent();
		DS checkMasterVesion();
		DS connectAndDumpBinlog(const char* fileName, uint64_t offset);
		DS dumpLocalBinlog(const char* fileName, uint64_t offset);
		DS readLocalBinlog(const char*& binlogEvent, size_t& size);
		DS readRemoteBinlog(const char*& binlogEvent, size_t& size);
		DS readBinlogWrap(const char*& logEvent, size_t& size);
		DS seekBinlogInRemote(uint32_t fileID, uint64_t position);
		DS seekBinlogInLocal(uint32_t fileID, uint64_t position);
		DS getBinlogFileList(std::map<uint64_t, fileInfo>& files, bool localORRemote);
		DS getFirstLogeventTimestamp(const char* file, uint64_t& timestamp, bool localORRemote = false);
		DS seekBinlogFile(const std::map<uint64_t, fileInfo>& binaryLogs, uint64_t timestamp, bool strick, bool localORRemmote);
		DS seekBinlogInFile(uint64_t timestamp, const char* fileName, bool localORRemmote = false, bool strick = false);
		DS dumpBinlog(const char* file, uint64_t offset, bool localORRemote = false);
		DS initRemoteServerID(uint32_t serverID);
		DS startDumpByCheckpoint(const RPC::Checkpoint*ckp);
		formatEvent* getFmtDescEvent();
		DS startFrom(RPC::Checkpoint* ckp);

	public:
		mysqlBinlogReader(LocalLogFileCache* localLog, Config* conf, mysqlConnector* mysqlConnector);
		~mysqlBinlogReader();
		DS startDump();
		DS seekBinlogByTimestamp(uint64_t timestamp, bool strick = true);
		DS seekBinlogByGtid(const char * gtid);
		DS seekBinlogByNow();
		DS readBinlog(const char*& data, size_t& size);
		DS seekBinlogByCheckpoint(uint32_t fileID, uint64_t position);
		DS seekBinlogFile(uint64_t timestamp, bool strick = true);
		virtual DS init(RPC::Checkpoint* currentCkp, RPC::Checkpoint* safeCkp);
	};
}
