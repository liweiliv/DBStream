/*
 * mysqlBinlogReader.cpp
 *
 *  Created on: 2018年5月9日
 *      Author: liwei
 */
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <thread>
#include <map>
#include "mysqlBinlogReader.h"
#include "BinaryLogEvent.h"
#include "../../memory/ringBuffer.h"
#include "../../util/fileList.h"
#include "..//..//util/winString.h"
#include "BinlogFile.h"


namespace DATA_SOURCE {
	struct binlogFileInfo
	{
		std::string file;
		uint64_t size;
		binlogFileInfo(const char* _file, uint64_t _size) :file(_file), size(_size) {}
		binlogFileInfo(const binlogFileInfo& info) :file(info.file), size(info.size) {}
	};

	mysqlBinlogReader::mysqlBinlogReader(ringBuffer* pool,mysqlConnector * connector) :m_pool(pool),m_mysqlConnector(connector)
	{
		m_conn = NULL;
		m_serverId = mysqlConnector::genSrvierId(time(nullptr));
		m_remoteServerID = 0;
		m_readLocalBinlog = false;
		m_localBinlogList = NULL;
		m_currFileID = 0;

		memset(m_isTypeNeedParse, 0, sizeof(m_isTypeNeedParse));
		m_isTypeNeedParse[WRITE_ROWS_EVENT_V1] = true;
		m_isTypeNeedParse[WRITE_ROWS_EVENT] = true;
		m_isTypeNeedParse[UPDATE_ROWS_EVENT_V1] = true;
		m_isTypeNeedParse[UPDATE_ROWS_EVENT] = true;
		m_isTypeNeedParse[DELETE_ROWS_EVENT_V1] = true;
		m_isTypeNeedParse[DELETE_ROWS_EVENT] = true;
		m_isTypeNeedParse[ROWS_QUERY_LOG_EVENT] = true;
		m_isTypeNeedParse[QUERY_EVENT] = true;
		m_isTypeNeedParse[TABLE_MAP_EVENT] = true;
		m_isTypeNeedParse[FORMAT_DESCRIPTION_EVENT] = true;
		m_isTypeNeedParse[ROTATE_EVENT] = true;
	}
	mysqlBinlogReader::~mysqlBinlogReader()
	{
		if (m_conn)
			mysql_close(m_conn);
		if (m_localBinlogList)
			delete m_localBinlogList;
		if (m_descriptionEvent)
			delete m_descriptionEvent;
	}
	int mysqlBinlogReader::init()
	{
		return 0; //nothing to do
	}

	int mysqlBinlogReader::initRemoteServerID(uint32_t serverID)
	{
		if (m_remoteServerID == 0)
			m_remoteServerID = serverID;
		else if (m_remoteServerID != serverID)
		{
			LOG(ERROR) << "initRemoteServerID failed,serverid changes from " << m_remoteServerID << " to " << serverID;
			return -1;
		}
		return 0;
	}
	int mysqlBinlogReader::initFmtDescEvent()
	{
		if (m_descriptionEvent != NULL)
			delete m_descriptionEvent;
		assert(!m_serverVersion.empty());
		m_descriptionEvent = new formatEvent(4, m_serverVersion.c_str());
		if (!m_descriptionEvent)
		{
			LOG(ERROR) << "Failed creating Format_description_log_event; out of memory?";
			return -1;
		}
		return 0;
	}
	int mysqlBinlogReader::checkMasterVesion()
	{
		uint64_t srvVersion = mysql_get_server_version(m_conn);
		int majorVersion = srvVersion / 10000;
		char serverVersion[64] =
		{ 0 };
		sprintf(serverVersion, "%d.%ld.%ld", majorVersion,
			(srvVersion % 10000) / 100, srvVersion % 100);
		if (majorVersion < 5)
		{
			LOG(ERROR) << "mysql version " << serverVersion << " is lower than 5,not support";
			return -1;
		}
		m_serverVersion = serverVersion;
		return 0;
	}
	int mysqlBinlogReader::connectAndDumpBinlog(const char* fileName, uint64_t offset)
	{
		if (m_conn)
		{
			mysql_close(m_conn);
			m_conn = NULL;
		}
		if (nullptr == (m_conn = m_mysqlConnector->getConnect()))
		{
			LOG(ERROR) << "connect to remote mysql server failed ,dump binlog failed";
			return -1;
		}
		if (0 != checkMasterVesion())
		{
			LOG(ERROR) << "check server version failed ,dump binlog failed";
			return -1;
		}
		if (0 != initFmtDescEvent())
		{
			LOG(ERROR) << "create format desc log event failed";
			return -1;
		}
		LOG(INFO) << "start dump binlog from " << offset << "@" << fileName;

		if (0 != mysqlConnector::startDumpBinlog(m_conn, fileName, offset, m_serverId))
		{
			mysql_close(m_conn);
			m_conn = NULL;
			LOG(ERROR) << "dump binlog failed";
		}
		m_readLocalBinlog = false;
		m_currentPos = offset;
		m_currFile = fileName;
		m_currFileID = getFileId(fileName);
		LOG(ERROR) << "dump binlog success";
		return 0;
	}
	int mysqlBinlogReader::dumpLocalBinlog(const char* fileName, uint64_t offset)
	{
		if (0 != initFmtDescEvent())
		{
			LOG(ERROR) << "create format desc log event failed";
			return -1;
		}
		if (m_localBinlogList == nullptr)
		{
			m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
			if (m_localBinlogList->load() != 0)
			{
				LOG(ERROR) << "load local binlog file list failed";
				return -1;
			}
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(getFileId(fileName));
		if (iter == m_localBinlogList->get().end())
		{
			LOG(ERROR) << "start read binlog " << fileName << " failed for file not exist";
			return READ_CODE::READ_FAILED;
		}
		if (m_localFile != NULL)
			delete m_localFile;
		m_localFile =
			new ReadBinlogFile(
				m_localBinlogPath.empty() ?
				iter->second.fileName.c_str() :
				(std::string(m_localBinlogPath) + "/"
					+ iter->second.fileName).c_str());
		if (m_localFile->setPosition(offset) != 0
			|| m_localFile->getErr() != 0)
		{
			LOG(ERROR) << "start read binlog from " << iter->second.fileName << " failed for open file failed";
			return READ_CODE::READ_FAILED;
		}
		m_readLocalBinlog = true;
		m_currentPos = offset;
		m_currFile = fileName;
		m_currFileID = getFileId(fileName);
		return 0;
	}
	int mysqlBinlogReader::readLocalBinlog(const char*& logEvent, size_t& size)
	{
		int ret = m_localFile->readBinlogEvent(logEvent, size);
		if (likely(ret == 0))
			return READ_CODE::READ_OK;

		if (m_localFile->getErr() == BINLOG_READ_STATUS::BINLOG_READ_END)
		{
			std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(m_currFileID + 1);
			if (iter == m_localBinlogList->get().end())//end of local binlog ,read from remote
			{
				LOG(ERROR) << "read the end of loacl binlog file " << m_currFile;
				return READ_END_OF_LOCAL_FILE;
			}
			else
			{
				const char* nextBinlog = NULL;
				uint64_t nextBinlogStartPos;
				if (m_localFile->getNextBinlogFileInfo(nextBinlog,
					nextBinlogStartPos) != 0 || nextBinlog == NULL)
				{
					LOG(ERROR) << "read the end of binlog " << m_localFile->getFile() << " ,but can not get the next filename";
					return READ_CODE::READ_FAILED;
				}
				if (strcmp(iter->second.fileName.c_str(), nextBinlog) != 0)
				{
					LOG(ERROR) << "read the end of binlog " << m_localFile->getFile() << " ,but the next filename in index [" << iter->second.fileName << "] is not [" << nextBinlog << "]";
					return READ_CODE::READ_FAILED;
				}
				if (0 != m_localFile->setFile(nextBinlog, nextBinlogStartPos))
				{
					LOG(ERROR) << "start read log event from next binlog " << nextBinlog << " in position " << nextBinlogStartPos << " failed ";
					return READ_CODE::READ_FAILED;
				}
				return readLocalBinlog(logEvent, size);
			}
		}
		else
		{
			LOG(ERROR) << "read log event from local binlog file " << m_localFile->getFile() << " failed ,err code " << m_localFile->getErr();
			return READ_CODE::READ_FAILED;
		}
	}
	int mysqlBinlogReader::readRemoteBinlog(const char*& logEvent, size_t& size)
	{
		uint64_t readLength = cli_safe_read(m_conn, NULL);
		if (packet_error == readLength)
		{
			int connErrno = mysql_errno(m_conn);
			LOG(ERROR) << "error in read binlog: " << connErrno << "," << mysql_error(m_conn);
			if (connErrno == CR_SERVER_LOST || connErrno == CR_SERVER_GONE_ERROR
				|| connErrno == CR_CONN_HOST_ERROR)
				return READ_FAILED_NEED_RETRY;
			else
				return READ_FAILED;
		}
		NET* net = &m_conn->net;
		if (readLength < 8 && net->read_pos[0] == 254)
		{
			LOG(ERROR) << "read to end of binlog " << m_currFile;
			return READ_FAILED_NEED_RETRY;
		}
		logEvent = (const char*)net->read_pos + 1;
		size = readLength - 1;
		return READ_OK;
	}
	int mysqlBinlogReader::readBinlogWrap(const char*& logEvent, size_t& size)
	{
		int ret = 0;
		if (!m_readLocalBinlog)
		{
		READ:		if (READ_OK != (ret = readRemoteBinlog(logEvent, size)))
		{
			int retryTimes = 32;
			if (READ_FAILED_NEED_RETRY == ret && --retryTimes > 0)
			{
				while (--retryTimes > 0 && (ret = connectAndDumpBinlog(m_currFile.c_str(), m_currentPos) != 0))
					std::this_thread::sleep_for(std::chrono::seconds(1));
				if (ret != 0)
				{
					LOG(ERROR) << "dump binlog failed";
					return READ_FAILED;
				}
				else
					goto READ;
			}
			else
			{
				return ret;
			}
		}
		}
		else
		{
			ret = readLocalBinlog(logEvent, size);
			if (ret != READ_OK)
			{
				if (ret == READ_END_OF_LOCAL_FILE)
				{
					const char* nextBinlog = NULL;
					uint64_t nextBinlogStartPos;
					if (m_localFile->getNextBinlogFileInfo(nextBinlog,
						nextBinlogStartPos) != 0 || nextBinlog == NULL)
					{
						LOG(ERROR) << "read the end of binlog " << m_localFile->getFile() << " ,but can not get the next filename";
						return READ_CODE::READ_FAILED;
					}
					if (connectAndDumpBinlog(nextBinlog, nextBinlogStartPos) != 0)
					{
						LOG(ERROR) << "read the end of binlog " << m_localFile->getFile() << " ,but read from  next filename in remote mysql server  failed";
						return READ_CODE::READ_FAILED;
					}
					m_readLocalBinlog = false;
					return readRemoteBinlog(logEvent, size);
				}
				else
					return ret;
			}
		}
	}
	int mysqlBinlogReader::readBinlog(const char*& data)
	{
		const char* binlogEvent;
		size_t size;
		int ret = readRemoteBinlog(binlogEvent, size);
		if (READ_OK != ret)
			return ret;

		const commonMysqlBinlogEventHeader_v4* header =
			(const commonMysqlBinlogEventHeader_v4*)binlogEvent;
		if (header->type == Log_event_type::FORMAT_DESCRIPTION_EVENT)
		{
			formatEvent* tmp = new formatEvent(binlogEvent, size);
			if (tmp == NULL)
			{
				LOG(ERROR) << "seek Binlog in remote mysql server failed for Format_description_log_event of " << m_currFile << " parse failed";
				return READ_CODE::READ_FAILED;
			}
			if (m_descriptionEvent != NULL)
				delete m_descriptionEvent;
			m_descriptionEvent = tmp;
		}
		else if ((header->type == Log_event_type::ROTATE_EVENT
			&& header->timestamp == 0)) //end of file
		{
			const char* error_msg;
			RotateEvent* tmp = new RotateEvent(binlogEvent, size,
				m_descriptionEvent);
			if (m_currFile.compare(tmp->fileName) != 0)
				m_currentPos = 4;
			m_currFile = tmp->fileName;
			m_currFileID = getFileId(tmp->fileName);
			delete tmp;
		}

		if (header->eventOffset < m_currentPos)
		{
			if (header->type != FORMAT_DESCRIPTION_EVENT
				&& header->type != ROTATE_EVENT)
				m_currentPos += header->eventSize;
		}
		else
			m_currentPos = header->eventOffset;
		if (!m_isTypeNeedParse[header->type])
			size = sizeof(commonMysqlBinlogEventHeader_v4) < size ? sizeof(commonMysqlBinlogEventHeader_v4) : size;
		char* wrapper = (char*)m_pool->alloc(size + sizeof(size));
		*(uint64_t*)wrapper = size;
		memcpy(wrapper + sizeof(size), binlogEvent, size);
		return READ_OK;
	}
	static int showBinaryLogs(MYSQL* conn,
		std::map<uint64_t, fileInfo>& binaryLogs)
	{
		MYSQL_RES* res = NULL;
		MYSQL_ROW row;
		uint32_t fields;
		if (mysql_query(conn, "show binary logs")
			|| !(res = mysql_store_result(conn)))
		{
			LOG(ERROR) << "error show binary logs: " << mysql_errno(conn) << "," << mysql_error(conn);
			return -1;
		}
		while (NULL != (row = mysql_fetch_row(res)))
		{
			if (2 == (fields = mysql_num_fields(res)) && row[0] != nullptr && atol(row[1]) != 0)//ignore empty binlog file
			{
				fileInfo file;
				file.fileName = row[0];
				file.size = atol(row[1]);
				file.timestamp = 0;
				binaryLogs.insert(std::pair<uint64_t, fileInfo>(getFileId(file.fileName.c_str()), file));
			}
		}
		mysql_free_result(res);
		return 0;
	}

	int mysqlBinlogReader::seekBinlogInRemote(uint32_t fileID, uint64_t position)
	{
		std::map<uint64_t, fileInfo> binaryLogs;
		if (m_conn != nullptr)
		{
			mysql_close(m_conn);
			m_conn = nullptr;
		}
		if (nullptr == (m_conn = m_mysqlConnector->getConnect()))
		{
			LOG(ERROR) << "seek Binlog in remote mysql server failed for connect db failed";
			return -1;
		}
		if (showBinaryLogs(m_conn, binaryLogs) != 0)
		{
			LOG(ERROR) << "seek Binlog in remote mysql server failed for showBinaryLogs fail";
			return -1;
		}
		if (binaryLogs.size() == 0)
		{
			LOG(ERROR) << "seek Binlog in remote mysql server failed for binlog list is empty";
			return -1;
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = binaryLogs.find(fileID);
		if (iter == binaryLogs.end())
		{
			LOG(ERROR) << "file " << fileID << " is not exists in local binlog files";
			return -1;
		}
		if (iter->second.size < position)
		{
			LOG(ERROR) << "seek Binlog[" << position << "@" << fileID << "] in local binlog file list failed for binlog size:" << iter->second.size << " is less than position";
			return -1;
		}
		return connectAndDumpBinlog(iter->second.fileName.c_str(), position);
	}
	int mysqlBinlogReader::seekBinlogInLocal(uint32_t fileID, uint64_t position)
	{
		if (m_localBinlogList == nullptr)
		{
			m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
			if (m_localBinlogList->load() != 0)
			{
				LOG(ERROR) << "load local binlog file list failed";
				return -1;
			}
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(fileID);
		if (iter == m_localBinlogList->get().end())
		{
			LOG(ERROR) << "file " << fileID << " is not exists in local binlog files";
			return -1;
		}
		if (iter->second.size < position)
		{
			LOG(ERROR) << "seek Binlog[" << position << "@" << fileID << "] in local binlog file list failed for binlog size:" << iter->second.size << " is less than position";
			return -1;
		}
		return dumpLocalBinlog(iter->second.fileName.c_str(), position);
	}
	int mysqlBinlogReader::seekBinlogByCheckpoint(uint32_t fileID, uint64_t position)
	{
		if (seekBinlogInRemote(fileID, position) != 0)
		{
			if (!m_localBinlogPath.empty())
			{
				if (seekBinlogInLocal(fileID, position) != 0)
				{
					LOG(ERROR) << "seek binlog in remote mysql server failed ,and seek binlog in local binlog files is also failed";
					return READ_CODE::READ_FAILED;
				}
				return READ_CODE::READ_OK;
			}
			else
			{
				LOG(ERROR) << "seek binlog in remote mysql server failed";
				return READ_CODE::READ_OK;
			}
		}
		else
			return READ_CODE::READ_OK;
	}

	int mysqlBinlogReader::dumpBinlog(const char* file, uint64_t offset,
		bool localORRemote)
	{
		if (localORRemote)
		{
			if (0 != dumpLocalBinlog(file, offset))
			{
				LOG(ERROR) << "start read binlog from " << offset << "@" << file << " failed";
				return READ_CODE::READ_FAILED;
			}
		}
		else
		{
			if (0 != connectAndDumpBinlog(file, offset))
			{
				LOG(ERROR) << "start read binlog from " << offset << "@" << file << " failed";
				return READ_CODE::READ_FAILED;
			}
		}
		return READ_CODE::READ_OK;
	}
	int mysqlBinlogReader::getFirstLogeventTimestamp(const char* file,
		uint64_t& timestamp, bool localORRemote)
	{
		if (0 != dumpBinlog(file, 4, localORRemote))
		{
			LOG(ERROR) << "dumpBinlog " << file << " failed";
			return READ_CODE::READ_FAILED;
		}
		uint32_t fmtEventTimestamp = 0;
		while (true)
		{
			const char* logEvent;
			size_t size;
			if (localORRemote ?
				0 != readLocalBinlog(logEvent, size) :
				0 != readRemoteBinlog(logEvent, size))
			{
				LOG(ERROR) << "getFirstLogeventTimestamp of file " << file << " failed";
				return READ_CODE::READ_FAILED;
			}
			commonMysqlBinlogEventHeader_v4* header =
				(commonMysqlBinlogEventHeader_v4*)logEvent;
			if (header->type == FORMAT_DESCRIPTION_EVENT)
			{
				fmtEventTimestamp = header->timestamp;
				const char* error_msg;
				formatEvent* tmp = new formatEvent(logEvent, size);
				if (tmp == NULL)
				{
					LOG(ERROR) << "seek Binlog in remote mysql server failed for Format_description_log_event of " << m_currFile << " parse failed";
					return READ_CODE::READ_FAILED;
				}
				if (m_descriptionEvent != NULL)
					delete m_descriptionEvent;
				m_descriptionEvent = tmp;
			}
			else if ((header->type == ROTATE_EVENT && header->timestamp != 0)
				|| header->type == Log_event_type::HEARTBEAT_LOG_EVENT) //end of file
			{
				if (fmtEventTimestamp == 0)
				{
					LOG(ERROR) << "getFirstLogeventTimestamp of file " << file << " failed for no valid log event in binlog file";
					return READ_CODE::READ_FAILED;
				}
				else
				{
					LOG(ERROR) << "reach the end of " << file << ",but there is no ddl or dml log event ,use timestamp " << fmtEventTimestamp << " of  FORMAT_DESCRIPTION_EVENT as begin timestamp ";
					timestamp = fmtEventTimestamp;
					return 0;
				}
			}
			else if (header->type == QUERY_EVENT)
			{
				const char* query = NULL;
				uint32_t querySize = 0;
				if (0
					!= QueryEvent::getQuery(logEvent, size,
						m_descriptionEvent, query, querySize))
				{
					LOG(ERROR) << "ILLEGAL query log event in " << header->eventOffset << "@" << file;
					return READ_CODE::READ_FAILED;
				}
				if (query != NULL && strncasecmp(query, "BEGIN", 5) != 0) //not begin
				{
					timestamp = header->timestamp;
					return 0;
				}
			}
			else if (header->type == Log_event_type::XID_EVENT) //commit
			{
				timestamp = header->timestamp;
				return 0;
			}
		}
	}
	int mysqlBinlogReader::getBinlogFileList(std::map<uint64_t, fileInfo>& files, bool localORRemote)
	{
		if (localORRemote)
		{
			if (m_localBinlogList == nullptr)
			{
				m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
				if (m_localBinlogList->load() != 0)
				{
					LOG(ERROR) << "load local binlog file list failed";
					return -1;
				}
			}
			files = m_localBinlogList->get();
		}
		else
		{
			std::map<uint64_t, fileInfo> _files;
			if (m_conn != nullptr)
			{
				mysql_close(m_conn);
				m_conn = nullptr;
			}
			if (nullptr == (m_conn = m_mysqlConnector->getConnect()))
			{
				LOG(ERROR) << "seek Binlog in remote mysql server failed for connect db failed";
				return READ_CODE::READ_FAILED;
			}
			if (showBinaryLogs(m_conn, files) != 0)
			{
				LOG(ERROR) << "seek Binlog in remote mysql server failed for showBinaryLogs fail";
				return READ_CODE::READ_FAILED;
			}
			files = _files;
		}
		return READ_CODE::READ_OK;
	}
	int mysqlBinlogReader::seekBinlogFile(const std::map<uint64_t, fileInfo>& binaryLogs, uint64_t timestamp, bool strick, bool localORRemmote)
	{
		int64_t s = binaryLogs.begin()->first, e = binaryLogs.rbegin()->first, idx = 0;
		uint64_t timestampOfFile = 0;
		std::map<uint64_t, fileInfo>::const_iterator iter;
		while (e >= s)
		{
			idx = (s + e) / 2;
			iter = binaryLogs.find(idx);
			if (binaryLogs.end() == iter)
			{
				LOG(ERROR) << "seek Binlog in " << (localORRemmote ? "local binlog list " : "remote mysql server ") << "by timestamp:" << timestamp << " failed for binlog is not increase strictly,log :" << idx << "not exist";
				return READ_CODE::READ_FILE_NOT_EXIST;
			}
			if (0
				!= getFirstLogeventTimestamp(iter->second.fileName.c_str(), timestampOfFile, localORRemmote))
			{
				LOG(ERROR) << "seek Binlog in " << (localORRemmote ? "local binlog list" : "remote mysql server") << "  failed";
				return READ_CODE::READ_FAILED;
			}
			LOG(INFO) << "first log event timestamp of " << iter->second.fileName << " is " << timestampOfFile;
			if (timestamp < timestampOfFile)
				e = idx - 1;
			else
				s = idx + 1;
		}
		if (timestamp <= timestampOfFile)
		{
			if (iter == binaryLogs.begin())
			{
				if (strick)
				{
					LOG(ERROR) << "seek Binlog in remote mysql server failed ,"
						"begin timestamp " << timestampOfFile << " of the first binlog file " << iter->second.fileName << " is newer than " << timestamp;
					return READ_CODE::READ_FILE_NOT_EXIST;
				}
				else
				{
					LOG(INFO) << "seek to the begin of binlog ,but can not find timestamp " << timestamp << ",in loose mode,use first file " << iter->second.fileName << " as the begin pos";

				}
			}
		}
		m_currFile = iter->second.fileName;
		m_currFileID = iter->first;
		m_currentPos = 4;
		LOG(INFO) << "seek binlog file success ,timestamp " << timestamp << " ,file " << m_currFile;
		return READ_CODE::READ_OK;
	}
	int mysqlBinlogReader::seekBinlogFile(uint64_t timestamp, bool strick)
	{
		m_readLocalBinlog = false;
		std::map<uint64_t, fileInfo> binaryLogs;
		int ret = READ_OK;
		if (m_conn != nullptr)
		{
			mysql_close(m_conn);
			m_conn = nullptr;
		}
		if (nullptr == (m_conn = m_mysqlConnector->getConnect()))
		{
			LOG(ERROR) << "seek Binlog in remote mysql server failed for connect db failed";
			return -1;
		}
		if (showBinaryLogs(m_conn, binaryLogs) != 0 || binaryLogs.size() == 0)
		{
			LOG(ERROR) << "seek Binlog in remote mysql server failed for binlog list is empty";
			return READ_CODE::READ_FAILED;
		}
		if ((ret = seekBinlogFile(binaryLogs, timestamp, strick, false)) == READ_CODE::READ_OK)
			return READ_CODE::READ_OK;
		if (!m_localBinlogPath.empty())
		{
			if (m_localBinlogList == nullptr)
			{
				m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
				if (m_localBinlogList->load() != 0)
				{
					LOG(ERROR) << "load local binlog file list failed";
					return -1;
				}
			}
			if ((ret = seekBinlogFile(m_localBinlogList->get(), timestamp, strick, false)) == READ_CODE::READ_OK)
			{
				m_readLocalBinlog = true;
				return READ_CODE::READ_OK;
			}
		}
		return ret;
	}
	int mysqlBinlogReader::seekBinlogInFile(uint64_t timestamp, const char* fileName,
		bool localORRemmote, bool strick)
	{
		if (0 != dumpBinlog(fileName, 4, localORRemmote))
		{
			LOG(ERROR) << "dumpBinlog " << fileName << " failed";
		}
		uint64_t beginOffset = 4;
		uint64_t currentOffset = 4;
		while (true)
		{
			const char* logEvent;
			size_t size;
			if (localORRemmote ?
				0 != readLocalBinlog(logEvent, size) :
				0 != readRemoteBinlog(logEvent, size))
			{
				LOG(ERROR) << "seek Binlog in remote mysql server failed for read file " << m_currFile << " failed";
				return READ_CODE::READ_FAILED;
			}
			const commonMysqlBinlogEventHeader_v4* header =
				(const commonMysqlBinlogEventHeader_v4*)logEvent;
			if (header->type == Log_event_type::FORMAT_DESCRIPTION_EVENT)
			{
				formatEvent* tmp = new formatEvent(logEvent,
					size);
				if (m_descriptionEvent != NULL)
					delete m_descriptionEvent;
				m_descriptionEvent = tmp;
			}
			else if ((header->type == Log_event_type::ROTATE_EVENT
				&& header->timestamp > 0)) //end of file
			{
				RotateEvent* event = new RotateEvent(logEvent, size, m_descriptionEvent);
				m_currFile = event->fileName;
				m_currentPos = 4;
				delete event;
				return READ_OK;
			}
			else if (header->type == Log_event_type::HEARTBEAT_LOG_EVENT)
			{
				if (strick)
				{
					LOG(ERROR) << "reached end of binlog ,bu t can not find " << timestamp;
					return READ_CODE::READ_FAILED;
				}
				else
				{
					LOG(ERROR) << "seek to the endof binlog ,but can not find timestamp " << timestamp << ",in loose mode,use current pos " << m_currentPos << "@" << m_currFile << " as the begin pos";
					return READ_OK;
				}
			}

			if (header->timestamp >= timestamp)
			{
				m_currentPos = beginOffset;
				return 0;
			}

			if (header->type == Log_event_type::QUERY_EVENT)
			{
				const char* query = NULL;
				uint32_t querySize = 0;
				if (0
					!= QueryEvent::getQuery(logEvent, size, m_descriptionEvent, query, querySize))
				{
					LOG(ERROR) << "ILLEGAL query log event in " << header->eventOffset << "@" << m_currFile;
					return READ_CODE::READ_FAILED;
				}
				if (query != NULL && strncasecmp(query, "BEGIN", 5) == 0) // begin
				{
					beginOffset = currentOffset;
				}
			}

			if (header->eventOffset < currentOffset)
			{
				if (header->type != FORMAT_DESCRIPTION_EVENT
					&& header->type != ROTATE_EVENT)
					currentOffset += header->eventSize;
			}
			else
				currentOffset = header->eventOffset;
		}
	}
	int mysqlBinlogReader::seekBinlogByTimestamp(uint64_t timestamp, bool strick)
	{
		int ret = READ_OK;
		if ((ret = seekBinlogFile(timestamp, strick)) != READ_OK)
			return ret;
		if ((ret = seekBinlogInFile(timestamp, m_currFile.c_str(), m_readLocalBinlog, strick)) == READ_OK)
			return READ_OK;
		return ret;
	}
	int mysqlBinlogReader::startDump()
	{
		return dumpBinlog(m_currFile.c_str(), m_currentPos, m_readLocalBinlog);
	}
	formatEvent* mysqlBinlogReader::getFmtDescEvent()
	{
		return m_descriptionEvent;
	}
}
