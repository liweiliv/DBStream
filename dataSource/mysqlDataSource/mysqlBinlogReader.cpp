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
#include "memory/ringBuffer.h"
#include "util/fileList.h"
#include "util/winString.h"
#include "BinlogFile.h"


namespace DATA_SOURCE {
	struct binlogFileInfo
	{
		std::string file;
		uint64_t size;
		binlogFileInfo(const char* _file, uint64_t _size) :file(_file), size(_size) {}
		binlogFileInfo(const binlogFileInfo& info) :file(info.file), size(info.size) {}
	};

	mysqlBinlogReader::mysqlBinlogReader(mysqlConnector* connector) :m_mysqlConnector(connector), m_conn(nullptr),m_serverId(0),m_remoteServerID(0),m_localFile(nullptr),m_currentPos(0)
	{
		m_serverId = mysqlConnector::genSrvierId(time(nullptr));
		m_readLocalBinlog = false;
		m_localBinlogList = NULL;
		m_currFileID = 0;
		m_currentFileRotateCount = 0;
		m_descriptionEvent = nullptr;

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

	DS mysqlBinlogReader::init()
	{
		dsOk(); //nothing to do
	}

	DS mysqlBinlogReader::initRemoteServerID(uint32_t serverID)
	{
		if (m_remoteServerID == 0)
			m_remoteServerID = serverID;
		else if (m_remoteServerID != serverID)
		{
			dsFailedAndLogIt(1, "initRemoteServerID failed, serverid changes from " << m_remoteServerID << " to " << serverID,ERROR);
		}
		dsOk();
	}

	DS mysqlBinlogReader::initFmtDescEvent()
	{
		if (m_descriptionEvent != NULL)
			delete m_descriptionEvent;
		assert(!m_serverVersion.empty());
		m_descriptionEvent = new formatEvent(4, m_serverVersion.c_str());
		if (!m_descriptionEvent)
			dsFailedAndLogIt(1, "Failed creating Format_description_log_event; out of memory?", ERROR);
		dsOk();
	}

	DS mysqlBinlogReader::checkMasterVesion()
	{
		uint64_t srvVersion = mysql_get_server_version(m_conn);
		int majorVersion = srvVersion / 10000;
		m_serverVersion.append(majorVersion).append(".").append((srvVersion % 10000) / 100).append(srvVersion % 100);
		if (majorVersion < 5)
			dsFailedAndLogIt(1, "mysql version " << m_serverVersion << " is lower than 5,not support", ERROR);
		dsOk();
	}

	DS mysqlBinlogReader::connectAndDumpBinlog(const char* fileName, uint64_t offset)
	{
		if (m_conn)
		{
			mysql_close(m_conn);
			m_conn = nullptr;
		}
		dsReturnIfFailed(m_mysqlConnector->getConnect(m_conn));
		if (0 != checkMasterVesion())
		{
			dsFailedAndLogIt(1, "check server version failed ,dump binlog failed", ERROR);
		}
		if (0 != initFmtDescEvent())
		{
			dsFailedAndLogIt(1, "create format desc log event failed", ERROR);
		}
		LOG(INFO) << "start dump binlog from " << offset << "@" << fileName;

		if (!dsCheck(mysqlConnector::startDumpBinlog(m_conn, fileName, offset, m_serverId)))
		{
			mysql_close(m_conn);
			m_conn = nullptr;
			dsReturnCode(getLocalStatus().code);
		}
		m_readLocalBinlog = false;
		m_currentPos = offset;
		m_currFile = fileName;
		if(m_currFileID != getFileId(fileName))
		{
			m_currFileID = getFileId(fileName);
			m_currentFileRotateCount = 0;
		}
		LOG(INFO) << "dump binlog success";
		dsOk();
	}

	DS mysqlBinlogReader::dumpLocalBinlog(const char* fileName, uint64_t offset)
	{
		if (0 != initFmtDescEvent())
		{
			dsFailedAndLogIt(1, "create format desc log event failed", ERROR);
		}
		if (m_localBinlogList == nullptr)
		{
			m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
			if (m_localBinlogList->load() != 0)
			{
				dsFailedAndLogIt(1, "load local binlog file list failed", ERROR);
			}
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(getFileId(fileName));
		if (iter == m_localBinlogList->get().end())
			dsFailedAndLogIt(READ_CODE::READ_FAILED, "start read binlog " << fileName << " failed for file not exist", ERROR);

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
			dsFailedAndLogIt(READ_CODE::READ_FAILED, "start read binlog from " << iter->second.fileName << " failed for open file failed", ERROR);
		}
		m_readLocalBinlog = true;
		m_currentPos = offset;
		m_currFile = fileName;
		m_currFileID = getFileId(fileName);
		dsOk();
	}

	DS mysqlBinlogReader::readLocalBinlog(const char*& logEvent, size_t& size)
	{
		DS ret = m_localFile->readBinlogEvent(logEvent, size);
		if (likely(ret == 0))
			dsReturnCode(READ_CODE::READ_OK);

		if (m_localFile->getErr() == BINLOG_READ_STATUS::BINLOG_READ_END)
		{
			std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(m_currFileID + 1);
			if (iter == m_localBinlogList->get().end())//end of local binlog ,read from remote
			{
				dsFailedAndLogIt(READ_CODE::READ_END_OF_LOCAL_FILE, "read the end of loacl binlog file " << m_currFile, ERROR);
			}
			else
			{
				const char* nextBinlog = NULL;
				uint64_t nextBinlogStartPos;
				if (m_localFile->getNextBinlogFileInfo(nextBinlog,
					nextBinlogStartPos) != 0 || nextBinlog == NULL)
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "read the end of binlog " << m_localFile->getFile() << " ,but can not get the next filename", ERROR);
				}
				if (strcmp(iter->second.fileName.c_str(), nextBinlog) != 0)
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "read the end of binlog " << m_localFile->getFile() << " ,but the next filename in index [" << iter->second.fileName << "] is not [" << nextBinlog << "]", ERROR);
				}
				if (0 != m_localFile->setFile(nextBinlog, nextBinlogStartPos))
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "start read log event from next binlog " << nextBinlog << " in position " << nextBinlogStartPos << " failed ", ERROR);
				}
				dsReturn(readLocalBinlog(logEvent, size));
			}
		}
		else
		{
			dsFailedAndLogIt(READ_CODE::READ_FAILED, "read log event from local binlog file " << m_localFile->getFile() << " failed ,err code " << m_localFile->getErr(), ERROR);
		}
	}

	DS mysqlBinlogReader::readRemoteBinlog(const char*& logEvent, size_t& size)
	{
		uint64_t readLength = cli_safe_read(m_conn, NULL);
		if (packet_error == readLength)
		{
			int connErrno = mysql_errno(m_conn);
			LOG(WARNING) << "error in read binlog: " << connErrno << "," << mysql_error(m_conn);
			if (connErrno == CR_SERVER_LOST || connErrno == CR_SERVER_GONE_ERROR
				|| connErrno == CR_CONN_HOST_ERROR)
				dsReturnCode(READ_CODE::READ_FAILED_NEED_RETRY);
			else
				dsFailedAndLogIt(READ_FAILED, "read binlog failed " << connErrno << "," << mysql_error(m_conn), ERROR);
		}
		NET* net = &m_conn->net;
		if (readLength < 8 && net->read_pos[0] == 254)
		{
			LOG(ERROR) << "read to end of binlog " << m_currFile;
			dsReturnCode(READ_FAILED_NEED_RETRY);
		}
		logEvent = (const char*)net->read_pos + 1;
		size = readLength - 1;
		dsOk();
	}

	DS mysqlBinlogReader::readBinlogWrap(const char*& logEvent, size_t& size)
	{
		DS ret = 0;
		if (!m_readLocalBinlog)
		{
		READ:
			if (READ_OK != (ret = readRemoteBinlog(logEvent, size)))
			{
				int retryTimes = 32;
				if (READ_FAILED_NEED_RETRY == ret && --retryTimes > 0)
				{
					while (--retryTimes > 0 && (ret = connectAndDumpBinlog(m_currFile.c_str(), m_currentPos) != 0))
						std::this_thread::sleep_for(std::chrono::seconds(1));
					if (!dsCheck(ret))
						dsReturn(ret);
					else
						goto READ;
				}
				else
				{
					dsReturn(ret);
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
						dsFailedAndLogIt(READ_CODE::READ_FAILED, "read the end of binlog " << m_localFile->getFile() << " ,but can not get the next filename", ERROR);
					}
					dsReturnIfFailed(connectAndDumpBinlog(nextBinlog, nextBinlogStartPos));
					m_readLocalBinlog = false;
					dsReturn(readRemoteBinlog(logEvent, size));
				}
				else
					dsReturn(ret);
			}
		}
		dsReturn(ret);
	}

	DS mysqlBinlogReader::readBinlog(const char*& binlogEvent, size_t& size)
	{
		dsReturnIfFailed(readBinlogWrap(binlogEvent, size));//todo

		const commonMysqlBinlogEventHeader_v4* header =
			(const commonMysqlBinlogEventHeader_v4*)binlogEvent;
		if (header->type == Log_event_type::FORMAT_DESCRIPTION_EVENT)
		{
			formatEvent* tmp = new formatEvent(binlogEvent, size);
			if (tmp == nullptr)
				dsFailedAndLogIt(READ_CODE::READ_FAILED, "read failed for Format_description_log_event of " << m_currFile << " parse failed", ERROR);

			if (m_descriptionEvent != NULL)
				delete m_descriptionEvent;
			m_descriptionEvent = tmp;
		}
		else if ((header->type == Log_event_type::ROTATE_EVENT
			&& header->timestamp == 0)) //end of file
		{
			RotateEvent* tmp = new RotateEvent(binlogEvent, size,
				m_descriptionEvent);
			if (getFileId(tmp->fileName) != m_currFileID)
			{
				m_currentPos = 4;//attach to next file
				m_currFile = tmp->fileName;
				m_currFileID = getFileId(tmp->fileName);
				m_currentFileRotateCount = 0;
				delete tmp;
			}
			else
			{
				delete tmp;
				if (++m_currentFileRotateCount > 1) //this rotate is created by reconnect,not need to send it to parser
					dsReturn(readBinlog(binlogEvent, size));
			}
		}

		if (header->eventOffset < m_currentPos)
		{
			if (header->type != FORMAT_DESCRIPTION_EVENT
				&& header->type != ROTATE_EVENT)
				m_currentPos += header->eventSize;
		}
		else
			m_currentPos = header->eventOffset;
		dsOk();
	}

	static DS showBinaryLogs(MYSQL* conn,
		std::map<uint64_t, fileInfo>& binaryLogs)
	{
		MYSQL_RES* res = NULL;
		MYSQL_ROW row;
		if (mysql_query(conn, "show binary logs")
			|| !(res = mysql_store_result(conn)))
		{
			dsFailedAndLogIt(1, "error show binary logs: " << mysql_errno(conn) << "," << mysql_error(conn), ERROR);
		}
		while (NULL != (row = mysql_fetch_row(res)))
		{
			if (row[0] != nullptr && atol(row[1]) != 0)//ignore empty binlog file
			{
				fileInfo file;
				file.fileName = row[0];
				file.size = atol(row[1]);
				file.timestamp = 0;
				binaryLogs.insert(std::pair<uint64_t, fileInfo>(getFileId(file.fileName.c_str()), file));
			}
		}
		mysql_free_result(res);
		dsOk();
	}

	DS mysqlBinlogReader::seekBinlogInRemote(uint32_t fileID, uint64_t position)
	{
		std::map<uint64_t, fileInfo> binaryLogs;
		if (m_conn != nullptr)
		{
			mysql_close(m_conn);
			m_conn = nullptr;
		}
		dsReturnIfFailed(m_mysqlConnector->getConnect(m_conn));
		dsReturnIfFailed(showBinaryLogs(m_conn, binaryLogs));
		if (binaryLogs.size() == 0)
		{
			dsFailedAndLogIt(1, "seek Binlog in remote mysql server failed for binlog list is empty", ERROR);
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = binaryLogs.find(fileID);
		if (iter == binaryLogs.end())
		{
			dsFailedAndLogIt(1, "file " << fileID << " is not exists in local binlog files", ERROR);
		}
		if ((uint64_t)iter->second.size < position)
		{
			dsFailedAndLogIt(1, "seek Binlog[" << position << "@" << fileID << "] in local binlog file list failed for binlog size:" << iter->second.size << " is less than position", ERROR);
		}
		dsReturn(connectAndDumpBinlog(iter->second.fileName.c_str(), position));
	}

	DS mysqlBinlogReader::seekBinlogInLocal(uint32_t fileID, uint64_t position)
	{
		if (m_localBinlogList == nullptr)
		{
			m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
			if (m_localBinlogList->load() != 0)
			{
				dsFailedAndLogIt(1, "load local binlog file list failed", ERROR);
			}
		}
		std::map<uint64_t, fileInfo>::const_iterator iter = m_localBinlogList->get().find(fileID);
		if (iter == m_localBinlogList->get().end())
		{
			dsFailedAndLogIt(1, "file " << fileID << " is not exists in local binlog files", ERROR);
		}
		if ((uint64_t)iter->second.size < position)
		{
			dsFailedAndLogIt(1, "seek Binlog[" << position << "@" << fileID << "] in local binlog file list failed for binlog size:" << iter->second.size << " is less than position", ERROR);
		}
		dsReturn(dumpLocalBinlog(iter->second.fileName.c_str(), position));
	}

	DS mysqlBinlogReader::seekBinlogByCheckpoint(uint32_t fileID, uint64_t position)
	{
		if (!dsCheck(seekBinlogInRemote(fileID, position)))
		{
			if (!m_localBinlogPath.empty())
			{
				getLocalStatus().clear();
				dsReturn(seekBinlogInLocal(fileID, position));
			}
			else
			{
				dsReturn(getLocalStatus().code);
			}
		}
		m_currentFileRotateCount = 0;
		dsOk();
	}

	DS mysqlBinlogReader::dumpBinlog(const char* file, uint64_t offset,
		bool localORRemote)
	{
		if (localORRemote)
		{
			dsReturn(dumpLocalBinlog(file, offset));
		}
		else
		{
			dsReturn(connectAndDumpBinlog(file, offset));
		}
	}

	DS mysqlBinlogReader::getFirstLogeventTimestamp(const char* file,
		uint64_t& timestamp, bool localORRemote)
	{
		dsReturnIfFailed(dumpBinlog(file, 4, localORRemote));
		uint32_t fmtEventTimestamp = 0;
		while (true)
		{
			const char* logEvent;
			size_t size;
			dsReturnIfFailed(localORRemote ? readLocalBinlog(logEvent, size) : readRemoteBinlog(logEvent, size));
			commonMysqlBinlogEventHeader_v4* header =
				(commonMysqlBinlogEventHeader_v4*)logEvent;
			if (header->type == FORMAT_DESCRIPTION_EVENT)
			{
				fmtEventTimestamp = header->timestamp;
				formatEvent* tmp = new formatEvent(logEvent, size);
				if (tmp == nullptr)
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "seek Binlog in remote mysql server failed for Format_description_log_event of " << m_currFile << " parse failed", ERROR);
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
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "getFirstLogeventTimestamp of file " << file << " failed for no valid log event in binlog file", ERROR);
				}
				else
				{
					LOG(ERROR) << "reach the end of " << file << ",but there is no ddl or dml log event ,use timestamp " << fmtEventTimestamp << " of  FORMAT_DESCRIPTION_EVENT as begin timestamp ";
					timestamp = fmtEventTimestamp;
					dsOk();
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
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "ILLEGAL query log event in " << header->eventOffset << "@" << file, ERROR);
				}
				if (query != NULL && strncasecmp(query, "BEGIN", 5) != 0) //not begin
				{
					timestamp = header->timestamp;
					dsOk();
				}
			}
			else if (header->type == Log_event_type::XID_EVENT) //commit
			{
				timestamp = header->timestamp;
				dsOk();
			}
		}
	}

	DS mysqlBinlogReader::getBinlogFileList(std::map<uint64_t, fileInfo>& files, bool localORRemote)
	{
		if (localORRemote)
		{
			if (m_localBinlogList == nullptr)
			{
				m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
				if (m_localBinlogList->load() != 0)
				{
					dsFailedAndLogIt(1, "load local binlog file list failed", ERROR);
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
			dsReturnIfFailed(m_mysqlConnector->getConnect(m_conn));
			dsReturnIfFailed(showBinaryLogs(m_conn, files));
			files = _files;
		}
		dsOk();
	}

	DS mysqlBinlogReader::seekBinlogFile(const std::map<uint64_t, fileInfo>& binaryLogs, uint64_t timestamp, bool strick, bool localORRemmote)
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
				dsFailedAndLogIt(READ_CODE::READ_FILE_NOT_EXIST, "seek Binlog in " << (localORRemmote ? "local binlog list " : "remote mysql server ") << "by timestamp:" << timestamp << " failed for binlog is not increase strictly,log :" << idx << "not exist", ERROR);
			}
			dsReturnIfFailed(getFirstLogeventTimestamp(iter->second.fileName.c_str(), timestampOfFile, localORRemmote));
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
					dsFailedAndLogIt(READ_CODE::READ_FILE_NOT_EXIST, "seek Binlog in remote mysql server failed ,"
						"begin timestamp " << timestampOfFile << " of the first binlog file " << iter->second.fileName << " is newer than " << timestamp, ERROR);
				}
				else
				{
					LOG(INFO) << "seek to the begin of binlog ,but can not find timestamp " << timestamp << ",in loose mode,use first file " << iter->second.fileName << " as the begin pos";
				}
			}
			else
				iter = binaryLogs.find(iter->first - 1);
		}
		m_currFile = iter->second.fileName;
		m_currFileID = iter->first;
		m_currentPos = 4;
		LOG(INFO) << "seek binlog file success ,timestamp " << timestamp << " ,file " << m_currFile;
		dsOk();
	}

	DS mysqlBinlogReader::seekBinlogFile(uint64_t timestamp, bool strick)
	{
		m_readLocalBinlog = false;
		std::map<uint64_t, fileInfo> binaryLogs;
		int ret = READ_OK;
		if (m_conn != nullptr)
		{
			mysql_close(m_conn);
			m_conn = nullptr;
		}
		dsReturnIfFailed(m_mysqlConnector->getConnect(m_conn));
		dsReturnIfFailed(showBinaryLogs(m_conn, binaryLogs));
		if (binaryLogs.size() == 0)
			dsFailedAndLogIt(READ_CODE::READ_FILE_NOT_EXIST, "seek Binlog in remote mysql server failed for binlog list is empty", ERROR);
		if (dsCheck(seekBinlogFile(binaryLogs, timestamp, strick, false)))
			dsReturn(getLocalStatus().code);
		if (!m_localBinlogPath.empty())
		{
			getLocalStatus().clear();
			if (m_localBinlogList == nullptr)
			{
				m_localBinlogList = new fileList(m_localBinlogPath.c_str(), m_localBinlogPrefix.c_str());
				if (m_localBinlogList->load() != 0)
					dsFailedAndLogIt(READ_CODE::LOCAL_FILE_DAMAGED, "load local binlog file list failed", ERROR);
			}
			if (dsCheck(seekBinlogFile(m_localBinlogList->get(), timestamp, strick, false)))
			{
				m_readLocalBinlog = true;
				dsOk();
			}
			else
				dsReturn(getLocalStatus().code);
		}
		else
			dsReturn(getLocalStatus().code);
	}

	DS mysqlBinlogReader::seekBinlogInFile(uint64_t timestamp, const char* fileName,
		bool localORRemmote, bool strick)
	{
		dsReturnIfFailed(dumpBinlog(fileName, 4, localORRemmote));
		uint64_t beginOffset = 4;
		uint64_t currentOffset = 4;
		int rotate = 0;
		while (true)
		{
			const char* logEvent;
			size_t size;
			if (localORRemmote)
				dsReturnIfFailed(readLocalBinlog(logEvent, size));
			else
				dsReturnIfFailed(readRemoteBinlog(logEvent, size));
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
			else if (header->type == Log_event_type::ROTATE_EVENT) //end of file
			{
				if(++rotate>1)
				{
					RotateEvent* event = new RotateEvent(logEvent, size, m_descriptionEvent);
					m_currFile = event->fileName;
					m_currentPos = 4;
					delete event;
					dsOk();
				}
				continue;
			}
			else if (header->type == Log_event_type::HEARTBEAT_LOG_EVENT)
			{
				if (strick)
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "reached end of binlog ,but can not find " << timestamp, ERROR);
				}
				else
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "seek to the endof binlog ,but can not find timestamp " << timestamp << ",in loose mode,use current pos " << m_currentPos << "@" << m_currFile << " as the begin pos", ERROR);
				}
			}

			if (header->timestamp >= timestamp)
			{
				m_currentPos = beginOffset;
				dsOk();
			}

			if (header->type == Log_event_type::QUERY_EVENT)
			{
				const char* query = NULL;
				uint32_t querySize = 0;
				if (0
					!= QueryEvent::getQuery(logEvent, size, m_descriptionEvent, query, querySize))
				{
					dsFailedAndLogIt(READ_CODE::READ_FAILED, "ILLEGAL query log event in " << header->eventOffset << "@" << m_currFile, ERROR);
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

	DS mysqlBinlogReader::seekBinlogByTimestamp(uint64_t timestamp, bool strick)
	{
		int ret = READ_OK;
		dsReturnIfFailed(seekBinlogFile(timestamp, strick));
		dsReturnIfFailed(seekBinlogInFile(timestamp, m_currFile.c_str(), m_readLocalBinlog, strick));
		m_currentFileRotateCount = 0;
		LOG(INFO)<<"seek timestamp:"<<timestamp<<" in log pos:"<<m_currFile<<"."<<m_currentPos;
		dsReturnIfFailed(dumpBinlog(m_currFile.c_str(), m_currentPos, m_readLocalBinlog));
		dsOk();
	}

	DS mysqlBinlogReader::startDump()
	{
		dsReturn(dumpBinlog(m_currFile.c_str(), m_currentPos, m_readLocalBinlog));
	}

	formatEvent* mysqlBinlogReader::getFmtDescEvent()
	{
		return m_descriptionEvent;
	}
}
