/*
 * BinlogRead.cpp
 *
 *  Created on: 2018年5月9日
 *      Author: liwei
 */
#include <string.h>
#include <unistd.h>
#include <stdint.h>
//#include <mysql.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <ifaddrs.h>
#include <my_sys.h>
#include "sql_common.h"
#include "mysqld_error.h"
#include <pthread.h>
#include <errmsg.h>
#include "Log_r.h"
#include "BinlogRead.h"
#include "MySQLTransaction.h"
#include "stackLog.h"
#include "BinlogFile.h"
#include "MempRing.h"
#include "BinaryLogEvent.h"
#include "MySqlUtils.h"
#define BIN_LOG_HEADER_SIZE	4U
using namespace std;
struct BinlogEvent
{
    const char * data;
    uint64_t dataSize;
};
BinlogRead::BinlogRead(struct _memp_ring * mp,
        const std::map<std::string, std::string> & modConfig) :
        MySqlBinlogReader(mp, modConfig)
{
    m_conn = NULL;
    m_serverId = genSrvierId(1);
    m_remoteServerID = 0;
    m_readLocalBinlog = false;
    m_localBinlogFiles = NULL;
    m_localBinlogItor = NULL;
    m_localFile = NULL;
    m_currFileID = 0;
}
BinlogRead::~BinlogRead()
{
    if (m_conn)
        mysql_close(m_conn);
    if (m_localBinlogFiles)
        delete m_localBinlogFiles;
    if (m_localBinlogItor)
        delete m_localBinlogItor;
    if (m_localFile)
        delete m_localFile;
    if (m_descriptionEvent)
        delete m_descriptionEvent;
}
int BinlogRead::init()
{
    return 0; //nothing to do
}

int BinlogRead::initRemoteServerID(uint32_t serverID)
{
    if (m_remoteServerID == 0)
        m_remoteServerID = serverID;
    else if (m_remoteServerID != serverID)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "initRemoteServerID failed,serverid changes from %u to %u",
                m_remoteServerID, serverID);
    }
    return 0;
}

int BinlogRead::connectDB(uint32_t retryTimes, bool ctrlFreq)
{
    /*使用最近600s内的连接次数作为sleep的参数，限制连接频率*/
    time_t now;
    int sleepSeconds = 0, _sleepSeconds = 0;
    RETRY: now = time(NULL);
    if (ctrlFreq)
    {
        while (!m_connFrequency.empty() && m_connFrequency.front() < now - 600)
            m_connFrequency.pop();
        m_connFrequency.push(now);
        if (m_connFrequency.size() > 10)
        {
            _sleepSeconds = (m_connFrequency.size() - 10)
                    * (m_connFrequency.size() - 10);
            if (_sleepSeconds > 32)
                sleepSeconds = 32;
            else
                sleepSeconds = _sleepSeconds;
        }
        else if (m_connFrequency.size() > 3)
            sleepSeconds = 1;
        if (sleepSeconds > 0)
        {
            Log_r::Warn(
                    "BinlogReader connect to mysql for %lu times in last 600 seconds , sleep %d seconds",
                    m_connFrequency.size(), sleepSeconds);
            sleep(sleepSeconds);
        }
    }
    if (NULL != m_conn)
    {
        if (0 == mysql_ping(m_conn))
            return 0;
        else
            mysql_close(m_conn);
    }
    if (NULL
            == (m_conn = ::connectDB(m_host.c_str(), m_port, m_user.c_str(),
                    m_passwd.c_str(), m_ca.empty() ? NULL : m_ca.c_str(),
                    m_cert.empty() ? NULL : m_cert.c_str(),
                    m_key.empty() ? NULL : m_key.c_str())))
    {
        if (--retryTimes > 0)
        {
            cleanStackLog();
            goto RETRY;
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(-1, -1, "connect to mysql server failed");
        }
    }
    string serverID;
    if (!getVariables(m_conn, "server_id", serverID) || serverID.empty())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "connect to mysql server success ,but can not ge serverid oof mysql server");
    }
    if (m_remoteServerID == 0)
        m_remoteServerID = atol(serverID.c_str());
    else if (m_remoteServerID != atol(serverID.c_str()))
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "connect to mysql server success ,but find serverid changes from %u to %s",
                m_remoteServerID, serverID.c_str());
    }
    return 0;
}
bool BinlogRead::getVariables(st_mysql *mysqld, const char * variableName,
        std::string &v)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    string sql = string("show variables where Variable_name='")
            + string(variableName) + string("'");
    if (0 != mysql_real_query(mysqld, sql.c_str(), sql.length()))
    {
        SET_STACE_LOG_AND_RETURN_(false, -1,
                "show variabls :%s failed for:%d,%s,sql:%s", variableName,
                mysql_errno(mysqld), mysql_error(mysqld), sql.c_str());
    }
    if (mysql_field_count(mysqld) == 0)
    {
        Log_r::Error("show variabls :%s failed for:%d,%s,sql:%s", variableName,
                mysql_errno(mysqld), mysql_error(mysqld), sql.c_str());
    }
    if (NULL == ((res) = mysql_store_result(mysqld)))
    {
        SET_STACE_LOG_AND_RETURN_(false, -1,
                "show variabls :%s failed for no result:%d,%s,sql:%s",
                variableName, mysql_errno(mysqld), mysql_error(mysqld),
                sql.c_str());
    }
    if (NULL != (row = mysql_fetch_row(res)))
    {
        v.assign(row[1]);
        mysql_free_result(res);
        return true;
    }
    else
    {
        mysql_free_result(res);
        SET_STACE_LOG_AND_RETURN_(false, -1,
                "show variabls :%s failed for no result,sql:%s", variableName,
                sql.c_str());
    }
}
int BinlogRead::setConnectToGetBinlog(const char * file, uint64_t pos)
{
    const char *cmd = "set @master_binlog_checksum = 'NONE'";
    if (mysql_query(m_conn, cmd) != 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1, "exec sql %s failed for error %d, %s",
                cmd, mysql_errno(m_conn), mysql_error(m_conn));
    }
    cmd = "SET @master_heartbeat_period = 500000000";
    if (mysql_query(m_conn, cmd) != 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1, "query %s failed for error %d, %s",
                cmd, mysql_errno(m_conn), mysql_error(m_conn));
    }
    size_t len = strlen(file);
    unsigned char buf[128];
    int4store(buf, pos > BIN_LOG_HEADER_SIZE ? pos : BIN_LOG_HEADER_SIZE);
    int2store(buf + 4, 0);
    int4store(buf + 6, m_serverId);
    memcpy(buf + 10, file, len);
    if (simple_command(m_conn, COM_BINLOG_DUMP, buf, len + 10, 1))
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "binlog dump error: %ld %s serverid:%u", pos, file, m_serverId);
        return -1;
    }
    return 0;
}
int BinlogRead::initFmtDescEvent()
{
    if (m_descriptionEvent != NULL)
        delete m_descriptionEvent;
    assert(!m_serverVersion.empty());
    m_descriptionEvent = new formatEvent(4, m_serverVersion.c_str());
    if (!m_descriptionEvent)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -2,
                "Failed creating Format_description_log_event; out of memory?");
    }
    return 0;
}
int BinlogRead::checkMasterVesion()
{
    uint64_t srvVersion = mysql_get_server_version(m_conn);
    int majorVersion = srvVersion / 10000;
    char serverVersion[64] =
    { 0 };
    sprintf(serverVersion, "%d.%ld.%ld", majorVersion,
            (srvVersion % 10000) / 100, srvVersion % 100);
    if (majorVersion < 5)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "mysql version %s is lower than 5,not support", serverVersion);
    }
    m_serverVersion = serverVersion;
    return 0;
}
int BinlogRead::connectAndDumpBinlog()
{
    if (m_conn)
    {
        mysql_close(m_conn);
        m_conn = NULL;
    }
    if (0 != connectDB())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "connect to remote mysql server failed ,dump binlog failed");
    }
    if (0 != checkMasterVesion())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "check server version failed ,dump binlog failed");
    }
    if (0 != initFmtDescEvent())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1, "create format desc log event failed");
    }
    Log_r::Notice("start dump binlog from %lu@%s", m_currentPos,
            m_currFile.c_str());
    if (0 != setConnectToGetBinlog(m_currFile.c_str(), m_currentPos))
    {
        mysql_close(m_conn);
        m_conn = NULL;
        SET_STACE_LOG_AND_RETURN_(-1, -1, "dump binlog failed");
    }
    m_readLocalBinlog = false;
    Log_r::Notice("dump binlog success");
    return 0;
}
int BinlogRead::dumpLocalBinlog()
{
    if (m_localBinlogIndex.empty())
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "local binlog index filename in config is empty");
    }
    if (0 != initFmtDescEvent())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1, "create format desc log event failed");
    }
    block_file_manager::iterator itor(m_localBinlogFiles,
            block_file_manager::get_id(m_currFile.c_str()),
            block_file_manager::BM_SEARCH_EQUAL);
    if (!itor.valid())
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "start read binlog %s failed for file not exist",
                m_currFile.c_str());
    }
    if (m_localFile != NULL)
        delete m_localFile;
    m_localFile =
            new ReadBinlogFile(
                    m_localBinlogPath.empty() ?
                            itor.get_filename() :
                            (string(m_localBinlogPath) + "/"
                                    + itor.get_filename()).c_str());
    if (m_localFile->setPosition(m_currentPos) != 0
            || m_localFile->getErr() != 0)
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "start read binlog from %s failed for open file failed",
                itor.get_filename());
    }
    m_readLocalBinlog = true;
    return 0;
}
uint32_t BinlogRead::genSrvierId(uint32_t seed)
{
    uint32_t serverId;
    struct ifaddrs * ifAddrStruct = NULL;
    struct ifaddrs * ifa = NULL;
    getifaddrs(&ifAddrStruct);
    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (ifa->ifa_addr->sa_family == AF_INET)
        {
            in_addr_t addr =
                    ((struct sockaddr_in *) ifa->ifa_addr)->sin_addr.s_addr;
            if (addr != 16777343) //127.0.0.1
            {
                srand(seed);
                serverId = rand();
                serverId <<= 20;
                // linux litte endian n.n+1.n+2.n+3 [n+3 n+2 n+1 n] in int32_t
                serverId |= (addr & 0x0fffffff);
                break;
            }
        }
    }
    if (ifAddrStruct != NULL)
        freeifaddrs(ifAddrStruct);
    if (0 == serverId)
        Log_r::Warn("auto gen server id error.");
    return serverId;
}
int BinlogRead::readLocalBinlog(BinlogEvent *event)
{
    const char * log = NULL;
    int ret = m_localFile->readBinlogEvent(log, event->dataSize);
    if (ret == 0)
    {
        event->data = log;
        return READ_CODE::READ_OK;
    }
    else
    {
        if (m_localFile->getErr() == BINLOG_READ_STATUS::BINLOG_READ_END)
        {
            if (!m_localBinlogItor->next()) //end of local binlog ,read from remote
            {
                Log_r::Notice("read the end of loacl binlog file %s",
                        m_localBinlogItor->get_filename());
                return READ_END_OF_LOCAL_FILE;
            }
            else
            {
                const char * nextBinlog = NULL;
                uint64_t nextBinlogStartPos;
                if (m_localFile->getNextBinlogFileInfo(nextBinlog,
                        nextBinlogStartPos) != 0 || nextBinlog == NULL)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "read the end of binlog %s ,but can not get the next filename",
                            m_localFile->getFile());
                }
                if (strcmp(m_localBinlogItor->get_filename(), nextBinlog) != 0)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "read the end of binlog %s ,but the next filename in index [%s] is not [%s] ",
                            m_localFile->getFile(),
                            m_localBinlogItor->get_filename(), nextBinlog);
                }
                if (0 != m_localFile->setFile(nextBinlog, nextBinlogStartPos))
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "start read log event from next binlog %s in position %lu failed ",
                            nextBinlog, nextBinlogStartPos);
                }
                return readLocalBinlog(event);
            }
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "read log event from local binlog file %s failed ,err code %d",
                    m_localFile->getFile(), m_localFile->getErr());
        }
    }
}
int BinlogRead::readRemoteBinlog(BinlogEvent *logEvent)
{
    uint64_t readLength = cli_safe_read(m_conn, NULL);
    if (packet_error == readLength)
    {
        int connErrno = mysql_errno(m_conn);
        Log_r::Error("error in read binlog: %d, %s", connErrno,
                mysql_error(m_conn));
        if (connErrno == CR_SERVER_LOST || connErrno == CR_SERVER_GONE_ERROR
                || connErrno == CR_CONN_HOST_ERROR)
        {
            SET_STACE_LOG_AND_RETURN_(READ_FAILED_NEED_RETRY,
                    READ_FAILED_NEED_RETRY, "read binlog failed for %d, %s",
                    mysql_errno(m_conn), mysql_error(m_conn));
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(READ_FAILED_NEED_RETRY, READ_FAILED,
                    "read binlog failed for %d, %s", mysql_errno(m_conn),
                    mysql_error(m_conn));
        }
    }
    NET* net = &m_conn->net;
    if (readLength < 8 && net->read_pos[0] == 254)
    {
        SET_STACE_LOG_AND_RETURN_(READ_FAILED_NEED_RETRY,
                READ_FAILED_NEED_RETRY, "read to end of binlog");
    }
    logEvent->data = (const char*) net->read_pos + 1;
    logEvent->dataSize = readLength - 1;
    return READ_OK;
}
int BinlogRead::readBinlog(MySQLLogWrapper *wrapper)
{
    int ret = READ_OK;
    BinlogEvent logEvent;
    if (!m_readLocalBinlog)
    {
        int retryTimes = 32;
        READ: if (READ_OK != (ret = readRemoteBinlog(&logEvent)))
        {
            if (READ_FAILED_NEED_RETRY == ret && --retryTimes > 0)
            {
                cleanStackLog();
                while (--retryTimes > 0 && (ret = connectAndDumpBinlog() != 0))
                {
                    cleanStackLog();
                }
                if (ret != 0)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED, "dump binlog failed");
                }
                else
                    goto READ;
            }
            else
            {
                SET_STACE_LOG_AND_RETURN(READ_FAILED_NEED_RETRY,
                        READ_FAILED_NEED_RETRY, "read to end of binlog");
            }
        }
    }
    else
    {
        ret = readLocalBinlog(&logEvent);
        if (ret != READ_OK)
        {
            if (ret == READ_END_OF_LOCAL_FILE)
            {
                const char * nextBinlog = NULL;
                uint64_t nextBinlogStartPos;
                if (m_localFile->getNextBinlogFileInfo(nextBinlog,
                        nextBinlogStartPos) != 0 || nextBinlog == NULL)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "read the end of binlog %s ,but can not get the next filename",
                            m_localFile->getFile());
                }
                m_currFile = nextBinlog;
                m_currentPos = nextBinlogStartPos;
                if (connectAndDumpBinlog() != 0)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "read the end of binlog %s ,but read from  next filename in remote mysql server  failed",
                            m_localFile->getFile());
                }
                m_readLocalBinlog = false;
                return readRemoteBinlog(&logEvent);
            }
            else
                return ret;
        }
    }
    commonMysqlBinlogEventHeader_v4 * header =
            (commonMysqlBinlogEventHeader_v4*) logEvent.data;
    if (header->type == Log_event_type::FORMAT_DESCRIPTION_EVENT)
    {
        formatEvent * tmp = new formatEvent(logEvent.data, logEvent.dataSize);
        if (tmp == NULL)
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek Binlog in remote mysql server failed for Format_description_log_event of %s parse failed",
                    m_currFile.c_str());
        }
        if (m_descriptionEvent != NULL)
            delete m_descriptionEvent;
        m_descriptionEvent = tmp;
    }
    else if ((header->type == Log_event_type::ROTATE_EVENT
            && header->timestamp == 0)) //end of file
    {
        const char *error_msg;
        RotateEvent * tmp = new RotateEvent(logEvent.data, logEvent.dataSize,
                m_descriptionEvent);
        if (m_currFile.compare(tmp->fileName) != 0)
            m_currentPos = 4;
        m_currFile = tmp->fileName;
        m_currFileID = block_file_manager::get_id(tmp->fileName);
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
    if (m_isTypeNeedParse[header->type])
    {
        wrapper->rawDataSize = logEvent.dataSize;
        wrapper->rawData = ring_alloc(m_memp, logEvent.dataSize);
        memcpy(wrapper->rawData, logEvent.data, logEvent.dataSize);
    }
    else
    {
        wrapper->rawDataSize = std::min(sizeof(commonMysqlBinlogEventHeader_v4),
                logEvent.dataSize);
        wrapper->rawData = ring_alloc(m_memp, wrapper->rawDataSize);
        memcpy(wrapper->rawData, logEvent.data, wrapper->rawDataSize);
    }
    wrapper->fileID = m_currFileID;
    wrapper->offset = m_currentPos;
    return READ_OK;
}
int BinlogRead::showBinaryLogs(st_mysql * conn,
        std::vector<binlogFileInfo> & binaryLogs)
{
    MYSQL_RES* res = NULL;
    MYSQL_ROW row;
    uint32_t fields;
    if (mysql_query(conn, "show binary logs")
            || !(res = mysql_store_result(conn)))
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1, "error show binary logs: %d, %s",
                mysql_errno(conn), mysql_error(conn));
    }
    while (NULL != (row = mysql_fetch_row(res)))
    {
        if (2 == (fields = mysql_num_fields(res)) && atol(row[1]) != 0)
            binaryLogs.push_back(binlogFileInfo(row[0], atol(row[1])));
    }
    mysql_free_result(res);
    return 0;
}
int BinlogRead::seekBinlogInRemote(uint32_t fileID, uint64_t position)
{
    std::vector<binlogFileInfo> binaryLogs;
    if (0 != connectDB())
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "seek Binlog in remote mysql server failed for connect db failed");
    }
    if (showBinaryLogs(m_conn, binaryLogs) != 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "seek Binlog in remote mysql server failed for showBinaryLogs fail");
    }
    if (binaryLogs.size() == 0)
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "seek Binlog in remote mysql server failed for binlog list is empty");
    }
    int s = 0, e = binaryLogs.size() - 1;
    while (e >= s)
    {
        int idx = (s + e) / 2;
        uint64_t _tmpFileID = block_file_manager::get_id(
                binaryLogs[idx].file.c_str());
        if (_tmpFileID == fileID)
        {
            if (binaryLogs[idx].size < position)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek Binlog[%lu@%u] in remote mysql server  failed for binlog size %lu is less than position",
                        position, fileID, binaryLogs[idx].size);
            }
            else
            {
                m_currFile = binaryLogs[idx].file;
                m_currentPos = position;
                if (connectAndDumpBinlog() != 0)
                {
                    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                            READ_CODE::READ_FAILED,
                            "start read binlog from %lu@%u failed", position,
                            fileID);
                }
                return READ_CODE::READ_OK;
            }
        }
        else if (_tmpFileID < fileID)
            s = idx + 1;
        else
            e = idx - 1;
    }
    SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED, READ_CODE::READ_FAILED,
            "seek Binlog[%lu@%u] in remote mysql server  failed for binlog is not exist",
            position, fileID);
}
int BinlogRead::seekBinlogInLocal(uint32_t fileID, uint64_t position)
{
    block_file_manager::iterator iter(m_localBinlogFiles, fileID,
            block_file_manager::search_type::BM_SEARCH_EQUAL);
    if (!iter.valid())
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "file %u is not exists in local binlog files", fileID);
    }
    else
    {
        if (m_localFile)
        {
            delete m_localFile;
            m_localFile = NULL;
        }
        m_localFile =
                new ReadBinlogFile(
                        m_localBinlogPath.empty() ?
                                iter.get_filename() :
                                (string(m_localBinlogPath) + "/"
                                        + iter.get_filename()).c_str());
        if (m_localFile->getErr() != 0)
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "start read log event from %lu@%s in local binlog failed for open binlog file failed,errno:%d",
                    position, iter.get_filename(), m_localFile->getErr());
            delete m_localFile;
            m_localFile = NULL;
        }
        if (0 != m_localFile->setPosition(position))
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "start read log event from %lu@%s in local binlog failed,errno:%d",
                    position, iter.get_filename(), m_localFile->getErr());
        }
        m_currFile = m_localFile->getFile();
        m_currentPos = position;
        Log_r::Notice("seek binlog [%lu@%s]success in local binlog files",
                position, m_localFile->getFile());
        return READ_CODE::READ_OK;
    }
}
int BinlogRead::seekBinlogByCheckpoint(uint32_t fileID, uint64_t position)
{
    if (seekBinlogInRemote(fileID, position) != 0)
    {
        if (!m_localBinlogIndex.empty())
        {
            string errorInfo;
            getChildLog(errorInfo); //save error in seekBinlogInRemote
            if (m_localBinlogFiles != NULL)
            {
                delete m_localBinlogFiles;
                m_localBinlogFiles = NULL;
            }
            m_localBinlogFiles = new block_file_manager(
                    m_localBinlogIndex.c_str());
            if (m_localBinlogFiles->get_err() != 0)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "start read log event in local binlog failed for load index from %s failed ,errno:%d",
                        m_localBinlogIndex.c_str(),
                        m_localBinlogFiles->get_err());
            }
            if (seekBinlogInLocal(fileID, position) != 0)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek binlog in remote mysql server failed for %s,and seek binlog in local binlog files is also failed",
                        errorInfo.c_str());
            }
            m_readLocalBinlog = true; //read form local binlog
            return READ_CODE::READ_OK;
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek binlog in remote mysql server failed");
        }
    }
    else
        return READ_CODE::READ_OK;
}

int BinlogRead::dumpBinlog(const char * file, uint64_t offset,
        bool localORRemote)
{
    m_currFile = file;
    m_currentPos = offset;
    if (localORRemote)
    {
        if (0 != dumpLocalBinlog())
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "start read binlog from %lu@%s failed", offset, file);
        }
    }
    else
    {
        if (0 != connectAndDumpBinlog())
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "start read binlog from %lu@%s failed", offset, file);
        }
    }
    return 0;
}
int BinlogRead::getFirstLogeventTimestamp(const char * file,
        uint64_t &timestamp, bool localORRemote)
{
    if (0 != dumpBinlog(file, 4, localORRemote))
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED, "dumpBinlog %s failed", file);
    }
    uint32_t fmtEventTimestamp = 0;
    while (true)
    {
        BinlogEvent logEvent;
        if (localORRemote ?
                0 != readLocalBinlog(&logEvent) :
                0 != readRemoteBinlog(&logEvent))
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "getFirstLogeventTimestamp of file %s failed", file);
        }
        commonMysqlBinlogEventHeader_v4 * header =
                (commonMysqlBinlogEventHeader_v4*) logEvent.data;
        if (header->type == FORMAT_DESCRIPTION_EVENT)
        {
            fmtEventTimestamp = header->timestamp;
            const char *error_msg;
            formatEvent * tmp = new formatEvent(logEvent.data,
                    logEvent.dataSize);
            if (tmp == NULL)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek Binlog in remote mysql server failed for Format_description_log_event of %s parse failed",
                        m_currFile.c_str());
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
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "getFirstLogeventTimestamp of file %s failed for no valid log event in binlog file",
                        file);
            }
            else
            {
                Log_r::Notice(
                        "reach the end of %s,but there is no ddl or dml log event ,use timestamp %u of  FORMAT_DESCRIPTION_EVENT as begin timestamp ",
                        file, fmtEventTimestamp);
                timestamp = fmtEventTimestamp;
                return 0;
            }
        }
        else if (header->type == QUERY_EVENT)
        {
            const char * query = NULL;
            uint32_t querySize = 0;
            if (0
                    != QueryEvent::getQuery(logEvent.data, logEvent.dataSize,
                            m_descriptionEvent, query, querySize))
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "ILLEGAL query log event in %u@%s", header->eventOffset,
                        file);
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
int BinlogRead::seekBinlogFileInRemote(uint64_t timestamp, bool strick)
{
    std::vector<binlogFileInfo> binaryLogs;
    if (connectDB() != 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "seek Binlog in remote mysql server failed connect to db failed");
    }
    if (showBinaryLogs(m_conn, binaryLogs) != 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1, -1,
                "seek Binlog in remote mysql server failed for showBinaryLogs fail");
    }
    if (binaryLogs.size() == 0)
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "seek Binlog in remote mysql server failed for binlog list is empty");
    }
    int s = 0, e = binaryLogs.size() - 1, idx = 0;
    uint64_t timestampOfFile = 0;
    while (e >= s)
    {
        idx = (s + e) / 2;
        if (0
                != getFirstLogeventTimestamp(binaryLogs[idx].file.c_str(),
                        timestampOfFile))
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek Binlog in remote mysql server failed");
        }
        Log_r::Notice("first log event timestamp of %s is %lu",
                binaryLogs[idx].file.c_str(), timestampOfFile);
        if (timestamp < timestampOfFile)
            e = idx - 1;
        else
            s = idx + 1;
    }
    if (timestamp <= timestampOfFile)
    {
        if (block_file_manager::get_id(binaryLogs[idx].file.c_str()) != 0)
        {
            if (strick)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek Binlog in remote mysql server failed ,"
                                "begin timestamp %lu of the first binlog file %s is newer than %lu",
                        timestampOfFile, binaryLogs[idx].file.c_str(),
                        timestamp);
            }
            else
            {
                Log_r::Notice(
                        "seek to the begin of binlog ,but can not find timestamp %lu,in loose mode,use first file %s as the begin pos",
                        timestamp, binaryLogs[idx].file.c_str());
            }
        }
    }
    m_currFile = binaryLogs[idx].file;
    m_currentPos = 4;
    Log_r::Notice("seekBinlogFileInRemote success ,timestamp %lu ,file %s",
            timestamp, m_currFile.c_str());
    return READ_CODE::READ_OK;
}
int BinlogRead::seekBinlogInFile(uint64_t timestamp, const char * fileName,
        bool localORRemmote, bool strick)
{
    if (0 != dumpBinlog(fileName, 4, localORRemmote))
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED, "dumpBinlog %s failed", fileName);
    }
    uint64_t beginOffset = 4;
    uint64_t currentOffset = 4;
    while (true)
    {
        BinlogEvent logEvent;
        if (localORRemmote ?
                0 != readLocalBinlog(&logEvent) :
                0 != readRemoteBinlog(&logEvent))
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek Binlog in remote mysql server failed for read file %s failed",
                    m_currFile.c_str());
        }
        commonMysqlBinlogEventHeader_v4 * header =
                (commonMysqlBinlogEventHeader_v4*) logEvent.data;
        if (header->type == Log_event_type::FORMAT_DESCRIPTION_EVENT)
        {
            formatEvent * tmp = new formatEvent(logEvent.data,
                    logEvent.dataSize);
            if (m_descriptionEvent != NULL)
                delete m_descriptionEvent;
            m_descriptionEvent = tmp;
        }
        else if ((header->type == Log_event_type::ROTATE_EVENT
                && header->timestamp > 0)) //end of file
        {
            RotateEvent * event = new RotateEvent(logEvent.data,
                    logEvent.dataSize, m_descriptionEvent);
            m_currFile = event->fileName;
            m_currentPos = 4;
            delete event;
            return 0;
        }
        else if (header->type == Log_event_type::HEARTBEAT_LOG_EVENT)
        {
            if (strick)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "reached end of binlog ,bu t can not find %lu",
                        timestamp);
            }
            else
            {
                Log_r::Notice(
                        "seek to the endof binlog ,but can not find timestamp %lu,in loose mode,use current pos %lu@%s as the begin pos",
                        timestamp, m_currentPos, m_currFile.c_str());
                return 0;
            }
        }

        if (header->timestamp >= timestamp)
        {
            m_currentPos = beginOffset;
            return 0;
        }

        if (header->type == Log_event_type::QUERY_EVENT)
        {
            const char * query = NULL;
            uint32_t querySize = 0;
            if (0
                    != QueryEvent::getQuery(logEvent.data, logEvent.dataSize,
                            m_descriptionEvent, query, querySize))
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "ILLEGAL query log event in %u@%s", header->eventOffset,
                        m_currFile.c_str());
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
int BinlogRead::seekBinlogFileInLocal(uint64_t timestamp, bool strick)
{
    std::vector<binlogFileInfo> binaryLogs;
    for (block_file_manager::iterator itor = m_localBinlogFiles->begin();
            itor.valid(); itor.next())
        binaryLogs.push_back(binlogFileInfo(itor.get_filename(), 0xffffffff));
    if (binaryLogs.size() == 0)
    {
        SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                READ_CODE::READ_FAILED,
                "seek Binlog in remote mysql server failed for binlog list is empty");
    }
    int s = 0, e = binaryLogs.size() - 1, idx = 0;
    uint64_t timestampOfFile = 0;
    while (e >= s)
    {
        idx = (s + e) / 2;
        if (0
                != getFirstLogeventTimestamp(binaryLogs[idx].file.c_str(),
                        timestampOfFile, true))
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek Binlog in remote mysql server failed");
        }
        Log_r::Notice("first log event timestamp of %s is %lu",
                binaryLogs[idx].file.c_str(), timestampOfFile);
        if (timestamp < timestampOfFile)
            e = idx - 1;
        else
            s = idx + 1;
    }
    if (timestamp <= timestampOfFile)
    {
        if (block_file_manager::get_id(binaryLogs[idx].file.c_str()) != 0)
        {
            if (strick)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek Binlog in remote mysql server failed ,"
                                "begin timestamp %lu of the first binlog file %s is newer than %lu",
                        timestampOfFile, binaryLogs[idx].file.c_str(),
                        timestamp);
            }
            else
            {
                Log_r::Notice(
                        "seek to the begin of binlog ,but can not find timestamp %lu,in loose mode,use first file %s as the begin pos",
                        timestamp, binaryLogs[idx].file.c_str());
            }
        }
    }
    m_currFile = binaryLogs[idx].file;
    m_currentPos = 4;
    return READ_CODE::READ_OK;
}
int BinlogRead::seekBinlogByTimestamp(uint64_t timestamp, bool strick)
{
    if (seekBinlogFileInRemote(timestamp, strick) != 0
            || seekBinlogInFile(timestamp, m_currFile.c_str(), false, strick)
                    != 0)
    {
        if (!m_localBinlogIndex.empty())
        {
            string errorInfo;
            getChildLog(errorInfo); //save error in seekBinlogInRemote
            if (m_localBinlogFiles != NULL)
            {
                delete m_localBinlogFiles;
                m_localBinlogFiles = NULL;
            }
            m_localBinlogFiles = new block_file_manager(
                    m_localBinlogIndex.c_str());
            if (seekBinlogFileInLocal(timestamp, strick) != 0
                    || seekBinlogInFile(timestamp, m_currFile.c_str(), true,
                            strick) != 0)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek binlog in remote mysql server failed for %s,and seek binlog in local binlog files is also failed",
                        errorInfo.c_str());
            }
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek binlog in remote mysql server failed");
        }
    }
    return READ_CODE::READ_OK;
}
int BinlogRead::seekBinlogFile(uint64_t timestamp, bool strick)
{
    if (seekBinlogFileInRemote(timestamp, strick) != 0)
    {
        if (!m_localBinlogIndex.empty())
        {
            string errorInfo;
            getChildLog(errorInfo); //save error in seekBinlogInRemote
            if (m_localBinlogFiles != NULL)
            {
                delete m_localBinlogFiles;
                m_localBinlogFiles = NULL;
            }
            m_localBinlogFiles = new block_file_manager(
                    m_localBinlogIndex.c_str());
            if (seekBinlogFileInLocal(timestamp, strick) != 0)
            {
                SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                        READ_CODE::READ_FAILED,
                        "seek binlog in remote mysql server failed for %s,and seek binlog in local binlog files is also failed",
                        errorInfo.c_str());
            }
        }
        else
        {
            SET_STACE_LOG_AND_RETURN_(READ_CODE::READ_FAILED,
                    READ_CODE::READ_FAILED,
                    "seek binlog in remote mysql server failed");
        }
    }
    return READ_CODE::READ_OK;
}
int BinlogRead::startDump()
{
    return dumpBinlog(m_currFile.c_str(), m_currentPos, m_readLocalBinlog);
}
formatEvent * BinlogRead::getFmtDescEvent()
{
    return m_descriptionEvent;
}
