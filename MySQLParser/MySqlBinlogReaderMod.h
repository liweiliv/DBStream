/*
 * MySqlBinlogReader.h
 *
 *  Created on: 2017年2月22日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLBINLOGREADER_H_
#define SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLBINLOGREADER_H_
#include <stdint.h>
#include <time.h>
#include <map>
#include <string>
#include "stackLog.h"
#include "MySQLTransaction.h"
#include "BinaryLogEvent.h"
struct _memp_ring;
class RawBinlog;
struct st_mysql;
class formatEvent;
class MySqlBinlogReader
{
public:
    enum READ_CODE
    {
        READ_OK,
        READ_TIMEOUT,
        READ_FAILED,
        READ_FAILED_NEED_RETRY,
        READ_END_OF_LOCAL_FILE
    };
protected:
    std::string m_currFile;
    uint64_t m_currentPos;
    time_t m_binlogTimestamp;

    std::string m_host;
    uint16_t m_port;
    std::string m_user;
    std::string m_passwd;

    /*ssl info*/
    std::string m_ca;
    std::string m_cert;
    std::string m_key;

    struct _memp_ring *m_memp;
    const std::map<std::string, std::string> & m_modConfig;
    formatEvent* m_descriptionEvent;
    bool m_isTypeNeedParse[256];

public:
    MySqlBinlogReader(struct _memp_ring * mp,
            const std::map<std::string, std::string> & modConfig) :
                m_currentPos(0), m_binlogTimestamp(0), m_port(
                    0), m_memp(mp), m_modConfig(modConfig), m_descriptionEvent(
                    NULL)
    {
        memset(m_isTypeNeedParse,0,sizeof(m_isTypeNeedParse));
        m_isTypeNeedParse[WRITE_ROWS_EVENT_V1]=true;
        m_isTypeNeedParse[WRITE_ROWS_EVENT]=true;
        m_isTypeNeedParse[UPDATE_ROWS_EVENT_V1]=true;
        m_isTypeNeedParse[UPDATE_ROWS_EVENT]=true;
        m_isTypeNeedParse[DELETE_ROWS_EVENT_V1]=true;
        m_isTypeNeedParse[DELETE_ROWS_EVENT]=true;
        m_isTypeNeedParse[ROWS_QUERY_LOG_EVENT]=true;
        m_isTypeNeedParse[QUERY_EVENT]=true;
        m_isTypeNeedParse[TABLE_MAP_EVENT]=true;
        m_isTypeNeedParse[FORMAT_DESCRIPTION_EVENT]=true;
        m_isTypeNeedParse[ROTATE_EVENT]=true;
        initStackLog();
    }
    virtual ~MySqlBinlogReader()
    {
        destroyStackLog();
    }
    virtual int seekBinlogByCheckpoint(uint32_t fileID, uint64_t position)=0;
    virtual int seekBinlogByTimestamp(uint64_t timestamp, bool strick = true)=0;
    virtual int seekBinlogFile(uint64_t timestamp, bool strick = true)=0;
    virtual int startDump()=0;
    virtual formatEvent * getFmtDescEvent()=0;
    virtual int readBinlog(MySQLLogWrapper *wrapper)=0;
    virtual int init()=0;
    void setMasterInfo(const char * host, short port, const char * user,
            const char *pass, const char * ca = NULL, const char *cert = NULL,
            const char * key = NULL)
    {
        m_host = host;
        m_port = port;
        m_user = user;
        m_passwd = pass;
        if (ca != NULL)
            m_ca = ca;
        if (cert != NULL)
            m_cert = cert;
        if (key != NULL)
            m_key = key;
    }
};
extern "C"
{
MySqlBinlogReader * createMySqlBinlogReader(struct _memp_ring *m_mp,
        const std::map<std::string, std::string> & modConfig);
int checkVersion(const char * ip, short port, const char * user,
        const char * pass,const char * ca , const char *cert,
        const char * key);
}

#endif/*SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLBINLOGREADER_H_*/
