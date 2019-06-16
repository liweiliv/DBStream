/*
 * BinlogFile.h
 *
 *  Created on: 2017年2月14日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_BINLOGFILE_H_
#define SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_BINLOGFILE_H_
#include <stdint.h>
#include <string>
/*
 * 文件错误err设置为errno，其他错误为以下错误号
 */
#define BINLOG_ERR_EOF 1  //读到文件尾
#define BINLOG_ERR_BAD_LENGTH 2 //binlog长度校验错误
#define BINLOG_ERR_WRONG_CRC 3 //crc校验失败
#define BINLOG_ERR_INCOMPLETE 4 //binlog残缺，无法读取到完整的binlog

#define BINLOG_WRITE_ERR_FILE_EXIST 5 //文件已存在
#define BINLOG_ERR_BAD_FILENAME 6
#define DEFAULT_BINLOG_BUF_SIZE (1024*1024*4)
//批量写入binlog，积累超过1M后写入文件
/*
 * 用于读取已经写完的binlog文件，不支持读取正在写入的binlog
 * 包括store备份的binlog文件和mysql的binlog文件
 * store备份的binlog文件如果时正在备份，则无需读取store正在备份的日志，直接读取源库的binlog即可
 */
const static int headMagicNumber = 0x6e6962fe;
class Format_description_log_event;
class Rotate_log_event;
enum BINLOG_READ_STATUS
{
   BINLOG_READ_FAKE_ROTATE,
   BINLOG_READ_FORMAT,
   BINLOG_READ_DATA,
   BINLOG_READ_END
};
class ReadBinlogFile
{
private:
    std::string m_filePath;
    int m_fileFD;
    size_t m_fileSize;
    char * m_readBuf;
    char * m_defaultDataBuf;
    uint32_t m_readBufSize;
    uint64_t m_logEventOffset;
    uint32_t m_serverId;
    uint8_t m_crc;
    std::string m_nextFile;
    uint64_t m_nextBeginPos;
    int m_err;
    BINLOG_READ_STATUS m_status;
public:
    ReadBinlogFile(const char *filePath,bool crc = false);
    ~ReadBinlogFile();
    int getErr();
    const char *getFile();
    uint64_t getOffset();
    int setPosition(uint64_t offset);
    int setFile(const char *filePath,uint64_t offset=0);
    /*
     * 读取下一条binlog
     * 成功返回0，失败返回-1
     * 失败后调用getErr()获取错误码
     */
    int readBinlogEvent(const char *&binlog, size_t &size);
    int getNextBinlogFileInfo(const char *&binlogName,uint64_t &firstBinlogPos);
private:
    int _setPosition(uint64_t offset);
    int readBinlog();
    int fakeRotateEvent(const char* &binlog,size_t & size);
    int readFormatEvent();
};

class WriteBinlogFile
{
private:
    int m_fileFD;
    char * m_writeBuf;
    char*  m_writerBufPos;
    size_t m_logOffset;
    int m_err;
    size_t m_batch;
    bool m_sync ;
    uint32_t m_flushCycle;
    uint32_t m_lastFlushTime;
public:
    WriteBinlogFile(size_t batch_size = 0,bool sync = false,uint32_t flushCycle = 1);
    int finish();
    int setFileInfo(int fd,uint64_t offset);
    int getErr();
    inline uint64_t getLogOffset(){return m_logOffset;}
    int flushBuf();
    ~WriteBinlogFile();
    int writeBinlogEvent(const char * binlogEvent,size_t size,bool &flushed);
};
#endif /* SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_BINLOGFILE_H_ */
