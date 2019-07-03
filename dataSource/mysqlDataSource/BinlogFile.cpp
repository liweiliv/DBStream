/*
 * BinlogFile.cpp
 *
 *  Created on: 2017年2月16日
 *      Author: liwei
 */
#include "BinlogFile.h"
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <fcntl.h>
#include "BinaryLogEvent.h"
#include "mysql/my_byteorder.h"
#include "../../glog/logging.h"
#include <stdint.h>
#ifdef OS_LINUX
#include <zlib.h> //for checksum calculations
#endif
namespace DATA_SOURCE {
	/**
	 *   Calculate a long checksum for a memoryblock.
	 *
	 *     @param crc       start value for crc
	 *       @param pos       pointer to memory block
	 *         @param length    length of the block
	 *
	 *           @return checksum for a memory block
	 *           */
	static inline uint32_t checksum_crc32(uint32_t crc, const unsigned char* pos,
		size_t length)
	{
#ifdef OS_LINUX
		assert(length <= 0xffffffffu);
		return static_cast<uint32_t>(crc32(static_cast<unsigned int>(crc), pos,
			static_cast<unsigned int>(length)));
#else
		return 0;//todo
#endif
	}
	ReadBinlogFile::ReadBinlogFile(const char* filePath, bool crc)
	{
		m_filePath = filePath;
		m_fileSize = 0;
		m_readBuf = (char*)malloc(DEFAULT_BINLOG_BUF_SIZE);
		m_defaultDataBuf = m_readBuf;
		m_readBufSize = DEFAULT_BINLOG_BUF_SIZE;
		m_fileFD = 0;
		m_crc = crc;
		m_serverId = 0;
		m_nextBeginPos = 0;
		m_logEventOffset = 0;
		setFile(filePath);
	}
	ReadBinlogFile::~ReadBinlogFile()
	{
		if (m_fileFD > 0)
			closeFile(m_fileFD);
		if (m_readBuf != m_defaultDataBuf)
			free(m_readBuf);
		free(m_defaultDataBuf);
	}
	int ReadBinlogFile::getErr()
	{
		return m_err;
	}
	const char* ReadBinlogFile::getFile()
	{
		return m_filePath.c_str();
	}
	uint64_t ReadBinlogFile::getOffset()
	{
		return m_logEventOffset;
	}

	int ReadBinlogFile::setPosition(uint64_t offset)
	{
		if (_setPosition(offset) < 0)
			return -1;
		m_status = BINLOG_READ_FAKE_ROTATE;
		return 0;
	}
	int ReadBinlogFile::_setPosition(uint64_t offset)
	{
		if (m_err == BINLOG_ERR_EOF)
			m_err = 0;
		else if (m_err != 0) //一旦发生过错误，则不允许继续读
			return -1;
		if (offset < 4)
			offset = 4;
		if (offset == m_logEventOffset)
			return 0;
		m_fileSize = seekFile(m_fileFD, 0, SEEK_END);
		if (m_fileSize == (size_t)-1)
		{
			seekFile(m_fileFD, m_logEventOffset, SEEK_SET);
			m_err = -errno;
			return -1;
		}
		if (offset > m_fileSize)
		{
			seekFile(m_fileFD, m_logEventOffset, SEEK_SET);
			return -2;
		}
		if (-1 == seekFile(m_fileFD, offset, SEEK_SET))
		{
			seekFile(m_fileFD, m_logEventOffset, SEEK_SET);
			m_err = -errno;
			return -1;
		}
		m_logEventOffset = offset;
		return 0;
	}
	/*
	 *setFile不会重置m_crc
	 */
	int ReadBinlogFile::setFile(const char* filePath, uint64_t offset)
	{
		if (m_fileFD > 0)
		{
			closeFile(m_fileFD);
			m_fileFD = 0;
		}
		m_filePath = filePath;
		m_fileFD = openFile(filePath, true,false,false);
		if (m_fileFD <= 0)
		{
			m_err = -errno;
			return -1;
		}
		else
		{
			m_err = 0;
			return setPosition(offset < 4 ? 4 : offset);
		}
	}
	int ReadBinlogFile::readFormatEvent()
	{
		uint64_t logEventOffset = m_logEventOffset;
		_setPosition(4);
		if (0 != readBinlog()
			|| m_readBuf[EVENT_TYPE_OFFSET]
			!= FORMAT_DESCRIPTION_EVENT)
			return -1;
		m_crc = (uint8_t)m_readBuf[uint4korr(m_readBuf + EVENT_LEN_OFFSET)
			- BINLOG_CHECKSUM_LEN - BINLOG_CHECKSUM_ALG_DESC_LEN];
		m_serverId = uint4korr(m_readBuf + SERVER_ID_OFFSET);
		if (0 != uint4korr(m_readBuf + LOG_POS_OFFSET) && (uint4korr(m_readBuf + EVENT_LEN_OFFSET) + 4 != uint4korr(m_readBuf + LOG_POS_OFFSET)))
			return -1;
		//m_readBuf[FLAGS_OFFSET] &= ~LOG_EVENT_BINLOG_IN_USE_F;
		if (logEventOffset > 4)
			_setPosition(logEventOffset);
		return 0;
	}
	int ReadBinlogFile::fakeRotateEvent(const char*& binlog, size_t& size)
	{
		if (m_filePath.empty())
			return -1;
		const char* fileName = strrchr(m_filePath.c_str(), '/');
		if (fileName == NULL)
			fileName = m_filePath.c_str();
		else
			fileName++;
		size_t ident_len = strlen(fileName);
		size_t event_len = ident_len + LOG_EVENT_HEADER_LEN
			+ ROTATE_HEADER_LEN;
		if (m_crc != BINLOG_CHECKSUM_ALG_OFF)
			event_len += BINLOG_CHECKSUM_LEN;

		uint8_t* header = (uint8_t*)m_readBuf;
		uint8_t* rotate_header = header + LOG_EVENT_HEADER_LEN;
		int4store(header, 0);
		header[EVENT_TYPE_OFFSET] = ROTATE_EVENT;
		int4store(header + SERVER_ID_OFFSET, m_serverId);
		int4store(header + EVENT_LEN_OFFSET, static_cast<uint32_t>(event_len));
		int4store(header + LOG_POS_OFFSET, 0);
		int2store(header + FLAGS_OFFSET, LOG_EVENT_ARTIFICIAL_F);
		int8store(rotate_header, m_logEventOffset);
		memcpy(rotate_header + ROTATE_HEADER_LEN, fileName,
			ident_len);

		if (m_crc != BINLOG_CHECKSUM_ALG_OFF)
		{
			uint32_t crc = checksum_crc32(0L, NULL, 0);
			crc = checksum_crc32(crc, header, event_len - BINLOG_CHECKSUM_LEN);
			int4store(header + event_len - BINLOG_CHECKSUM_LEN, crc);
		}
		binlog = (const char*)header;
		size = event_len;
		return 0;
	}
	int ReadBinlogFile::readBinlog()
	{
		uint32_t binlogSize;
		if (m_err != 0) /*一旦发生过错误，则不允许继续读*/
			return -1;

		int readSize = readFile(m_fileFD, m_readBuf, LOG_EVENT_MINIMAL_HEADER_LEN);
		if (readSize < 0)
		{
			m_err = -errno;
			return -1;
		}
		else if (readSize == 0)
		{
			m_err = BINLOG_ERR_EOF;
			return -1;
		}
		else if (readSize != LOG_EVENT_MINIMAL_HEADER_LEN)
		{
			m_err = BINLOG_ERR_INCOMPLETE;
			return -1;
		}

		binlogSize = uint4korr(m_readBuf + EVENT_LEN_OFFSET);
		if (binlogSize < LOG_EVENT_MINIMAL_HEADER_LEN)
		{
			m_err = BINLOG_ERR_BAD_LENGTH;
			return -1;
		}
		/*未限制最大允许的binlogSize，如果文件内有格式错误，可能会读取到异常大的数据*/
		if (binlogSize > m_readBufSize)
		{
			if (m_defaultDataBuf != m_readBuf)
			{
				char* tmp = (char*)malloc(m_readBufSize = binlogSize);
				memcpy(tmp, m_readBuf, LOG_EVENT_MINIMAL_HEADER_LEN);
				free(m_readBuf);
				m_readBuf = tmp;
			}
			else
			{
				m_readBuf = (char*)malloc(m_readBufSize = binlogSize);
				memcpy(m_readBuf, m_defaultDataBuf, LOG_EVENT_MINIMAL_HEADER_LEN);
			}
		}
		else
		{
			if (m_defaultDataBuf != m_readBuf)
			{
				if (binlogSize > DEFAULT_BINLOG_BUF_SIZE)
				{
					char* tmp = (char*)malloc(m_readBufSize = binlogSize);
					memcpy(tmp, m_readBuf, LOG_EVENT_MINIMAL_HEADER_LEN);
					free(m_readBuf);
					m_readBuf = tmp;
				}
				else
				{
					memcpy(m_defaultDataBuf, m_readBuf, LOG_EVENT_MINIMAL_HEADER_LEN);
					free(m_readBuf);
					m_readBuf = m_defaultDataBuf;
					m_readBufSize = DEFAULT_BINLOG_BUF_SIZE;
				}
			}
		}
		readSize = readFile(m_fileFD, m_readBuf + LOG_EVENT_MINIMAL_HEADER_LEN,
			binlogSize - LOG_EVENT_MINIMAL_HEADER_LEN);
		if (readSize < 0)
		{
			m_err = -errno;
			return -1;
		}
		else if (binlogSize - LOG_EVENT_MINIMAL_HEADER_LEN != (uint32_t)readSize)
		{
			m_err = BINLOG_ERR_INCOMPLETE;
			return -1;
		}
		/* 校验binlog的连续性，binlog的offset必须是本条binlog的起始偏移
		 *     *binlogevent中的offset为uint32_t ,在超过4G的binlog中会溢出
		 *         */
		if (uint4korr(m_readBuf + LOG_POS_OFFSET)
			!= ((m_logEventOffset + binlogSize) & 0xffffffff))
		{
			m_err = BINLOG_ERR_BAD_LENGTH;
			return -1;
		}
		m_logEventOffset += binlogSize;
		return 0;
	}
	/*
	 * 读取下一条binlog
	 * 成功返回0，失败返回-1
	 * 失败后调用getErr()获取错误码
	 * 不管文件偏移设置成什么值,首先会发送虚拟的rotate，然后发送format，再从设置的文件偏移开始读取
	 */
	int ReadBinlogFile::readBinlogEvent(const char*& binlog, size_t& size)
	{
		if (m_err != 0) /*一旦发生过错误，则不允许继续读*/
		{
			binlog = NULL;
			size = 0;
			return -1;
		}
		if (m_status == BINLOG_READ_FAKE_ROTATE) /*BINLOG_READ_FAKE_ROTATE状态下，虚构一个rotate，然后直接发送*/
		{
			fakeRotateEvent(binlog, size);
			m_status = BINLOG_READ_FORMAT;
			return 0;
		}
		else if (m_status == BINLOG_READ_FORMAT)/*由于首先直接读取了format，而且在BINLOG_READ_FORMAT状态之前不会被覆盖，所以无需再读取，直接发送*/
		{
			if (readFormatEvent() != 0)
				goto ERR;
			m_status = BINLOG_READ_DATA;
		}
		else if (readBinlog() != 0)
			goto ERR;

		binlog = m_readBuf;
		size = uint4korr(m_readBuf + EVENT_LEN_OFFSET);

		if (binlog[EVENT_TYPE_OFFSET] == ROTATE_EVENT) //读取到文件尾
		{
			m_err = BINLOG_ERR_EOF;
			m_status = BINLOG_READ_END;
			if (m_crc)
				m_nextFile = std::string(
					binlog + LOG_EVENT_HEADER_LEN
					+ ROTATE_HEADER_LEN,
					size
					- (LOG_EVENT_HEADER_LEN
						+ ROTATE_HEADER_LEN
						+ BINLOG_CHECKSUM_LEN));
			else
				m_nextFile = std::string(
					binlog + LOG_EVENT_HEADER_LEN
					+ ROTATE_HEADER_LEN,
					size
					- (LOG_EVENT_HEADER_LEN
						+ ROTATE_HEADER_LEN));
			m_nextBeginPos = uint8korr(binlog + LOG_EVENT_HEADER_LEN);
		}
		/*通常stop event在文件末尾，没有rotate指示下一个binlog文件，此时根据文件名预测下一个binlog文件*/
		else if (binlog[EVENT_TYPE_OFFSET] == STOP_EVENT)
		{
			if (m_logEventOffset == (uint64_t)seekFile(m_fileFD, 0, SEEK_END))
			{
				char binlogFile[256] =
				{ 0 };
				strncpy(binlogFile, basename(m_filePath.c_str()), 256);
				int len = strlen(binlogFile) - 1;
				char c;
				for (char cr = 1; cr > 0;)
				{
					c = binlogFile[len];
					if (c > '9' || c < '0')
					{
						m_err = BINLOG_ERR_BAD_FILENAME;
						m_status = BINLOG_READ_END;
						goto ERR;
					}
					binlogFile[len] = '0' + (c - '0' + cr) % 10;
					cr = (c - '0' + cr) / 10;
					if (--len < 0)
					{
						m_err = BINLOG_ERR_BAD_FILENAME;
						m_status = BINLOG_READ_END;
						goto ERR;
					}
				}
				m_err = BINLOG_ERR_EOF;
				m_status = BINLOG_READ_END;
				m_nextFile = binlogFile;
				m_nextBeginPos = 4;
			}
			else
			{
				if (m_logEventOffset
					!= (uint64_t)seekFile(m_fileFD, m_logEventOffset, SEEK_SET))
				{
					m_err = -errno;
					m_status = BINLOG_READ_END;
					goto ERR;
				}

			}
		}
		return 0;
	ERR:
		binlog = NULL;
		size = 0;
		return -1;
	}
	int ReadBinlogFile::getNextBinlogFileInfo(const char*& binlogName,
		uint64_t& firstBinlog)
	{
		if (m_status != BINLOG_READ_END)
			return -1;
		binlogName = m_nextFile.c_str();
		firstBinlog = m_nextBeginPos;
		return 0;
	}
	WriteBinlogFile::WriteBinlogFile(size_t batch_size, bool sync, uint32_t flushCycle)
	{
		m_err = 0;
		m_fileFD = INVALID_HANDLE_VALUE;
		m_batch = batch_size;
		m_sync = sync;
		if (m_batch)
		{
			m_writeBuf = (char*)malloc(batch_size);
			m_writerBufPos = m_writeBuf;
		}
		else
			m_writerBufPos = m_writeBuf = NULL;
		m_logOffset = 0;
		m_lastFlushTime = 0;
		m_flushCycle = flushCycle;
	}

	int WriteBinlogFile::getErr()
	{
		return m_err;
	}
	int WriteBinlogFile::finish()
	{
		if (flushBuf() < 0)
		{
			LOG(ERROR) << "flush buf to file failed";
			return -1;
		}
		if (!m_sync)
		{
			if (0 != fsync(m_fileFD))
			{
				m_err = -errno;
				LOG(ERROR)<<"fsync failed,errno: "<< errno;
				return -2;
			}
		}

		if (0 != closeFile(m_fileFD))
		{
			while (errno == EINTR && 0 != closeFile(m_fileFD))
				errno = 0;
			if (errno != EINTR)
			{
				m_err = -errno;
				LOG(ERROR) << "close file failed,errno" << errno;
				return -4;
			}
		}
		return 0;
	}
	int WriteBinlogFile::setFileInfo(fileHandle fd, uint64_t offset)
	{
		m_err = 0;
		m_fileFD = fd;
		m_logOffset = offset;
		if (m_logOffset == sizeof(headMagicNumber))
		{
			if (seekFile(fd, 0, SEEK_END) > 0 && errno != ESPIPE)
			{
				m_err = BINLOG_ERR_INCOMPLETE;
				LOG(ERROR)<<"lseek64 0@SEEK_END failed,errno:"<<errno;
				return -1;
			}
		}
		else
		{
			if (seekFile(fd, 0, SEEK_END) < (int64_t)m_logOffset && errno != ESPIPE)
			{
				m_err = BINLOG_ERR_INCOMPLETE;
				LOG(ERROR) << "lseek64 0@SEEK_END failed,errno:" << errno;
				return -1;
			}
		}

		if (0 != seekFile(fd, 0, SEEK_SET) && errno != ESPIPE)
		{
			m_err = -errno;
			LOG(ERROR) << "lseek64 0@SEEK_END failed,errno:" << errno;
			return -2;
		}
		if (sizeof(headMagicNumber) != writeFile(fd, (char*)& headMagicNumber, sizeof(headMagicNumber)))
		{
			m_err = -errno;
			LOG(ERROR) << "write headMagicNumber to binlog file failed,errno"<<errno;
			return -3;
		}
		if (m_logOffset != (uint64_t)seekFile(fd, m_logOffset, SEEK_SET) && errno != ESPIPE)
		{
			m_err = -errno;
			LOG(ERROR) << "lseek64 "<< m_logOffset <<"@SEEK_SET failed,errno" << errno;
			return -4;
		}
		return 0;
	}
	/*
	 * return value
	 * <0 : error
	 * =0 : no data need flush
	 * >0 : flushed data
	 * */
	int WriteBinlogFile::flushBuf()
	{
		if (m_batch)
		{
			if (m_writerBufPos - m_writeBuf == 0)
				return 0;
			if (m_writerBufPos - m_writeBuf
				!= writeFile(m_fileFD, m_writeBuf,
					m_writerBufPos - m_writeBuf))
			{
				m_err = -errno;
				LOG(ERROR) << "write data in buf to file failed,errno"<<errno;
				return -1;
			}
			m_writerBufPos = m_writeBuf;
			if (m_sync)
			{
				if (0 != fsync(m_fileFD))
				{
					m_err = -errno;
					LOG(ERROR) << "call fsync failed after write data in buf to file,errno "<<errno;
					return -2;
				}
			}
		}
		else
		{
			if (m_sync)
			{
				if (0 != fsync(m_fileFD))
				{
					m_err = -errno;
					LOG(ERROR) << "fsync failed,errno "<<errno;
					return -2;
				}
			}
		}
		m_lastFlushTime = time(NULL);
		return 1;
	}
	WriteBinlogFile::~WriteBinlogFile()
	{
		if (m_fileFD > 0)
		{
			/*batch 模式下，如果开启了m_sync，会在flushBuf时flush*/
			if (m_batch)
				flushBuf();
			else if (m_sync)
				fsync(m_fileFD);
		}
		if (m_writeBuf != NULL)
			free(m_writeBuf);
	}

	int WriteBinlogFile::writeBinlogEvent(const char* binlogEvent, size_t size, bool& flushed)
	{
		flushed = false;
		uint32_t binlogSize = uint4korr(binlogEvent + EVENT_LEN_OFFSET);
		if (binlogSize > size)
		{
			m_err = BINLOG_ERR_BAD_LENGTH;
			LOG(ERROR) << "binlog size is diffrent between size by given [" << size << "] and size in log event [" << binlogSize << "] ";
			return -1;
		}
		if (binlogEvent[EVENT_TYPE_OFFSET] == FORMAT_DESCRIPTION_EVENT && m_logOffset > 4)//ignore dup FORMAT_DESCRIPTION_EVENT
			return 0;
		/* 校验binlog的连续性，binlog的offset必须是本条binlog的起始偏移
		 /binlogevent中的offset为uint32_t ,在超过4G的binlog中会溢出
		 */
		uint32_t binlogOffset = uint4korr(binlogEvent + LOG_POS_OFFSET);

		if (binlogOffset != ((m_logOffset + binlogSize) & 0xffffffff))
		{
			/*binlog的FORMAT_DESCRIPTION_EVENT特殊情况下offset可能是0*/
			if (!(m_logOffset == 4 && binlogEvent[EVENT_TYPE_OFFSET] == FORMAT_DESCRIPTION_EVENT &&
				uint4korr(binlogEvent + LOG_POS_OFFSET) == 0))
			{
				m_err = BINLOG_ERR_BAD_LENGTH;
				LOG(ERROR) << "binlog event size check failed,current binlog offset " << m_logOffset << " add size of new binlog event " << binlogSize <<
					" must equal offset of new binlog event ,but its offset is " << binlogOffset << ",type is " << binlogEvent[EVENT_TYPE_OFFSET];
				return -1;
			}
		}
		if (m_batch)
		{
			if (binlogSize > m_batch)
			{
				if (0 > flushBuf())
				{
					LOG(ERROR) << "flush buf to file failed";
					return -1;
				}
				if (binlogSize
					!= writeFile(m_fileFD, binlogEvent, binlogSize))
				{
					m_err = -errno;
					LOG(ERROR) << "write log event to file failed,errno "<<errno;
					return -1;
				}
				if (m_sync && fsync(m_fileFD) != 0)
				{
					LOG(ERROR) << "call fsync failed,errno "<<errno;
					return -2;
				}
				m_lastFlushTime = time(NULL);
				flushed = true;
			}
			else
			{
				if (m_writerBufPos + binlogSize > m_writeBuf + m_batch)
				{
					if (0 > flushBuf())
					{
						LOG(ERROR) << "flush buf to file failed";
						return -1;
					}
					flushed = true;
				}
				memcpy(m_writerBufPos, binlogEvent, binlogSize);
				m_writerBufPos += binlogSize;
				if (!flushed && time(NULL) - m_lastFlushTime >= m_flushCycle)
				{
					if (0 > flushBuf())
					{
						LOG(ERROR) << "flush buf to file failed";
						return -1;
					}
					flushed = true;
				}
			}
		}
		else
		{
			if (binlogSize
				!= writeFile(m_fileFD, binlogEvent, binlogSize))
			{
				m_err = -errno;
				LOG(ERROR) << "write log event to file failed,errno "<<errno;
				return -1;
			}
			if (m_sync && fsync(m_fileFD) != 0)
			{
				m_err = -errno;
				LOG(ERROR) << "call fsync failed,errno "<<errno;
			}
			flushed = true;
		}
		m_logOffset += binlogSize;
		return 0;
	}
}

