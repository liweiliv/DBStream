#pragma once
#include <stdint.h>
#include <string>
#include <vector>
#include "rpc.h"
#include "util/config.h"
namespace CLUSTER {
	constexpr static auto DEFAULT_BLOCK_SIZE = 128 * 1024;
	constexpr static auto MAX_BLOCK_SIZE = 32 * 1024 * 1024;

	constexpr static auto DEFAULT_LOG_FILE_SIZE = 512 * 1024 * 1024;
	constexpr static auto MAX_LOG_FILE_SIZE = 4096 * 1024 * 1024;

	constexpr static auto DEFAULT_MAX_LOG_ENTRY_SIZE = 1 * 1024 * 1024;
	constexpr static auto MAX_LOG_ENTRY_SIZE = 32 * 1024 * 1024;


	struct logConfig {
		uint32_t m_defaultBlockSize;
		uint32_t m_defaultLogFileSize;
		uint32_t m_maxLogEntrySize;
		logConfig() :m_defaultBlockSize(DEFAULT_BLOCK_SIZE), m_defaultLogFileSize(DEFAULT_LOG_FILE_SIZE), m_maxLogEntrySize(DEFAULT_MAX_LOG_ENTRY_SIZE) {}
		logConfig(const logConfig& conf) :m_defaultBlockSize(conf.m_defaultBlockSize), m_defaultLogFileSize(conf.m_defaultLogFileSize), m_maxLogEntrySize(conf.m_maxLogEntrySize) {}
		logConfig& operator=(const logConfig& conf)
		{
			m_defaultBlockSize = conf.m_defaultBlockSize;
			m_defaultLogFileSize = conf.m_defaultLogFileSize;
			m_maxLogEntrySize = conf.m_maxLogEntrySize;
			return *this;
		}
		std::string update(const std::string& key, const std::string& value, bool isInit)
		{
			if (key.compare("defaultBlockSize") == 0)
				return ::config::getUint32(value.c_str(), m_defaultBlockSize, 1, MAX_BLOCK_SIZE);
			if (key.compare("defaultLogFileSize") == 0)
				return ::config::getUint32(value.c_str(), m_defaultLogFileSize, 1, MAX_LOG_FILE_SIZE);
			if (key.compare("defaultLogFileSize") == 0)
				return ::config::getUint32(value.c_str(), m_maxLogEntrySize, sizeof(logEntryRpcBase), MAX_LOG_ENTRY_SIZE);
			return "unknown conf";
		}
	};

	constexpr static auto DEFAULT_PURGE_PERIOD = 1800;
	constexpr static auto DEFAULT_STORAGE_TIME = 7 * 24 * 3600;
	constexpr static auto DEFAULT_MAX_LOGS_SIZE = 4096LL * 1024LL * 1024LL * 1024LL;
	constexpr static auto DEFAULT_CLUSTER_LOG_DIR = "./clusterLog";

	struct clusterLogConfig {
		//every m_purgePeriod seconds ,we call purge func delete files which last record append before  m_storageTime
		uint32_t m_purgePeriod;
		uint32_t m_storageTime;
		uint64_t m_maxFilesSize;
		std::string m_logDir;
		clusterLogConfig() :m_purgePeriod(DEFAULT_PURGE_PERIOD), m_storageTime(DEFAULT_STORAGE_TIME), m_maxFilesSize(DEFAULT_MAX_LOGS_SIZE), m_logDir(DEFAULT_CLUSTER_LOG_DIR){}
		clusterLogConfig(const clusterLogConfig& conf) :m_purgePeriod(conf.m_purgePeriod), m_storageTime(conf.m_storageTime), m_maxFilesSize(conf.m_maxFilesSize), m_logDir(conf.m_logDir){}
		clusterLogConfig& operator=(const clusterLogConfig& conf)
		{
			m_purgePeriod = conf.m_purgePeriod;
			m_storageTime = conf.m_storageTime;
			m_maxFilesSize = conf.m_maxFilesSize;
			m_logDir = conf.m_logDir;
			return *this;
		}
		std::string update(const std::string& key, const std::string& value,bool isInit)
		{
			if (key.compare("purgePeriod") == 0)
				return ::config::getUint32(value.c_str(), m_purgePeriod);
			if (key.compare("storageTime") == 0)
				return ::config::getUint32(value.c_str(), m_storageTime);
			if (key.compare("maxFilesSize") == 0)
				return ::config::getUint64(value.c_str(), m_maxFilesSize);
			if (key.compare("logDir") == 0)
			{
				if(isInit)
					return "can not change logDir";
				m_logDir = value;
				return "";
			}
			return "unknown conf";
		}
	};
	struct clusterNetConfig {
		std::vector<std::string> m_listenAddrs;
		uint32_t m_netOutTime;
		bool m_compress;
		uint32_t m_sendBufferSize;
	};
	struct config {
		logConfig m_logConfig;
		clusterLogConfig m_clusterLogConfig;
		clusterNetConfig m_clusterNetConfig;
		std::string update(const std::string& key, const std::string& value , bool isInit)
		{
			if (strncmp(key.c_str(), "log.", 4) == 0)
			{
				return m_logConfig.update(std::string(key.c_str() + 4, key.size() - 4), value, isInit);
			}
			else if (strncmp(key.c_str(), "logManager.", 11) == 0)
			{
				return m_clusterLogConfig.update(std::string(key.c_str() + 11, key.size() - 11), value, isInit);
			}
			else
			{
				return "unknown conf";
			}
		}
	};
}