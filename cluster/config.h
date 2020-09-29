#pragma once
#include <stdint.h>
#include <string>
#include <vector>
#include <functional>
#include "rpc.h"
#include "util/config.h"
#include "thread/shared_mutex.h"
namespace CLUSTER {
#define UPD_ARGV const std::string & key, const std::string& value, bool isInit
	class configBase {
	public:
		configBase() {}
		configBase(const configBase& conf)
		{
			for (std::map<std::string, std::function<std::string(const std::string&, const std::string&, bool)>>::const_iterator iter = conf.updateOpt.begin(); iter != conf.updateOpt.end(); iter++)
				registe(iter->first, iter->second);
		}

		virtual ~configBase() {}
		configBase& operator=(const configBase& conf)
		{
			updateOpt.clear();
			for (std::map<std::string, std::function<std::string(const std::string&, const std::string&, bool)>>::const_iterator iter = conf.updateOpt.begin(); iter != conf.updateOpt.end(); iter++)
				registe(iter->first, iter->second);
			return *this;
		}

	protected:
		shared_mutex m_lock;
		std::map<std::string, std::function<std::string(const std::string&, const std::string&, bool)>> updateOpt;
		void registe(const std::string& key, const std::function<std::string(const std::string&, const std::string&, bool)>& func)
		{
			updateOpt.insert(std::pair<std::string, std::function<std::string(const std::string&, const std::string&, bool)>>(key, func));
		}
	public:
		std::string update(const std::string& key, const std::string& value, bool isInit)
		{
			std::lock_guard<shared_mutex> guard(m_lock);
			std::map<std::string, std::function<std::string(const std::string&, const std::string&, bool)>>::const_iterator iter = updateOpt.find(key);
			if (iter == updateOpt.end()) {
				return "unknown conf";
			}
			return iter->second(key, value, isInit);
		}
	};
	constexpr static uint32_t DEFAULT_BLOCK_SIZE = 128 * 1024;
	constexpr static uint32_t MAX_BLOCK_SIZE = 32 * 1024 * 1024;

	constexpr static uint32_t DEFAULT_LOG_FILE_SIZE = 512 * 1024 * 1024;
	constexpr static uint32_t MAX_LOG_FILE_SIZE = UINT32_MAX;

	constexpr static uint32_t DEFAULT_MAX_LOG_ENTRY_SIZE = 1 * 1024 * 1024;
	constexpr static uint32_t MAX_LOG_ENTRY_SIZE = 32 * 1024 * 1024;


	class logConfig :public configBase {
	private:
		uint32_t m_defaultBlockSize;
		uint32_t m_defaultLogFileSize;
		uint32_t m_maxLogEntrySize;
	public:
		logConfig() :m_defaultBlockSize(DEFAULT_BLOCK_SIZE), m_defaultLogFileSize(DEFAULT_LOG_FILE_SIZE), m_maxLogEntrySize(DEFAULT_MAX_LOG_ENTRY_SIZE)
		{
			registe("defaultBlockSize", [=](UPD_ARGV) {return ::config::getUint32(value.c_str(), m_defaultBlockSize, 1, MAX_BLOCK_SIZE); });
			registe("defaultLogFileSize", [=](UPD_ARGV) {return::config::getUint32(value.c_str(), m_defaultLogFileSize, 1, MAX_LOG_FILE_SIZE); });
			registe("maxLogEntrySize", [=](UPD_ARGV) {return ::config::getUint32(value.c_str(), m_maxLogEntrySize, sizeof(logEntryRpcBase), MAX_LOG_ENTRY_SIZE); });
		}
		logConfig(const logConfig& conf) :configBase(conf), m_defaultBlockSize(conf.m_defaultBlockSize), m_defaultLogFileSize(conf.m_defaultLogFileSize), m_maxLogEntrySize(conf.m_maxLogEntrySize) {}
		logConfig& operator=(const logConfig& conf)
		{
			configBase::operator=(conf);
			m_defaultBlockSize = conf.m_defaultBlockSize;
			m_defaultLogFileSize = conf.m_defaultLogFileSize;
			m_maxLogEntrySize = conf.m_maxLogEntrySize;
			return *this;
		}
		uint32_t getDefaultBlockSize()
		{
			return m_defaultBlockSize;
		}
		uint32_t getDefaultLogFileSize()
		{
			return m_defaultLogFileSize;
		}
		uint32_t getMaxLogEntrySize()
		{
			return m_maxLogEntrySize;
		}
	};

	constexpr static auto DEFAULT_PURGE_PERIOD = 1800;
	constexpr static auto DEFAULT_STORAGE_TIME = 7 * 24 * 3600;
	constexpr static auto DEFAULT_MAX_LOGS_SIZE = 4096LL * 1024LL * 1024LL * 1024LL;
	constexpr static auto DEFAULT_CLUSTER_LOG_DIR = "./clusterLog";
	constexpr static auto DEFAULT_CLUSTER_LOG_NAME_PREFIX = "clusterLog";

	class clusterLogConfig :public configBase {
	private:
		//every m_purgePeriod seconds ,we call purge func delete files which last record append before  m_storageTime
		uint32_t m_purgePeriod;
		uint32_t m_storageTime;
		uint64_t m_maxFilesSize;
		std::string m_logDir;
		std::string m_logFileNamePrefix;
	public:
		clusterLogConfig() :m_purgePeriod(DEFAULT_PURGE_PERIOD), m_storageTime(DEFAULT_STORAGE_TIME), m_maxFilesSize(DEFAULT_MAX_LOGS_SIZE)
			, m_logDir(DEFAULT_CLUSTER_LOG_DIR), m_logFileNamePrefix(DEFAULT_CLUSTER_LOG_NAME_PREFIX)
		{
			registe("purgePeriod", [=](UPD_ARGV) {return::config::getUint32(value.c_str(), m_purgePeriod); });
			registe("storageTime", [=](UPD_ARGV) {return::config::getUint32(value.c_str(), m_storageTime); });
			registe("maxFilesSize", [=](UPD_ARGV) {return::config::getUint64(value.c_str(), m_maxFilesSize); });
			registe("logDir", [=](UPD_ARGV) {
				if (isInit)
					return "can not change logDir";
				m_logDir = value;
				return "";
				});
			registe("logFileNamePrefix", [=](UPD_ARGV) {
				if (isInit)
					return "can not change logFileNamePrefix";
				m_logFileNamePrefix = value;
				return "";
				});
		}
		clusterLogConfig(const clusterLogConfig& conf) :configBase(conf), m_purgePeriod(conf.m_purgePeriod), m_storageTime(conf.m_storageTime), m_maxFilesSize(conf.m_maxFilesSize)
			, m_logDir(conf.m_logDir), m_logFileNamePrefix(conf.m_logFileNamePrefix) {}
		clusterLogConfig& operator=(const clusterLogConfig& conf)
		{
			configBase::operator=(conf);
			m_purgePeriod = conf.m_purgePeriod;
			m_storageTime = conf.m_storageTime;
			m_maxFilesSize = conf.m_maxFilesSize;
			m_logDir = conf.m_logDir;
			m_logFileNamePrefix = conf.m_logFileNamePrefix;
			return *this;
		}
		uint32_t getPurgePeriod()
		{
			return m_purgePeriod;
		}
		uint32_t getStorageTime()
		{
			return m_storageTime;
		}
		uint64_t getMaxFilesSize()
		{
			return m_maxFilesSize;
		}
		std::string getLogDir()
		{
			m_lock.lock_shared();
			std::string value = m_logDir;
			m_lock.unlock_shared();
			return value;
		}
		std::string getLogFileNamePrefix()
		{
			m_lock.lock_shared();
			std::string value = m_logFileNamePrefix;
			m_lock.unlock_shared();
			return value;
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
		std::string update(const std::string& key, const std::string& value, bool isInit)
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
