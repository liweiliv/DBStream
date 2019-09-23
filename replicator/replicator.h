#pragma once
#include "util/config.h"
#include "util/winDll.h"
#include "replicatorConf.h"
#include <set>
namespace DATABASE_INCREASE {
	struct record;
}
namespace META {
	class metaDataCollection;
}
namespace SQL_PARSER {
	class sqlParser;
}
namespace REPLICATOR {
	class replicator {
	public:
		enum REP_ERROR {
			OK,
			SQL_FORMAT_ERR,
			CONNECT_FAILED,
		};
	protected:
		config* m_conf;
		std::string m_strategy;
		uint16_t m_maxThreadCount;
		uint16_t m_currentThreadCount;
		std::string m_lastError;
		REP_ERROR m_lastErrno;
		META::metaDataCollection* m_metaDataCollection;
		SQL_PARSER::sqlParser* m_sqlParser;
		bool m_running;
		bool m_localMeta;
		std::set<int> m_retryErrno;
		std::set<int> m_reconnectErrno;
		std::set<int> m_ignoreErrno;

		uint64_t m_safeLogOffset;
		uint64_t m_safeTimestamp;
	private:
		void initByConf()
		{
			m_strategy = m_conf->get(SECTION, REP_STRATEGY, "bucket");
			m_maxThreadCount = m_conf->getLong(SECTION, THREAD_COUNT, 1, 0, 65535);
		}

		virtual std::string updateStrategyConf(const char * key,const char * value) = 0;
	public:
		DLL_EXPORT replicator(config* conf, META::metaDataCollection* metaDataCollection) :m_conf(conf), m_currentThreadCount(0), m_currentThreadCount(0), m_lastErrno(OK),m_metaDataCollection(metaDataCollection), m_running(false), m_localMeta(false)
		{
			if (metaDataCollection == nullptr)
			{
				m_metaDataCollection = new META::metaDataCollection("utf8", nullptr);
				m_localMeta = true;
			}
			initByConf();
		}
		DLL_EXPORT virtual int start() = 0;
		DLL_EXPORT virtual int stop() = 0;
		DLL_EXPORT virtual int put(DATABASE_INCREASE::record* record) = 0;
		DLL_EXPORT virtual bool running() const = 0;
		DLL_EXPORT std::string updateConf(const char* key, const char* value)
		{
			if (strcmp(key, REP_STRATEGY) == 0)
			{
				return std::string("conf:") + REP_STRATEGY + " can not update here";
			}
			else if (strcmp(key, THREAD_COUNT) == 0)
			{
				long threadCount = 0;
				if (!config::char2num(value, threadCount))
					return std::string("conf:") + THREAD_COUNT + " must be a number";
				if(threadCount<=0||threadCount>=65535)
					return std::string("conf:") + THREAD_COUNT + " range is [1-65535]";
				m_maxThreadCount = threadCount;
			}
			else if (strncmp(key, CONN_SECTION, strlen(CONN_SECTION) == 0)
			{
				return std::string("conf:") + key+"."+value + " can not update here";
			}
			else if (strncmp(key, STRATEGY_SECTION, strlen(STRATEGY_SECTION) == 0)
			{
				return updateStrategyConf(key,value);
			}
			else 
				return std::string("unknown config:")+key+"."+value;
		}
	};
	
}
