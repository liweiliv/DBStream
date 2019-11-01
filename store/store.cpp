#include "store.h"
#include "memory/bufferPool.h"
#include "message/record.h"
#include "meta/metaDataBaseCollection.h"
#include "database/database.h"
#include "database/block.h"
#include "schedule.h"
namespace STORE {
	DLL_EXPORT store::store(config* conf) : m_conf(conf)
	{
		m_bufferPool = new bufferPool();
		m_schedule = new schedule(conf);
		m_metaDataCollection = new META::metaDataCollection("utf8");
		m_genratedStream = new DATABASE::database(GENERATED_STREAM, conf,m_bufferPool,m_metaDataCollection);
		m_mainStream = new DATABASE::database(MAIN_STREAM, conf,m_bufferPool,m_metaDataCollection);
	}
	DLL_EXPORT int store::start()
	{
		if (0 != m_schedule->start())
		{
			LOG(ERROR) << "schedule module start failed";
			return -1;
		}
		if (0!=m_mainStream->load() || 0 != m_mainStream->start())
		{
			LOG(ERROR) << "blockManager module start failed";
			m_schedule->stop();
			return -1;
		}
		if (0!=m_genratedStream->load() || 0 != m_genratedStream->start())
		{
			LOG(ERROR) << "m_genratedStreamBlockManager module start failed";
			m_mainStream->stop();
			m_schedule->stop();
			return -1;
		}
		return 0;
	}
	DLL_EXPORT int store::stop()
	{
		m_genratedStream->stop();
		m_mainStream->stop();
		m_schedule->stop();
		return 0;
	}
	DLL_EXPORT void store::begin()
	{
		return m_mainStream->begin();
	}
	DLL_EXPORT void store::commit()
	{
		return m_mainStream->commit();
	}
	DLL_EXPORT bool store::checkpoint(uint64_t& timestamp, uint64_t &logOffset)
	{
		return m_mainStream->checkpoint(timestamp, logOffset);
	}

	DLL_EXPORT int store::insert(DATABASE_INCREASE::record* r)
	{
		return m_mainStream->insert(r);
	}

	DLL_EXPORT std::string store::updateConfig(const char* key, const char* value)
	{
		if (strncmp(key, C_SCHEDULE ".", sizeof(C_SCHEDULE)) == 0)
			return m_schedule->updateConfig(key, value);
		else if (strncmp(key, MAIN_STREAM ".", sizeof(MAIN_STREAM)) == 0)
			return m_mainStream->updateConfig(key, value);
		else if (strncmp(key, GENERATED_STREAM ".", sizeof(GENERATED_STREAM)) == 0)
			return m_genratedStream->updateConfig(key, value);
		else
			return std::string("unknown config :") + key;
	}
}
