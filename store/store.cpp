#include "store.h"
#include "../memory/bufferPool.h"
#include "../message/record.h"
#include "../meta/metaDataCollection.h"
#include "blockManager.h"
#include "block.h"
#include "schedule.h"
namespace STORE {
	DLL_EXPORT store::store(config* conf) : m_conf(conf)
	{
		m_bufferPool = new bufferPool();
		m_schedule = new schedule(conf);
		m_metaDataCollection = new META::metaDataCollection("utf8");
		m_genratedStreamBlockManager = new blockManager(GENERATED_STREAM, conf,m_bufferPool,m_metaDataCollection);
		m_mainStreamblockManager = new blockManager(MAIN_STREAM, conf,m_bufferPool,m_metaDataCollection);
	}
	DLL_EXPORT int store::start()
	{
		if (0 != m_schedule->start())
		{
			LOG(ERROR) << "schedule module start failed";
			return -1;
		}
		if (0!=m_mainStreamblockManager->load() || 0 != m_mainStreamblockManager->start())
		{
			LOG(ERROR) << "blockManager module start failed";
			m_schedule->stop();
			return -1;
		}
		if (0!=m_genratedStreamBlockManager->load() || 0 != m_genratedStreamBlockManager->start())
		{
			LOG(ERROR) << "m_genratedStreamBlockManager module start failed";
			m_mainStreamblockManager->stop();
			m_schedule->stop();
			return -1;
		}
		return 0;
	}
	DLL_EXPORT int store::stop()
	{
		m_genratedStreamBlockManager->stop();
		m_mainStreamblockManager->stop();
		m_schedule->stop();
		return 0;
	}
	DLL_EXPORT void store::begin()
	{
		return m_mainStreamblockManager->begin();
	}
	DLL_EXPORT void store::commit()
	{
		return m_mainStreamblockManager->commit();
	}
	DLL_EXPORT bool store::checkpoint(uint64_t& timestamp, uint64_t &logOffset)
	{
		return m_mainStreamblockManager->checkpoint(timestamp, logOffset);
	}

	DLL_EXPORT int store::insert(DATABASE_INCREASE::record* r)
	{
		return m_mainStreamblockManager->insert(r);
	}

	DLL_EXPORT std::string store::updateConfig(const char* key, const char* value)
	{
		if (strncmp(key, C_SCHEDULE ".", sizeof(C_SCHEDULE)) == 0)
			return m_schedule->updateConfig(key, value);
		else if (strncmp(key, MAIN_STREAM ".", sizeof(MAIN_STREAM)) == 0)
			return m_mainStreamblockManager->updateConfig(key, value);
		else if (strncmp(key, GENERATED_STREAM ".", sizeof(GENERATED_STREAM)) == 0)
			return m_genratedStreamBlockManager->updateConfig(key, value);
		else
			return std::string("unknown config :") + key;
	}
}
