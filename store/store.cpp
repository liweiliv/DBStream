#include "store.h"
#include "../message/record.h"
#include "blockManager.h"
#include "block.h"
#include "schedule.h"
namespace STORE {
	store::store(config* conf) : m_conf(conf)
	{
		m_schedule = new schedule(conf);
		m_genratedStreamBlockManager = new blockManager(GENERATED_STREAM, conf);
		m_mainStreamblockManager = new blockManager(MAIN_STREAM, conf, )
	}
	int store::start()
	{
		if (0 != m_schedule->start())
		{
			LOG(ERROR) << "schedule module start failed";
			return -1;
		}
		if (0 != m_mainStreamblockManager->start())
		{
			LOG(ERROR) << "blockManager module start failed";
			m_schedule->stop();
			return -1;
		}
		if (0 != m_genratedStreamBlockManager->start())
		{
			LOG(ERROR) << "m_genratedStreamBlockManager module start failed";
			m_mainStreamblockManager->stop();
			m_schedule->stop();
			return -1;
		}
		return 0;
	}
	std::string store::updateConfig(const char* key, const char* value)
	{
		if (strncmp(key, C_SCHEDULE ".", sizeof(C_SCHEDULE)) == 0)
			return m_schedule.updateConfig(key, value);
		else if (strncmp(key, MAIN_STREAM ".", sizeof(MAIN_STREAM)) == 0)
			return m_mainStreamblockManager.updateConfig(key, value);
		else if (strncmp(key, GENERATED_STREAM ".", sizeof(GENERATED_STREAM)) == 0)
			return m_genratedStreamBlockManager.updateConfig(key, value);
		else
			return std::string("unknown config:") + key;
	}
}