#include "store.h"
#include "storeConf.h"
#include "memory/bufferPool.h"
#include "memory/buddySystem.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "database/database.h"
#include "database/block.h"
#include "schedule.h"

namespace STORE {
	DLL_EXPORT store::store(config* conf) : m_conf(conf)
	{
		std::string bpType = conf->get(STORE_SECTION, STORE_MEM_ALLOCER, STORE_BUDDY_MEM_ALLOCER);
		if (bpType.compare(STORE_BUDDY_MEM_ALLOCER) == 0)
		{
			int64_t maxMem = conf->getLong(STORE_SECTION, STORE_MAX_MEM, STORE_MAX_MEM_DEFAULT, STORE_MAX_MEM_MIN, STORE_MAX_MEM_MAX);
			m_allocer = new buddySystem(maxMem, 4096, 14, 90, 10, highMemUsageCallback, nullptr, this);
		}
		else if (bpType.compare(STORE_SYSTEM_MEM_ALLOCER) == 0)
		{
			if (!conf->get(STORE_SECTION, STORE_MAX_MEM, "").empty())
			{
				LOG(ERROR) << STORE_SECTION << "." << STORE_SYSTEM_MEM_ALLOCER << " can not specified when " << STORE_MEM_ALLOCER << " is [" << STORE_SYSTEM_MEM_ALLOCER << "]";
				abort();
			}
			m_allocer = new defaultBufferBaseAllocer();
		}
		else
		{
			LOG(ERROR) << "unkown memAllocer:" << bpType;
			abort();
		}
		m_bufferPool = new bufferPool(m_allocer);
		m_schedule = new schedule(conf);
		m_metaDataCollection = new META::metaDataCollection("utf8");
		m_genratedStream = new DATABASE::database(GENERATED_STREAM, conf, m_bufferPool, m_metaDataCollection);
		m_mainStream = new DATABASE::database(MAIN_STREAM, conf, m_bufferPool, m_metaDataCollection);
	}
	DLL_EXPORT store::~store()
	{
		if (m_genratedStream != nullptr)
			delete m_genratedStream;
		if (m_mainStream != nullptr)
			delete m_mainStream;
		if (m_bufferPool != nullptr)
			delete m_bufferPool;
		if (m_schedule != nullptr)
			delete m_schedule;
		if (m_metaDataCollection != nullptr)
			delete m_metaDataCollection;
		if (m_allocer != nullptr)
			delete m_allocer;
	}
	void store::highMemUsageCallback(void* handle,uint16_t usages)
	{
		if (usages >= 99)
		{
			static_cast<store*>(handle)->m_mainStream->gc();
			static_cast<store*>(handle)->m_genratedStream->gc();
		}
		else
		{

		}

	}
	void store::monitor(store* s)
	{
		bool is
	}

	DLL_EXPORT int store::start()
	{
		if (0 != m_schedule->start())
		{
			LOG(ERROR) << "schedule module start failed";
			return -1;
		}
		if (0 != m_mainStream->load() || 0 != m_mainStream->start())
		{
			LOG(ERROR) << "blockManager module start failed";
			m_schedule->stop();
			return -1;
		}
		if (0 != m_genratedStream->load() || 0 != m_genratedStream->start())
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
	DLL_EXPORT bool store::checkpoint(uint64_t& timestamp, uint64_t& logOffset)
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
		else if (strcmp(key, STORE_BUDDY_MEM_ALLOCER) == 0)
			return std::string("can not change config:") + key;
		else if (strcmp(key, STORE_MAX_MEM) == 0)
		{
			if (!config::checkNumberString(value, STORE_MAX_MEM_MAX, STORE_MAX_MEM_MIN))
				return std::string("value of conf ") + STORE_SECTION + "." + STORE_MAX_MEM + " :[" + value + "] is not number or illege";
			if (m_conf->get(STORE_SECTION, STORE_MEM_ALLOCER, "").compare(STORE_BUDDY_MEM_ALLOCER) != 0)
				return std::string(STORE_SECTION) + "." + STORE_SYSTEM_MEM_ALLOCER + " can not specified when " + STORE_MEM_ALLOCER + " is not" + STORE_BUDDY_MEM_ALLOCER;
			static_cast<buddySystem*>(m_allocer)->resetMaxMemLimit(atol(value));
			m_conf->set(STORE_SECTION, key, value);
			return "";
		}
		else
			return std::string("unknown config :") + key;
	}
}
