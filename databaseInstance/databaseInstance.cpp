#include "databaseInstance.h"
#include "instanceConf.h"
#include "memory/bufferPool.h"
#include "memory/buddySystem.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "database/database.h"
#include "database/block.h"
#include "schedule.h"

namespace DB_INSTANCE {
	DLL_EXPORT DatabaseInstance::DatabaseInstance(Config* conf) : m_conf(conf)
	{
		std::string bpType = conf->get(INSTANCE_SECTION, INSTANCE_MEM_ALLOCER, INSTANCE_BUDDY_MEM_ALLOCER);
		if (bpType.compare(INSTANCE_BUDDY_MEM_ALLOCER) == 0)
		{
			int64_t maxMem = conf->getLong(INSTANCE_SECTION, INSTANCE_MAX_MEM, INSTANCE_MAX_MEM_DEFAULT, INSTANCE_MAX_MEM_MIN, INSTANCE_MAX_MEM_MAX);
			m_allocer = new buddySystem(maxMem, 4096, 14, 90, 10, highMemUsageCallback, nullptr, this);
		}
		else if (bpType.compare(INSTANCE_SYSTEM_MEM_ALLOCER) == 0)
		{
			if (!conf->get(INSTANCE_SECTION, INSTANCE_MAX_MEM, "").empty())
			{
				LOG(ERROR) << INSTANCE_SECTION << "." << INSTANCE_SYSTEM_MEM_ALLOCER << " can not specified when " << INSTANCE_MEM_ALLOCER << " is [" << INSTANCE_SYSTEM_MEM_ALLOCER << "]";
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
		m_schedule = new Schedule(conf);
		m_metaDataCollection = new META::MetaDataCollection("utf8");
		m_genratedStream = new DATABASE::Database(GENERATED_STREAM, conf, m_bufferPool, m_metaDataCollection);
		m_mainStream = new DATABASE::Database(MAIN_STREAM, conf, m_bufferPool, m_metaDataCollection);
	}
	DLL_EXPORT DatabaseInstance::~DatabaseInstance()
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
	void DatabaseInstance::highMemUsageCallback(void* handle,uint16_t usages)
	{
		if (usages >= 95)
		{
			static_cast<DatabaseInstance*>(handle)->m_mainStream->fullGc();
			static_cast<DatabaseInstance*>(handle)->m_genratedStream->fullGc();
		}
		else
		{

		}

	}
	void DatabaseInstance::monitor(DatabaseInstance* s)
	{
	}

	DLL_EXPORT int DatabaseInstance::start()
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
	DLL_EXPORT int DatabaseInstance::stop()
	{
		m_genratedStream->stop();
		m_mainStream->stop();
		m_schedule->stop();
		return 0;
	}
	DLL_EXPORT void DatabaseInstance::begin()
	{
		return m_mainStream->begin();
	}
	DLL_EXPORT void DatabaseInstance::commit()
	{
		return m_mainStream->commit();
	}
	DLL_EXPORT bool DatabaseInstance::checkpoint(uint64_t& timestamp, uint64_t& logOffset)
	{
		return m_mainStream->checkpoint(timestamp, logOffset);
	}

	DLL_EXPORT int DatabaseInstance::insert(RPC::Record* r)
	{
		return m_mainStream->insert(r);
	}

	DLL_EXPORT std::string DatabaseInstance::updateConfig(const char* key, const char* value)
	{
		if (strncmp(key, C_SCHEDULE ".", sizeof(C_SCHEDULE)) == 0)
			return m_schedule->updateConfig(key, value);
		else if (strncmp(key, MAIN_STREAM ".", sizeof(MAIN_STREAM)) == 0)
			return m_mainStream->updateConfig(key, value);
		else if (strncmp(key, GENERATED_STREAM ".", sizeof(GENERATED_STREAM)) == 0)
			return m_genratedStream->updateConfig(key, value);
		else if (strcmp(key, INSTANCE_BUDDY_MEM_ALLOCER) == 0)
			return std::string("can not change config:") + key;
		else if (strcmp(key, INSTANCE_MAX_MEM) == 0)
		{
			if (!Config::checkNumberString(value, INSTANCE_MAX_MEM_MAX, INSTANCE_MAX_MEM_MIN))
				return std::string("value of conf ") + INSTANCE_SECTION + "." + INSTANCE_MAX_MEM + " :[" + value + "] is not number or illege";
			if (m_conf->get(INSTANCE_SECTION, INSTANCE_MEM_ALLOCER, "").compare(INSTANCE_BUDDY_MEM_ALLOCER) != 0)
				return std::string(INSTANCE_SECTION) + "." + INSTANCE_SYSTEM_MEM_ALLOCER + " can not specified when " + INSTANCE_MEM_ALLOCER + " is not" + INSTANCE_BUDDY_MEM_ALLOCER;
			static_cast<buddySystem*>(m_allocer)->resetMaxMemLimit(atol(value));
			m_conf->set(INSTANCE_SECTION, key, value);
			return "";
		}
		else
			return std::string("unknown config :") + key;
	}
}
