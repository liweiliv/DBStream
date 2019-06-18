#pragma once
#include<string>
class config;
namespace META {
	class metaDataCollection;
}
namespace STORE {
	class store;
}
namespace DATABASE_INCREASE {
	struct record;
}
namespace DATA_SOURCE
{
	class dataSource
	{
	protected:
		config* m_conf;
		META::metaDataCollection* m_metaDataCollection;
		STORE::store* m_store;
		std::string m_lastError;
	public:
		dataSource(config* conf, META::metaDataCollection* metaDataCollection, STORE::store* store) :m_conf(conf), m_metaDataCollection(metaDataCollection), m_store(store)
		{}
		virtual ~dataSource()
		{}
		virtual bool start() = 0;
		virtual bool stop() = 0;
		std::string getLastError()
		{
			return m_lastError;
		}
		virtual DATABASE_INCREASE::record* read()=0;
		virtual const char* dataSourceName() const = 0;
	};
}

