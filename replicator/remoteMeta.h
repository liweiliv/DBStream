#pragma once
#include "util/config.h"
#include "meta/metaDataCollection.h"
namespace REPLICATOR {
	class remoteMeta {
	protected:
		config* m_conf;
		META::metaDataCollection* m_metaDataCollection;
	public:
		virtual int initMeta() = 0;
		virtual const META::tableMeta* updateMeta(const char* database, const char* table) = 0;
		virtual int escapeTableName(const META::tableMeta* table, std::string &escaped) = 0;
		virtual int escapeColumnName(const META::columnMeta* column, std::string& escaped) = 0;
		virtual const META::tableMeta* getMeta(const char* database, const char* table)
		{
			const META::tableMeta* meta;
			if (nullptr==(meta = m_metaDataCollection->get(database, table, 0xffffffffffffffffULL)))
			{
				if (nullptr == (meta = updateMeta(database, table)))
				{
					return nullptr;
				}
			}
			return meta;
		}
	};
}