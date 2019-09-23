#pragma once
#include "util/config.h"
#include "meta/metaDataCollection.h"
namespace REPLICATOR {
	class remoteMeta {
	private:
		config* m_conf;
		META::metaDataCollection* m_metaDataCollection;
	public:
		virtual int initMeta() = 0;
		virtual int updateMeta(const char* database, const char* table) = 0;
		virtual int escapeTableName(const META::tableMeta* table, std::string &escaped) = 0;
		virtual int escapeColumnName(const META::columnMeta* column, std::string& escaped) = 0;
	};
}