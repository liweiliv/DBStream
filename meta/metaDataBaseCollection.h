#pragma once
#include "metaData.h"
#include "util/nameCompare.h"
#include "charset.h"
namespace META
{
	class MetaDataBaseCollection
	{
	protected:
		UTIL::nameCompare m_nameCompare;
		const charsetInfo* m_defaultCharset;
	public:
		MetaDataBaseCollection(bool caseSensitive, const charsetInfo* defaultCharset) :m_nameCompare(caseSensitive), m_defaultCharset(defaultCharset) {}
		virtual ~MetaDataBaseCollection() {}
		virtual TableMeta* get(uint64_t tableID)=0;
		virtual TableMeta* getPrevVersion(uint64_t tableID) = 0;
		virtual TableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)=0;
	};
}
