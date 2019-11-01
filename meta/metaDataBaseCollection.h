#pragma once
#include "metaData.h"
#include "nameCompare.h"
#include "charset.h"
namespace META
{
	class metaDataBaseCollection
	{
	protected:
		nameCompare m_nameCompare;
		const charsetInfo* m_defaultCharset;
	public:
		metaDataBaseCollection(bool caseSensitive, const charsetInfo* defaultCharset) :m_nameCompare(caseSensitive), m_defaultCharset(defaultCharset) {}
		virtual ~metaDataBaseCollection() {}
		virtual tableMeta* get(uint64_t tableID)=0;
		virtual tableMeta* getPrevVersion(uint64_t tableID) = 0;
		virtual tableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)=0;
	};
}