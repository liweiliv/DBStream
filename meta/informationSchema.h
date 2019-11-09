#pragma once
#include "metaDataBaseCollection.h"
#include "message/record.h"
#include "database/database.h"
#include "database/iterator.h"
#include "nameCompare.h"
#include "util/sparsepp/spp.h"
namespace META {
	enum class IS_TABLE_ID {
		TABLES_ID = 1,
		STATISTIC_ID,
		MAX_TABLE_ID
	};
	typedef spp::sparse_hash_map<const char*, tableMeta*, nameCompare, nameCompare> tableHashMap;
	static constexpr auto IF_SHEMA_NAME = "information_schema";
	class informationSchema :public metaDataBaseCollection
	{
	private:
		tableHashMap m_tables;
		DATABASE::database * m_file;
		nameCompare m_nameCompare;
		tableMeta* m_tablesById[static_cast<int>(IS_TABLE_ID::MAX_TABLE_ID)];
		void tables()
		{
			tableMeta * tables = new tableMeta(m_nameCompare.caseSensitive);
			tables->m_dbName.assign(IF_SHEMA_NAME);
			tables->m_tableName.assign("tables");
			tables->m_id = tableMeta::genTableId(static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID),0);
			columnMeta* id = new columnMeta;
			id->m_columnName.assign("id");
			id->m_columnType = T_UINT64;
			id->m_signed = false;
			columnMeta* version = new columnMeta;
			version->m_columnName.assign("version");
			version->m_columnType = T_UINT32;
			version->m_signed = false;
			columnMeta* checkpoint = new columnMeta;
			checkpoint->m_columnName.assign("checkpoint");
			checkpoint->m_columnType = T_UINT64;
			checkpoint->m_signed = false;
			columnMeta* schema = new columnMeta;
			schema->m_columnName.assign("schema");
			schema->m_columnType = T_STRING;
			schema->m_size = 256;
			schema->m_charset = &charsets[utf8];
			columnMeta* table = new columnMeta;
			table->m_columnName.assign("table");
			table->m_columnType = T_STRING;
			table->m_size = 256;
			table->m_charset = &charsets[utf8];

			columnMeta* meta = new columnMeta;
			meta->m_columnName.assign("meta");
			meta->m_columnType = T_BLOB;

			tables->addColumn(id);
			tables->addColumn(version);
			tables->addColumn(checkpoint);
			tables->addColumn(schema);
			tables->addColumn(table);
			tables->addColumn(meta);
			std::list<std::string> pk("id");
			tables->createPrimaryKey(pk);
			std::list<std::string> uk;
			uk.push_back("schema");
			uk.push_back("table");
			uk.push_back("checkpoint");
			tables->addUniqueKey("tableName", uk);
			delete id;
			delete version;
			delete checkpoint;
			delete schema;
			delete table;
			m_tables.insert(std::pair<const char, tableMeta*>("tables", tables));
			m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)] = tables;
		}
	public:
		informationSchema(bool casesensitive):metaDataBaseCollection(casesensitive,&charsets[utf8]), m_file(nullptr),m_tables(0,m_nameCompare,m_nameCompare)
		{
		}
		virtual ~informationSchema() {}
		virtual tableMeta* get(uint64_t tableID)
		{
			if (tableMeta::tableID(tableID) < static_cast<uint32_t>(IS_TABLE_ID::MAX_TABLE_ID))
				return m_tablesById[tableMeta::tableID(tableID)];

			char * data = m_file->getRecord(m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)], KEY_TYPE::PRIMARY_KEY, 0, &tableID);
			if (data == nullptr)
				return nullptr;
			assert(((DATABASE_INCREASE::minRecordHead*)data)->type == DATABASE_INCREASE::R_INSERT);
			DATABASE_INCREASE::DMLRecord record(data,this);

			tableMeta* t = new tableMeta((DATABASE_INCREASE::TableMetaMessage*)record.column());
			basicBufferPool::free(record);
			return t;
		}
		virtual tableMeta* getPrevVersion(uint64_t tableID)
		{
			return nullptr;
		}
		virtual tableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)
		{
			if (m_nameCompare.compare(database, IF_SHEMA_NAME) == 0)
			{
				tableHashMap::iterator iter = m_tables.find(table);
				if (iter == m_tables.end())
					return nullptr;
				else
					return iter->second;
			}
			else
			{
				char tmpBuffer[1024];
				unionKey uk;
				uk.meta = m_tablesById[static_cast<int>(IS_TABLE_ID::TABLES_ID)]->getUniqueKey("tableName");
				uk.key = tmpBuffer;
				uint16_t pos = uk.startOffset();
				pos = uk.appendValue(database, strlen(database), 0, pos);
				pos = uk.appendValue(table, strlen(table), 1, pos);
				pos = uk.appendValue(&originCheckPoint, 8, 2, pos);
				setVarSize(pos);
				DATABASE::iterator* iter =  m_file->createIndexIterator(ITER_FLAG_DESC, m_tablesById[static_cast<int>(IS_TABLE_ID::TABLES_ID)], KEY_TYPE::UNIQUE_KEY, 0);
				if (iter == nullptr)
					return nullptr;
				if (!iter->valid())
				{
					delete iter;
					return nullptr
				}
				if (unionKey((const char*)iter->key(), uk.meta) != uk)
				{

				}
			}
		}
		int put(const tableMeta* table,uint64_t checkpoint)
		{
			const char* tableMetaMessage = table->createTableMetaRecord();
			char * mem = m_file.allocMemForRecord(&m_default, ((const DATABASE_INCREASE::minRecordHead*)tableMetaMessage)->size+1024);
			if (mem == nullptr)
				return -1;
			DATABASE_INCREASE::DMLRecord record(mem, m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)], DATABASE_INCREASE::R_INSERT);
			record.setFixedColumn<uint64_t>(0, tableMeta::tableID(table->m_id));
			record.setFixedColumn<uint32_t>(1, tableMeta::tableVersion(table->m_id));
			record.setFixedColumn<uint64_t>(2, checkpoint);
			record.setVarColumn(3, table->m_dbName.c_str(), table->m_dbName.size());
			record.setVarColumn(4, table->m_tableName.c_str(), table->m_tableName.size());
			record.setVarColumn(5, tableMetaMessage, ((const DATABASE_INCREASE::minRecordHead*)tableMetaMessage)->size);
			free(tableMetaMessage);
			if (0 != m_file.insert(&record))
				return -1;
			return 0;
		}
	};
}