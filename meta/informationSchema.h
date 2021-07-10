#pragma once
#include "metaDataBaseCollection.h"
#include "message/record.h"
#include "database/database.h"
#include "database/iterator.h"
#include "util/nameCompare.h"
#include "util/sparsepp/spp.h"
namespace META {
	enum class IS_TABLE_ID {
		TABLES_ID = 1,
		STATISTIC_ID,
		MAX_TABLE_ID
	};
	typedef spp::sparse_hash_map<const char*, TableMeta*, UTIL::nameCompare, UTIL::nameCompare> tableHashMap;
	static constexpr auto IF_SHEMA_NAME = "information_schema";
	class InformationSchema :public MetaDataBaseCollection
	{
	private:
		tableHashMap m_tables;
		DATABASE::database * m_file;
		UTIL::nameCompare m_nameCompare;
		TableMeta* m_tablesById[static_cast<int>(IS_TABLE_ID::MAX_TABLE_ID)];
		void tables()
		{
			TableMeta * tables = new TableMeta(m_nameCompare.caseSensitive);
			tables->m_dbName.assign(IF_SHEMA_NAME);
			tables->m_tableName.assign("tables");
			tables->m_id = TableMeta::genTableId(static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID),0);
			ColumnMeta* id = new ColumnMeta;
			id->m_columnName.assign("id");
			id->m_columnType = T_UINT64;
			id->m_signed = false;
			ColumnMeta* version = new columnMeta;
			version->m_columnName.assign("version");
			version->m_columnType = T_UINT32;
			version->m_signed = false;
			ColumnMeta* checkpoint = new columnMeta;
			checkpoint->m_columnName.assign("checkpoint");
			checkpoint->m_columnType = T_UINT64;
			checkpoint->m_signed = false;
			ColumnMeta* schema = new columnMeta;
			schema->m_columnName.assign("schema");
			schema->m_columnType = T_STRING;
			schema->m_size = 256;
			schema->m_charset = &charsets[utf8];
			ColumnMeta* table = new columnMeta;
			table->m_columnName.assign("table");
			table->m_columnType = T_STRING;
			table->m_size = 256;
			table->m_charset = &charsets[utf8];

			ColumnMeta* meta = new columnMeta;
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
			m_tables.insert(std::pair<const char, TableMeta*>("tables", tables));
			m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)] = tables;
		}
	public:
		InformationSchema(bool casesensitive):MetaDataBaseCollection(casesensitive,&charsets[utf8]), m_file(nullptr),m_tables(0,m_nameCompare,m_nameCompare)
		{
		}
		virtual ~InformationSchema() {}
		virtual TableMeta* get(uint64_t tableID)
		{
			if (TableMeta::tableID(tableID) < static_cast<uint32_t>(IS_TABLE_ID::MAX_TABLE_ID))
				return m_tablesById[TableMeta::tableID(tableID)];

			char * data = m_file->getRecord(m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)], KEY_TYPE::PRIMARY_KEY, 0, &tableID);
			if (data == nullptr)
				return nullptr;
			assert(((RPC::minRecordHead*)data)->type == RPC::R_INSERT);
			RPC::DMLRecord record(data,this);

			TableMeta* t = new TableMeta((RPC::TableMetaMessage*)record.column());
			basicBufferPool::free(record);
			return t;
		}
		virtual TableMeta* getPrevVersion(uint64_t tableID)
		{
			return nullptr;
		}
		virtual TableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)
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
		int put(const TableMeta* table,uint64_t checkpoint)
		{
			const char* TableMetaMessage = table->createTableMetaRecord();
			char * mem = m_file.allocMemForRecord(&m_default, ((const RPC::minRecordHead*)TableMetaMessage)->size+1024);
			if (mem == nullptr)
				return -1;
			RPC::DMLRecord record(mem, m_tablesById[static_cast<uint64_t>(IS_TABLE_ID::TABLES_ID)], RPC::R_INSERT);
			record.setFixedColumn<uint64_t>(0, TableMeta::tableID(table->m_id));
			record.setFixedColumn<uint32_t>(1, TableMeta::tableVersion(table->m_id));
			record.setFixedColumn<uint64_t>(2, checkpoint);
			record.setVarColumn(3, table->m_dbName.c_str(), table->m_dbName.size());
			record.setVarColumn(4, table->m_tableName.c_str(), table->m_tableName.size());
			record.setVarColumn(5, TableMetaMessage, ((const RPC::minRecordHead*)TableMetaMessage)->size);
			free(TableMetaMessage);
			if (0 != m_file.insert(&record))
				return -1;
			return 0;
		}
	};
}