#pragma once
#include "metaDataBaseCollection.h"
#include "message/record.h"
#include "database/blockManager.h"
namespace META {
	class metaCollectionForMetaFile :public metaDataBaseCollection
	{
	private:
		tableMeta m_default;
		DATABASE::blockManager m_file;
	public:
		metaCollectionForMetaFile(bool casesensitive):metaDataBaseCollection(casesensitive,&charsets[utf8])
		{
			m_default.m_dbName.assign("infomation_schema");
			m_default.m_tableName.assign("meta");
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

			m_default.addColumn(id);
			m_default.addColumn(version);
			m_default.addColumn(checkpoint);
			m_default.addColumn(schema);
			m_default.addColumn(table);
			m_default.addColumn(meta);
			std::list<std::string> pk("id");
			m_default.createPrimaryKey(pk);
			std::list<std::string> uk;
			uk.push_back("schema");
			uk.push_back("table");
			m_default.addUniqueKey("table name", uk);
			delete id;
			delete version;
			delete checkpoint;
			delete schema;
			delete table;
		}
		virtual ~metaCollectionForMetaFile() {}
		virtual tableMeta* get(uint64_t tableID)
		{
			if (tableID != 1)
				return nullptr;
			else
				return &m_default;
		}
		virtual tableMeta* getPrevVersion(uint64_t tableID)
		{
			return nullptr
		}
		virtual tableMeta* get(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)
		{
			if (m_nameCompare.compare(database, "infomation_schema") != 0 ||
				(m_nameCompare.compare(table, "meta") != 0))
				return nullptr;
			return &m_default;
		}
		tableMeta* getMetaFromFile(const char* database, const char* table, uint64_t originCheckPoint = 0xffffffffffffffffULL)
		{
			DATABASE::blockManagerIterator iter()
		}
		int save(const tableMeta* table,uint64_t checkpoint)
		{
			const char* tableMetaMessage = table->createTableMetaRecord();
			char * mem = m_file.allocMemForRecord(&m_default, ((const DATABASE_INCREASE::minRecordHead*)tableMetaMessage)->size+1024);
			if (mem == nullptr)
				return -1;
			DATABASE_INCREASE::DMLRecord* record = new DATABASE_INCREASE::DMLRecord();
			record->initRecord(mem, &m_default, DATABASE_INCREASE::R_INSERT);
			record->setFixedColumn<uint64_t>(0, tableMeta::tableID(table->m_id));
			record->setFixedColumn<uint32_t>(1, tableMeta::tableVersion(table->m_id));
			record->setFixedColumn<uint64_t>(2, checkpoint);
			record->setVarColumn(3, table->m_dbName.c_str(), table->m_dbName.size());
			record->setVarColumn(4, table->m_tableName.c_str(), table->m_tableName.size());
			record->setVarColumn(5, tableMetaMessage, ((const DATABASE_INCREASE::minRecordHead*)tableMetaMessage)->size);
			free(tableMetaMessage);
			if (0 != m_file.insert(record))
				return -1;
			return 0;
		}
	};
}