/*
 * metaData.h
 *
 *  Created on: 2018年11月11日
 *      Author: liwei
 */

#ifndef _METADATA_H_
#define _METADATA_H_
#include <string>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <list>
#include <assert.h>
#include "mysqlTypes.h"
#include "charset.h"
#include "util/winDll.h"
#include "util/nameCompare.h"
#include "util/status.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
namespace RPC {
	struct TableMetaMessage;
}
namespace META {

#define MAX_KEY_SIZE 2048
	struct stringArray
	{
		char** m_array;
		uint32_t m_count;
		stringArray() :m_array(nullptr), m_count(0) {}
		~stringArray()
		{
			clean();
		}
		void add(const char* str)
		{
			add(str, strlen(str));
		}
		void add(const char* str, uint32_t size)
		{
			char** array = (char**)malloc(sizeof(char*) * (m_count + 1));
			memcpy(array, m_array, sizeof(char*) * m_count);
			array[m_count] = (char*)malloc(size + 1);
			memcpy(array[m_count], str, size);
			array[m_count][size] = '\0';
			if (nullptr != m_array)
				free(m_array);
			m_array = array;
			m_count++;
		}
		void clean()
		{
			if (m_count > 0)
			{
				for (uint32_t i = 0; i < m_count; i++)
					free(m_array[i]);
				free(m_array);
			}
			m_count = 0;
			m_array = nullptr;

		}
		stringArray& operator =(const stringArray& c)
		{
			clean();
			m_count = c.m_count;
			if (m_count > 0)
			{
				m_array = (char**)malloc(sizeof(char*) * m_count);
				for (uint32_t i = 0; i < m_count; i++)
				{
					int32_t size = (int)strlen(c.m_array[i]);
					m_array[i] = (char*)malloc(size + 1);
					memcpy(m_array[i], c.m_array[i], size);
					m_array[i][size] = '\0';
				}
			}
			return *this;
		}
		stringArray& operator =(const std::list<std::string>& l)
		{
			clean();
			m_array = (char**)malloc(sizeof(char*) * l.size());
			for (std::list<std::string>::const_iterator iter = l.begin(); iter != l.end(); iter++)
			{
				m_array[m_count] = (char*)malloc((*iter).size() + 1);
				memcpy(m_array[m_count], (*iter).c_str(), (*iter).size());
				m_array[m_count][(*iter).size()] = '\0';
			}
			return *this;
		}
		bool operator==(const stringArray& dest)const
		{
			if (m_count != dest.m_count)
				return false;
			for (uint32_t idx = 0; idx < m_count; idx++)
			{
				if (strcmp(m_array[idx], dest.m_array[idx]) != 0)
					return false;
			}
			return true;
		}
		bool operator!=(const stringArray& dest)const
		{
			return !(*this == (dest));
		}

	};
	struct DefaultValue
	{
		void* m_defaultValue;
		COLUMN_TYPE  m_defaultValueType;
		uint32_t m_defaultValueSize;
		DefaultValue() :m_defaultValue(nullptr), m_defaultValueType(COLUMN_TYPE::T_MAX_TYPE), m_defaultValueSize(0) {}
		~DefaultValue()
		{
			if (m_defaultValue != nullptr)
				free(m_defaultValue);
		}
		DefaultValue(const DefaultValue& v) :m_defaultValue(nullptr), m_defaultValueType(v.m_defaultValueType), m_defaultValueSize(v.m_defaultValueSize)
		{
			if (v.m_defaultValue != nullptr && v.m_defaultValueSize > 0)
			{
				m_defaultValue = malloc(m_defaultValueSize);
				memcpy(m_defaultValue, v.m_defaultValue, m_defaultValueSize);
			}
		}
		DefaultValue& operator=(const DefaultValue& v)
		{
			m_defaultValueSize = v.m_defaultValueSize;
			m_defaultValueType = v.m_defaultValueType;
			if (v.m_defaultValue != nullptr && v.m_defaultValueSize > 0)
			{
				m_defaultValue = malloc(m_defaultValueSize);
				memcpy(m_defaultValue, v.m_defaultValue, m_defaultValueSize);
			}
			else
			{
				m_defaultValue = nullptr;
			}
			return *this;
		}
		bool operator==(const DefaultValue& v) const
		{
			if (m_defaultValueType != v.m_defaultValueType)
				return false;
			if (m_defaultValueSize != v.m_defaultValueSize)
				return false;
			if (m_defaultValue == nullptr && v.m_defaultValue == nullptr)
				return true;
			if (m_defaultValue == nullptr || v.m_defaultValue == nullptr)
				return false;
			return memcmp(m_defaultValue, v.m_defaultValue, m_defaultValueSize) == 0;
		}
		bool operator!=(const DefaultValue& v) const
		{
			return !(*this == (v)); 
		}
	};
#define COL_FLAG_SIGNED  0x01
#define COL_FLAG_NOT_NULL  0x02
#define COL_FLAG_PRIMARY_KEY  0x04
#define COL_FLAG_UNIQUE_KEY  0x08
#define COL_FLAG_INDEX  0x10
#define COL_FLAG_GENERATED  0x20
#define COL_FLAG_HIDDEN  0x40

	struct ColumnMeta
	{
		COLUMN_TYPE m_columnType; //type in DBStream
		uint8_t m_srcColumnType;// type in database
		uint8_t m_segmentCount;//int oracle
		uint16_t m_segmentStartId;//in oracle
		uint16_t m_columnIndex;  //column id in table
		std::string m_columnName;
		std::string m_alias;
		DefaultValue m_default;
		std::string m_collate;
		const charsetInfo* m_charset;
		uint32_t m_size;
		uint32_t m_precision;
		uint32_t m_decimals;
		stringArray m_setAndEnumValueList;
		uint32_t m_flag;
		ColumnMeta() :m_columnType(COLUMN_TYPE::T_MAX_TYPE), m_srcColumnType(0), m_segmentCount(0), m_segmentStartId(0), m_columnIndex(0), m_charset(nullptr), m_size(0), m_precision(0), m_decimals(0),
			m_setAndEnumValueList(), m_flag(0)
		{}
		ColumnMeta& operator =(const ColumnMeta& c)
		{
			m_columnType = c.m_columnType;
			m_srcColumnType = c.m_srcColumnType;
			m_segmentCount = c.m_segmentCount;
			m_segmentStartId = c.m_segmentStartId;
			m_columnIndex = c.m_columnIndex;
			m_columnName = c.m_columnName;
			m_default = c.m_default;
			m_charset = c.m_charset;
			m_collate = c.m_collate;
			m_size = c.m_size;
			m_precision = c.m_precision;
			m_decimals = c.m_decimals;
			m_setAndEnumValueList = c.m_setAndEnumValueList;
			m_flag = c.m_flag;
			return *this;
		}
		bool operator==(const ColumnMeta& c)const
		{
			if (m_columnType != c.m_columnType ||
				m_srcColumnType != c.m_srcColumnType ||
				m_segmentCount != c.m_segmentCount ||
				m_segmentStartId != c.m_segmentStartId ||
				m_columnIndex != c.m_columnIndex ||
				m_columnName != c.m_columnName ||
				m_default != c.m_default ||
				m_collate != c.m_collate ||
				m_charset != c.m_charset ||
				m_size != c.m_size ||
				m_precision != c.m_precision ||
				m_decimals != c.m_decimals ||
				m_setAndEnumValueList != c.m_setAndEnumValueList ||
				m_flag != c.m_flag
				)
				return false;
			return true;
		}
		bool operator!=(const ColumnMeta& c) const
		{
			return !(*this == c);
		}
		inline bool testFlag(uint32_t flag) const
		{
			return m_flag & flag;
		}
		inline uint32_t getFlag(uint32_t flag) const
		{
			return m_flag & flag;
		}
		inline void setFlag(uint32_t flag)
		{
			m_flag |= flag;
		}
		inline void unsetFlag(uint32_t flag)
		{
			m_flag &= ~flag;
		}
		std::string toString()const;
	};
	typedef spp::sparse_hash_map<const char*, ColumnMeta*, StrHash, StrCompare> tableColMap;
	struct DLL_EXPORT TableMeta
	{
		std::string  m_dbName;
		//for postgresql
		std::string  m_schemaName;
		std::string  m_tableName;
		const charsetInfo* m_charset;
		std::string m_collate;
		ColumnMeta* m_columns;
		uint16_t* m_realIndexInRowFormat;
		uint16_t* m_fixedColumnOffsetsInRecord;
		uint16_t m_fixedColumnCount;
		uint16_t m_varColumnCount;
		uint16_t m_columnsCount;
		uint64_t m_id;
		uint64_t m_objectIdInDB;//object id of table in source database,now used in oracle
		uint32_t m_subObjectIdInDBListSize;
		uint64_t* m_subObjectIdInDBList;//sub object id of table (like partition id) in source database,now used in oracle
		UnionKeyMeta* m_primaryKey;
		std::string m_primaryKeyName;
		uint16_t m_uniqueKeysCount;
		UnionKeyMeta** m_uniqueKeys;
		std::string* m_uniqueKeyNames;
		uint16_t m_indexCount;
		UnionKeyMeta** m_indexs;
		std::string* m_indexNames;
		uint16_t* m_unusedColumnIds;
		uint16_t m_unusedColumnIdCount;
		UTIL::nameCompare m_nameCompare;
		tableColMap m_colsByName;
		void* userData;
		constexpr static inline uint16_t tableVersion(uint64_t tableIDInfo)
		{
			return tableIDInfo & 0xffff;
		}
		constexpr static inline uint64_t tableID(uint64_t tableIDInfo)
		{
			return (tableIDInfo & 0xffffffffffff0000ul) >> 16;
		}
		constexpr static inline uint64_t genTableId(uint64_t tableid, uint16_t version)
		{
			return (tableid << 16) | version;
		}
		TableMeta(bool caseSensitive);
		TableMeta(RPC::TableMetaMessage* msg);
		const char* createTableMetaRecord()const;
		void clean();
		~TableMeta();
		TableMeta& operator =(const TableMeta& t);
		inline const ColumnMeta* getColumn(uint16_t idx) const
		{
			if (idx > m_columnsCount)
				return nullptr;
			return &m_columns[idx];
		}

		inline const ColumnMeta* getColumn(const char* columnName) const
		{
			if (m_columnsCount < 5)
			{
				for (uint32_t i = 0; i < m_columnsCount; i++)
				{
					if (m_nameCompare.compare(m_columns[i].m_columnName.c_str(), columnName) == 0)
						return &m_columns[i];
				}
				return nullptr;
			}
			else
			{
				tableColMap::const_iterator iter = m_colsByName.find(columnName);
				if (iter == m_colsByName.end())
					return nullptr;
				return iter->second;
			}
		}

		inline int getUniqueKeyId(const char* UniqueKeyname)const
		{
			for (uint16_t i = 0; i < m_uniqueKeysCount; i++)
			{
				if (m_nameCompare.compare(m_uniqueKeyNames[i].c_str(), UniqueKeyname) == 0)
					return i;
			}
			return -1;
		}
		inline UnionKeyMeta* getUniqueKey(const char* UniqueKeyname)const
		{
			int id = getUniqueKeyId(UniqueKeyname);
			if (id > 0)
				return m_uniqueKeys[id];
			else
				return nullptr;
		}
		inline int getIndexId(const char* indexName)const
		{
			for (uint16_t i = 0; i < m_indexCount; i++)
			{
				if (m_nameCompare.compare(m_indexNames[i].c_str(), indexName) == 0)
					return i;
			}
			return -1;
		}
		inline UnionKeyMeta* getIndex(const char* indexName)const
		{
			int id = getIndexId(indexName);
			if (id > 0)
				return m_indexs[id];
			else
				return nullptr;
		}
		inline bool isMe(const char* database, const char* table) const
		{
			return m_nameCompare.compare(database, m_dbName.c_str()) == 0 && m_nameCompare.compare(table, m_tableName.c_str()) == 0;
		}
		UnionKeyMeta* createUnionKey(uint16_t keyId, KEY_TYPE keyType, const uint16_t* columnIds, uint16_t columnCount);

		DS prepareUnionKey(UnionKeyMeta* key);

		void buildColumnOffsetList();
		void updateKeysWhenColumnUpdate(int from, int to, COLUMN_TYPE newType);
		int dropColumn(uint32_t columnIndex);//todo ,update key;
		int dropColumn(const char* column);
		int renameColumn(const char* oldName, const char* newName);
		int modifyColumn(const ColumnMeta* column, bool first, const char* addAfter);
		int changeColumn(const ColumnMeta* newColumn, const char* columnName, bool first, const char* addAfter);
		int addColumn(const ColumnMeta* column, const char* addAfter = nullptr, bool first = false);
		int dropPrimaryKey();
		int createPrimaryKey(const std::list<std::string>& columns);
		int dropUniqueKey(const char* ukName);
		int _addIndex(uint16_t& count, UnionKeyMeta**& indexs, std::string*& indexNames, UnionKeyMeta* index, const char* indexName);
		int addIndex(const char* indexName, const std::list<std::string>& columns, KEY_TYPE keyType);
		int _dropIndex(int idx, uint16_t& indexCount, UnionKeyMeta**& indexs, std::string*& indexNames, KEY_TYPE keyType);
		int dropIndex(const char* indexName);
		int renameIndex(const char* oldName, const char* newName);
		int defaultCharset(const charsetInfo* charset, const char* collationName);
		int convertDefaultCharset(const charsetInfo* charset, const char* collationName);
		bool operator==(const TableMeta& dest)const;
		bool operator!=(const TableMeta& dest)const;
		std::string toString()const;
	};
}
#endif /* _METADATA_H_ */
