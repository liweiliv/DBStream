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
#include "nameCompare.h"
namespace DATABASE_INCREASE {
	struct TableMetaMessage;
}
namespace META {

#define MAX_KEY_SIZE 2048
	struct stringArray
	{
		char ** m_array;
		uint32_t m_count;
		stringArray() :m_array(nullptr), m_count(0) {}
		~stringArray()
		{
			clean();
		}
		void add(const char* str)
		{
			char** array = (char**)malloc(sizeof(char*) * (m_count + 1));
			memcpy(array, m_array, sizeof(char*) * m_count);
			array[m_count] = (char*)malloc(sizeof(str) + 1);
			strcpy(array[m_count], str);
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
		stringArray &operator =(const stringArray &c)
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
		stringArray &operator =(const std::list<std::string> &l)
		{
			clean();
			m_array = (char**)malloc(sizeof(char*)*l.size());
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
			return !(*this==(dest));
		}

	};
	struct columnMeta
	{
		COLUMN_TYPE m_columnType; //type in DBStream
		uint8_t m_srcColumnType;// type in database
		uint16_t m_columnIndex;  //column id in table
		std::string m_columnName;
		std::string m_collate;
		const charsetInfo* m_charset;
		uint32_t m_size;
		uint32_t m_precision;
		uint32_t m_decimals;
		stringArray m_setAndEnumValueList;
		bool m_signed;
		bool m_isPrimary;
		bool m_isUnique;
		bool m_isIndex;
		bool m_generated;
		columnMeta() :m_columnType(COLUMN_TYPE::T_MAX_TYPE), m_srcColumnType(0), m_columnIndex(0), m_charset(nullptr), m_size(0), m_precision(0), m_decimals(0),
			m_setAndEnumValueList(), m_signed(false), m_isPrimary(false), m_isUnique(false), m_isIndex(false), m_generated(false)
		{}
		columnMeta& operator =(const columnMeta& c)
		{
			m_columnType = c.m_columnType;
			m_srcColumnType = c.m_srcColumnType;
			m_columnIndex = c.m_columnIndex;
			m_columnName = c.m_columnName;
			m_charset = c.m_charset;
			m_collate = c.m_collate;
			m_size = c.m_size;
			m_precision = c.m_precision;
			m_decimals = c.m_decimals;
			m_setAndEnumValueList = c.m_setAndEnumValueList;
			m_signed = c.m_signed;
			m_isPrimary = c.m_isPrimary;
			m_isUnique = c.m_isUnique;
			m_isIndex = c.m_isIndex;
			m_generated = c.m_generated;
			return *this;
		}
		bool operator==(const columnMeta& c)const
		{
			if (m_columnType != c.m_columnType ||
				m_srcColumnType != c.m_srcColumnType ||
				m_columnIndex != c.m_columnIndex ||
				m_columnName != c.m_columnName ||
				m_collate != c.m_collate ||
				m_charset != c.m_charset ||
				m_size != c.m_size ||
				m_precision != c.m_precision ||
				m_decimals != c.m_decimals ||
				m_setAndEnumValueList != c.m_setAndEnumValueList ||
				m_signed != c.m_signed ||
				m_isPrimary != c.m_isPrimary ||
				m_isUnique != c.m_isUnique ||
				m_isIndex != c.m_isIndex ||
				m_generated != c.m_generated
				)
				return false;
			return true;
		}
		bool operator!=(const columnMeta& c)const
		{
			return !(*this == c);
		}
		std::string toString()const;
	};

	struct DLL_EXPORT tableMeta
	{
		std::string  m_dbName;
		std::string  m_tableName;
		const charsetInfo *m_charset;
		std::string m_collate;
		columnMeta * m_columns;
		uint16_t * m_realIndexInRowFormat;
		uint16_t * m_fixedColumnOffsetsInRecord;
		uint16_t m_fixedColumnCount;
		uint16_t m_varColumnCount;
		uint16_t m_columnsCount;
		uint64_t m_id;
		unionKeyMeta* m_primaryKey;
		uint16_t m_uniqueKeysCount;
		unionKeyMeta** m_uniqueKeys;
		std::string* m_uniqueKeyNames;
		uint16_t m_indexCount;
		unionKeyMeta** m_indexs;
		std::string* m_indexNames;
		nameCompare m_nameCompare;
		void * userData;
		constexpr static inline uint16_t tableVersion(uint64_t tableIDInfo)
		{
			return tableIDInfo & 0xffff;
		}
		constexpr static inline uint64_t tableID(uint64_t tableIDInfo)
		{
			return (tableIDInfo & 0xffffffffffff0000ul)>>16;
		}
		constexpr static inline uint64_t genTableId(uint64_t tableid,uint16_t version)
		{
			return (tableid<<16)|version;
		}
		tableMeta(bool caseSensitive);
		tableMeta(DATABASE_INCREASE::TableMetaMessage * msg);
		const char * createTableMetaRecord()const;
		void clean();
		~tableMeta();
		tableMeta &operator =(const tableMeta &t);
		inline const columnMeta *getColumn(uint16_t idx) const
		{
			if (idx > m_columnsCount)
				return nullptr;
			return &m_columns[idx];
		}

		inline const columnMeta * getColumn(const char * columnName) const
		{
			for (uint32_t i = 0; i < m_columnsCount; i++)
			{
				if (m_nameCompare.compare(m_columns[i].m_columnName.c_str(), columnName) == 0)
					return &m_columns[i];
			}
			return nullptr;
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
		inline unionKeyMeta *getUniqueKey(const char *UniqueKeyname)const 
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
		inline unionKeyMeta* getIndex(const char* indexName)const
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
		unionKeyMeta* createUnionKey(uint16_t keyId, KEY_TYPE keyType, const uint16_t* columnIds, uint16_t columnCount);
		void buildColumnOffsetList();
		void updateKeysWhenColumnUpdate(int from, int to, COLUMN_TYPE newType);
		int dropColumn(uint32_t columnIndex);//todo ,update key;
		int dropColumn(const char *column);
		int renameColumn(const char* oldName, const char* newName);
		int modifyColumn(const columnMeta* column, bool first, const char* addAfter);
		int changeColumn(const columnMeta* newColumn,const char * columnName, bool first, const char* addAfter);
		int addColumn(const columnMeta* column, const char * addAfter = nullptr,bool first = false);
		int dropPrimaryKey();
		int createPrimaryKey(const std::list<std::string> &columns);
		int dropUniqueKey(const char *ukName);
		int _addIndex(uint16_t &count,unionKeyMeta** &indexs,std::string*& indexNames,unionKeyMeta*index,const char* indexName);
		int addIndex(const char* indexName, const std::list<std::string>& columns,KEY_TYPE keyType);
		int _dropIndex(int idx,uint16_t& indexCount,unionKeyMeta** &indexs,std::string*& indexNames,KEY_TYPE keyType);
		int dropIndex(const char* indexName);
		int renameIndex(const char* oldName, const char* newName);
		int defaultCharset(const charsetInfo* charset, const char* collationName);
		int convertDefaultCharset(const charsetInfo* charset, const char* collationName);
		bool operator==(const tableMeta& dest)const;
		bool operator!=(const tableMeta& dest)const;
		std::string toString()const;
	};
}
#endif /* _METADATA_H_ */
