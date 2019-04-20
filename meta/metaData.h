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
namespace DATABASE_INCREASE {
	struct TableMetaMessage;
}
namespace META {

#define MAX_KEY_SIZE 2048
	struct stringArray
	{
		char ** m_array;
		uint32_t m_Count;
		stringArray() :m_array(NULL), m_Count(0) {}
		~stringArray()
		{
			clean();
		}
		void clean()
		{
			if (m_Count > 0)
			{
				for (uint32_t i = 0; i < m_Count; i++)
					free(m_array[i]);
				free(m_array);
			}
			m_Count = 0;
			m_array = NULL;

		}
		stringArray &operator =(const stringArray &c)
		{
			m_Count = c.m_Count;
			if (m_Count > 0)
			{
				for (uint32_t i = 0; i < m_Count; i++)
				{
					int32_t size = strlen(c.m_array[i]);
					m_array[i] = (char*)malloc(size + 1);
					memcpy(m_array[i], c.m_array[i], size);
					m_array[i][size] = '\0';
				}
			}
			else
				m_array = NULL;
			return *this;
		}
		stringArray &operator =(const std::list<std::string> &l)
		{
			clean();
			m_array = (char**)malloc(sizeof(char*)*l.size());
			for (std::list<std::string>::const_iterator iter = l.begin(); iter != l.end(); iter++)
			{
				m_array[m_Count] = (char*)malloc((*iter).size() + 1);
				memcpy(m_array[m_Count], (*iter).c_str(), (*iter).size());
				m_array[m_Count][(*iter).size()] = '\0';
			}
			return *this;
		}

	};
	struct columnMeta
	{
		uint8_t m_columnType; //type in DBStream
		uint8_t m_srcColumnType;// type in database
		uint16_t m_columnIndex;  //column id in table
		std::string m_columnName;
		const charsetInfo* m_charset;
		uint32_t m_size;
		uint32_t m_precision;
		uint32_t m_decimals;
		stringArray m_setAndEnumValueList;
		bool m_signed;
		bool m_isPrimary;
		bool m_isUnique;
		bool m_generated;
		columnMeta() :m_columnType(0), m_srcColumnType(0), m_columnIndex(0), m_size(0), m_precision(0), m_decimals(0),
			m_setAndEnumValueList(), m_signed(false), m_isPrimary(false), m_isUnique(false), m_generated(false)
		{}
		columnMeta &operator =(const columnMeta &c)
		{
			m_columnType = c.m_columnType;
			m_srcColumnType = c.m_srcColumnType;
			m_columnIndex = c.m_columnIndex;
			m_columnName = c.m_columnName;
			m_charset = c.m_charset;
			m_size = c.m_size;
			m_precision = c.m_precision;
			m_decimals = c.m_decimals;
			m_setAndEnumValueList = c.m_setAndEnumValueList;
			m_signed = c.m_signed;
			m_isPrimary = c.m_isPrimary;
			m_isUnique = c.m_isUnique;
			m_generated = c.m_generated;
			return *this;
		}
		std::string toString()
		{
			std::string sql("`");
			sql.append(m_columnName).append("` ");
			char numBuf[40] = { 0 };
			switch (m_srcColumnType)
			{
			case MYSQL_TYPE_DECIMAL:
				sql.append("DECIMAL").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case MYSQL_TYPE_DOUBLE:
				sql.append("DOUBLE").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case MYSQL_TYPE_FLOAT:
				sql.append("FLOAT").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case MYSQL_TYPE_BIT:
				sql.append("BIT").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(")");
				break;
			case MYSQL_TYPE_TINY:
				sql.append("TINY");
				if (!m_signed)
					sql.append(" UNSIGNED");
				break;
			case MYSQL_TYPE_SHORT:
				sql.append("SMALLINT");
				if (!m_signed)
					sql.append(" UNSIGNED");
				break;
			case MYSQL_TYPE_INT24:
				sql.append("MEDIUMINT");
				if (!m_signed)
					sql.append(" UNSIGNED");
				break;
			case MYSQL_TYPE_LONG:
				sql.append("INTEGER");
				if (!m_signed)
					sql.append(" UNSIGNED");
				break;
			case MYSQL_TYPE_LONGLONG:
				sql.append("BIGINT");
				if (!m_signed)
					sql.append(" UNSIGNED");
				break;
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_DATETIME2:
				sql.append("DATETIME");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_TIMESTAMP2:
				sql.append("TIMESTAMP");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case MYSQL_TYPE_DATE:
				sql.append("DATE");
				break;
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_TIME2:
				sql.append("TIME");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case MYSQL_TYPE_YEAR:
				sql.append("YEAR");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case MYSQL_TYPE_STRING:
				sprintf(numBuf, "%u", m_size);
				if (m_charset != nullptr)
				{
					sql.append("BINARY").append("(").append(numBuf).append(")");
				}
				else
				{
					sql.append("CHAR").append("(").append(numBuf).append(") CHARACTER SET ").append(m_charset->name);
				}
				break;
			case MYSQL_TYPE_VARCHAR:
				sql.append("VARCHAR").append("(").append(numBuf).append(") CHARACTER SET ").append(m_charset->name);
				break;
			case MYSQL_TYPE_VAR_STRING:
				sprintf(numBuf, "%u", m_size);
				if (m_charset != nullptr)
				{
					sql.append("VARBINARY").append("(").append(numBuf).append(")");
				}
				else
				{
					sql.append("VARCHAR").append("(").append(numBuf).append(") CHARACTER SET").append(m_charset->name);
				}
				break;
			case MYSQL_TYPE_TINY_BLOB:
				if (m_charset != nullptr)
					sql.append("TINYTEXT").append(" CHARACTER SET ").append(m_charset->name);
				else
					sql.append("TINYBLOB");
				break;
			case MYSQL_TYPE_MEDIUM_BLOB:
				if (m_charset != nullptr)
					sql.append("MEDIUMTEXT").append(" CHARACTER SET ").append(m_charset->name);
				else
					sql.append("MEDIUMBLOB");
				break;
			case MYSQL_TYPE_BLOB:
				if (m_charset != nullptr)
					sql.append("TEXT").append(" CHARACTER SET ").append(m_charset->name);
				else
					sql.append("BLOB");
				break;
			case MYSQL_TYPE_LONG_BLOB:
				if (m_charset != nullptr)
					sql.append("LONGTEXT").append(" CHARACTER SET ").append(m_charset->name);
				else
					sql.append("LONGBLOB");
				break;
			case MYSQL_TYPE_ENUM:
			{
				sql.append("ENUM (");
				for (uint32_t idx = 0; idx < m_setAndEnumValueList.m_Count; idx++)
				{
					if (idx > 0)
						sql.append(",");
					sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
				}
				sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
				break;
			}
			case MYSQL_TYPE_SET:
			{
				sql.append("SET (");
				for (uint32_t idx = 0; idx < m_setAndEnumValueList.m_Count; idx++)
				{
					if (idx > 0)
						sql.append(",");
					sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
				}
				sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
				break;
			}
			case MYSQL_TYPE_GEOMETRY:
				sql.append("GEOMETRY");
				break;
			case MYSQL_TYPE_JSON:
				sql.append("JSON");
				break;
			default:
				abort();
			}
			return sql;
		}
	};
	struct keyInfo
	{
		std::string name;
		uint16_t count;
		uint16_t *keyIndexs;
		keyInfo() :count(0), keyIndexs(nullptr) {}
		keyInfo(const keyInfo & key) :name(key.name), count(key.count), keyIndexs(nullptr) {
			if (count > 0)
			{
				keyIndexs = new uint16_t[count];
				memcpy(keyIndexs, key.keyIndexs, sizeof(uint16_t)*count);
			}
		}
		void init(const char* name, uint16_t count, const uint16_t *keyIndexs)
		{
			this->count = count;
			this->name = name;
			if (count > 0)
			{
				this->keyIndexs = new uint16_t[count];
				memcpy(this->keyIndexs, keyIndexs, sizeof(uint16_t)*count);
			}
			else
				this->keyIndexs = nullptr;
		}
		keyInfo& operator =(const keyInfo &key)
		{
			name = key.name;
			count = key.count;
			if (count > 0)
			{
				keyIndexs = new uint16_t[count];
				memcpy(keyIndexs, key.keyIndexs, sizeof(uint16_t)*count);
			}
			else
				keyIndexs = nullptr;
			return *this;
		}
		void clean()
		{
			if (keyIndexs)
			{
				delete[]keyIndexs;
				keyIndexs = nullptr;
			}
			count = 0;
		}
		~keyInfo()
		{
			clean();
		}
	};

	struct tableMeta
	{
		std::string  m_dbName;
		std::string  m_tableName;
		const charsetInfo *m_charset;
		columnMeta * m_columns;
		uint16_t * m_realIndexInRowFormat;
		uint16_t * m_fixedColumnOffsetsInRecord;
		uint16_t m_fixedColumnCount;
		uint16_t m_varColumnCount;
		uint16_t m_columnsCount;
		uint64_t m_id;
		keyInfo m_primaryKey;
		uint16_t m_uniqueKeysCount;
		keyInfo * m_uniqueKeys;
		uint16_t m_indexCount;
		keyInfo * m_indexs;
		void * userData;
		static inline uint16_t tableVersion(uint64_t tableIDInfo)
		{
			return tableIDInfo & 0xffff;
		}
		static inline uint64_t tableID(uint64_t tableIDInfo)
		{
			return tableIDInfo & 0xffffffffffff0000ul;
		}
		tableMeta();
		tableMeta(DATABASE_INCREASE::TableMetaMessage * msg);
		const char * createTableMetaRecord();
		void clean();
		~tableMeta();
		tableMeta &operator =(const tableMeta &t);
		inline columnMeta *getColumn(uint16_t idx)
		{
			if (idx > m_columnsCount)
				return nullptr;
			return &m_columns[idx];
		}
		inline columnMeta * getColumn(const char * columnName)
		{
			for (uint32_t i = 0; i < m_columnsCount; i++)
			{
				if (strcmp(m_columns[i].m_columnName.c_str(), columnName) == 0)
					return &m_columns[i];
			}
			return NULL;
		}
		inline keyInfo *getUniqueKey(const char *UniqueKeyname)
		{
			for (uint16_t i = 0; i < m_uniqueKeysCount; i++)
			{
				if (strcmp(m_uniqueKeys[i].name.c_str(), UniqueKeyname) == 0)
					return &m_uniqueKeys[i];
			}
			return NULL;
		}
		void buildColumnOffsetList();
		int dropColumn(uint32_t columnIndex);//todo ,update key;
		int dropColumn(const char *column);
		int addColumn(const columnMeta* column, const char * addAfter = NULL);
		int dropPrimaryKey();
		int createPrimaryKey(const std::list<std::string> &columns);
		int dropUniqueKey(const char *ukName);
		int addUniqueKey(const char *ukName, const std::list<std::string> &columns);
		std::string toString();
	};
}
#endif /* _METADATA_H_ */
