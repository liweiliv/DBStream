#include "metaData.h"
#include "message/record.h"
#include "glog/logging.h"
namespace META {
	std::string columnMeta::toString()const
	{
			std::string sql("`");
			sql.append(m_columnName).append("` ");
			char numBuf[40] = { 0 };
			switch (m_columnType)
			{
			case COLUMN_TYPE::T_DECIMAL:
				sql.append("DECIMAL").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case COLUMN_TYPE::T_DOUBLE:
				sql.append("DOUBLE").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case COLUMN_TYPE::T_FLOAT:
				sql.append("FLOAT").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(",");
				sprintf(numBuf, "%u", m_decimals);
				sql.append(numBuf).append(")");
				break;
			case COLUMN_TYPE::T_BYTE:
				sql.append("BIT").append("(");
				sprintf(numBuf, "%u", m_size);
				sql.append(numBuf).append(")");
				break;
			case COLUMN_TYPE::T_UINT8:
				sql.append("TINY UNSIGNED");
				break;
			case COLUMN_TYPE::T_INT8:
				sql.append("TINY");
				break;
			case COLUMN_TYPE::T_UINT16:
				sql.append("SMALLINT UNSIGNED");
				break;
			case COLUMN_TYPE::T_INT16:
				sql.append("SMALLINT");
				break;
			case COLUMN_TYPE::T_UINT32:
				sql.append("INT UNSIGNED");
				break;
			case COLUMN_TYPE::T_INT32:
				sql.append("INT");
				break;
			case COLUMN_TYPE::T_UINT64:
				sql.append("BIGINT UNSIGNED");
				break;
			case COLUMN_TYPE::T_INT64:
				sql.append("BIGINT");
				break;
			case COLUMN_TYPE::T_DATETIME:
				sql.append("DATETIME");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case COLUMN_TYPE::T_TIMESTAMP:
				sql.append("TIMESTAMP");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case COLUMN_TYPE::T_DATE:
				sql.append("DATE");
				break;
			case COLUMN_TYPE::T_TIME:
				sql.append("TIME");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case COLUMN_TYPE::T_YEAR:
				sql.append("YEAR");
				if (m_precision > 0)
				{
					sprintf(numBuf, "%u", m_precision);
					sql.append("(").append(numBuf).append(")");
				}
				break;
			case COLUMN_TYPE::T_STRING:
				sprintf(numBuf, "%u", m_size / m_charset->byteSizePerChar);
				sql.append("CHAR").append("(").append(numBuf).append(") CHARACTER SET ").append(m_charset->name);
				break;
			case COLUMN_TYPE::T_TEXT:
				sql.append("TEXT").append(" CHARACTER SET ").append(m_charset->name);
				break;
			case COLUMN_TYPE::T_BLOB:
				sql.append("BLOB");
				break;
			case COLUMN_TYPE::T_ENUM:
			{
				sql.append("ENUM (");
				for (uint32_t idx = 0; idx < m_setAndEnumValueList.m_count; idx++)
				{
					if (idx > 0)
						sql.append(",");
					sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
				}
				sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
				break;
			}
			case COLUMN_TYPE::T_SET:
			{
				sql.append("SET (");
				for (uint32_t idx = 0; idx < m_setAndEnumValueList.m_count; idx++)
				{
					if (idx > 0)
						sql.append(",");
					sql.append("'").append(m_setAndEnumValueList.m_array[idx]).append("'");
				}
				sql.append(")").append(" CHARACTER SET ").append(m_charset->name);
				break;
			}
			case COLUMN_TYPE::T_GEOMETRY:
				sql.append("GEOMETRY");
				break;
			case COLUMN_TYPE::T_JSON:
				sql.append("JSON");
				break;
			default:
				abort();
			}
			return sql;
		
	}
	tableMeta::tableMeta(bool caseSensitive) :m_charset(nullptr), m_columns(nullptr),  m_realIndexInRowFormat(nullptr), m_fixedColumnOffsetsInRecord(nullptr),m_fixedColumnCount(0), m_varColumnCount(0),
		m_columnsCount(0), m_id(0), m_primaryKey(nullptr), m_uniqueKeysCount(0),m_uniqueKeys(nullptr), m_uniqueKeyNames(nullptr), m_indexCount(0), m_indexs(nullptr),m_indexNames(nullptr), m_nameCompare(caseSensitive),userData(nullptr)
	{
	}
	tableMeta::tableMeta(DATABASE_INCREASE::TableMetaMessage * msg) :m_dbName(msg->database ? msg->database : ""), m_tableName(msg->table ? msg->table : ""),
			m_charset(&charsets[msg->metaHead.charsetId]), m_realIndexInRowFormat(nullptr), m_fixedColumnOffsetsInRecord(nullptr),m_fixedColumnCount(0), m_varColumnCount(0),
			 m_columnsCount(msg->metaHead.columnCount),
		m_id(msg->metaHead.tableMetaID), m_primaryKey(nullptr),m_uniqueKeysCount(msg->metaHead.uniqueKeyCount), m_uniqueKeys(nullptr), m_uniqueKeyNames(nullptr), m_indexCount(0), m_indexs(nullptr), m_indexNames(nullptr), m_nameCompare(msg->metaHead.caseSensitive>0)
	{
		m_columns = new columnMeta[m_columnsCount];
		for (uint32_t i = 0; i < m_columnsCount; i++)
		{
			m_columns[i].m_columnIndex = i;
			m_columns[i].m_columnName = msg->columnName(i);
			if (msg->columns[i].charsetID < MAX_CHARSET)
				m_columns[i].m_charset = &charsets[msg->columns[i].charsetID];
			else
				m_columns[i].m_charset = nullptr;
			m_columns[i].m_columnType = static_cast<COLUMN_TYPE>(msg->columns[i].type);
			m_columns[i].m_srcColumnType = msg->columns[i].srcType;
			m_columns[i].m_decimals = msg->columns[i].decimals;
			m_columns[i].m_precision = msg->columns[i].precision;
			m_columns[i].m_generated = msg->columns[i].flag&COLUMN_FLAG_VIRTUAL;
			m_columns[i].m_signed = msg->columns[i].flag&COLUMN_FLAG_SIGNED;
			m_columns[i].m_size = msg->columns[i].size;
			if (msg->columns[i].setOrEnumInfoOffset != 0)
			{
				const char * base;
				uint16_t *valueList, valueListSize;
				msg->setOrEnumValues(i, base, valueList, valueListSize);
				m_columns[i].m_setAndEnumValueList.m_count = valueListSize;
				m_columns[i].m_setAndEnumValueList.m_array = (char**)malloc(sizeof(char*)*valueListSize);
				for (uint16_t j = 0; j < valueListSize; j++)
				{
					m_columns[i].m_setAndEnumValueList.m_array[j] = (char*)malloc(valueList[j + 1] - valueList[j]);
					memcpy(m_columns[i].m_setAndEnumValueList.m_array[j], base + valueList[j], valueList[j + 1] - valueList[j]);
				}
			}
			m_columns[i].m_isPrimary = false;
			m_columns[i].m_isUnique = false;
		}

		buildColumnOffsetList();
		if (msg->metaHead.primaryKeyColumnCount > 0)
		{
			m_primaryKey = createUnionKey(0, KEY_TYPE::PRIMARY_KEY, msg->primaryKeys, msg->metaHead.primaryKeyColumnCount);
			for (uint16_t i = 0; i < m_primaryKey->columnCount; i++)
				((columnMeta*)getColumn(m_primaryKey->columnInfo[i].columnId))->m_isPrimary = true;
		}
		if (msg->metaHead.uniqueKeyCount > 0)
		{
			m_uniqueKeys =   (unionKeyMeta**)malloc(sizeof(unionKeyMeta*)*msg->metaHead.uniqueKeyCount);
			m_uniqueKeyNames = new std::string[msg->metaHead.uniqueKeyCount];
			for (uint16_t i = 0; i < msg->metaHead.uniqueKeyCount; i++)
			{
				m_uniqueKeys[i] = createUnionKey(i, KEY_TYPE::UNIQUE_KEY, msg->uniqueKeys[i], msg->uniqueKeyColumnCounts[i]);
				m_uniqueKeyNames[i].assign(msg->data + msg->uniqueKeyNameOffset[i]);
				for (uint16_t j = 0; j < m_uniqueKeys[i]->columnCount; j++)
					((columnMeta*)getColumn(m_uniqueKeys[i]->columnInfo[j].columnId))->m_isUnique = true;
			}
			m_uniqueKeysCount = msg->metaHead.uniqueKeyCount;
		}
	}
	const char * tableMeta::createTableMetaRecord()const
	{

		return nullptr;//todo
	}
	void tableMeta::clean()
	{
		m_dbName.clear();
		m_tableName.clear();
		m_charset = nullptr;
		m_columnsCount = 0;
		m_fixedColumnCount = 0;
		m_varColumnCount = 0;
		m_id = 0;
		m_indexCount = 0;
		m_indexCount = 0;
		if (m_primaryKey != nullptr)
		{
			free(m_primaryKey);
			m_primaryKey = nullptr;
		}
		if (m_columns)
		{
			delete[]m_columns;
			m_columns = nullptr;
		}
		if (m_uniqueKeys != nullptr)
		{
			for (int i = 0; i < m_uniqueKeysCount; i++)
				free(m_uniqueKeys[i]);
			free(m_uniqueKeys);
			m_uniqueKeys = nullptr;
		}
		if (m_uniqueKeyNames != nullptr)
		{
			delete[]m_uniqueKeyNames;
			m_uniqueKeyNames = nullptr;
		}
		if (m_indexs != nullptr)
		{
			for (int i = 0; i < m_indexCount; i++)
				free(m_indexs[i]);
			free(m_indexs);
			m_indexs = nullptr;
		}
		if (m_indexNames != nullptr)
		{
			delete[]m_indexNames;
			m_indexNames = nullptr;
		}
		if (m_realIndexInRowFormat)
		{
			delete[] m_realIndexInRowFormat;
			m_realIndexInRowFormat = nullptr;
		}
		if (m_fixedColumnOffsetsInRecord)
		{
			delete[]m_fixedColumnOffsetsInRecord;
			m_fixedColumnOffsetsInRecord = nullptr;
		}

	}
	tableMeta::~tableMeta()
	{
		clean();
	}
	tableMeta &tableMeta::operator =(const tableMeta &t)
	{
		clean();
		m_tableName = t.m_tableName;
		m_dbName = t.m_dbName;
		m_charset = t.m_charset;
		m_nameCompare = t.m_nameCompare;
		if ((m_columnsCount = t.m_columnsCount) > 0)
		{
			m_columns = new columnMeta[m_columnsCount];
			for (uint32_t i = 0; i < m_columnsCount; i++)
				m_columns[i] = t.m_columns[i];
		}

		m_id = t.m_id;
		/*copy primary key*/
		if (t.m_primaryKey != nullptr)
		{
			m_primaryKey = (unionKeyMeta*)malloc(unionKeyMeta::memSize(t.m_primaryKey->columnCount));
			*m_primaryKey = *t.m_primaryKey;
		}
		/*copy unique key*/
		m_uniqueKeysCount = t.m_uniqueKeysCount;
		if (t.m_uniqueKeys != nullptr)
		{
			m_uniqueKeys = (unionKeyMeta**)malloc(sizeof(unionKeyMeta*)* t.m_uniqueKeysCount);
			m_uniqueKeyNames = new std::string[t.m_uniqueKeysCount];
			for (int i = 0; i < m_uniqueKeysCount; i++)
			{
				m_uniqueKeys[i] = (unionKeyMeta*)malloc(unionKeyMeta::memSize(t.m_uniqueKeys[i]->columnCount));
				*m_uniqueKeys[i] = *t.m_uniqueKeys[i];
				m_uniqueKeyNames[i] = t.m_uniqueKeyNames[i];
			}
		}
		m_indexCount = t.m_indexCount;
		if (t.m_indexs != nullptr)
		{
			m_indexNames = new std::string[t.m_indexCount];
			m_indexs = (unionKeyMeta**)malloc(sizeof(unionKeyMeta*) * t.m_indexCount);
			for (int i = 0; i < m_indexCount; i++)
			{
				m_indexs[i] = (unionKeyMeta*)malloc(unionKeyMeta::memSize(t.m_indexs[i]->columnCount));
				*m_indexs[i] = *t.m_indexs[i];
				m_indexNames[i] = t.m_indexNames[i];
			}
		}
		m_fixedColumnCount = t.m_fixedColumnCount;
		m_varColumnCount = t.m_varColumnCount;
		m_realIndexInRowFormat = new uint16_t[m_columnsCount];
		memcpy(m_realIndexInRowFormat,t.m_realIndexInRowFormat,sizeof(uint16_t)*m_columnsCount);
		m_fixedColumnOffsetsInRecord = new uint16_t[m_fixedColumnCount+1];
		memcpy(m_fixedColumnOffsetsInRecord,t.m_fixedColumnOffsetsInRecord,sizeof(uint16_t)*(m_fixedColumnCount+1));
		
		return *this;
	}
	unionKeyMeta* tableMeta::createUnionKey(uint16_t keyId, KEY_TYPE keyType, const uint16_t* columnIds, uint16_t columnCount)
	{
		unionKeyMeta* uk = (unionKeyMeta*)malloc(unionKeyMeta::memSize(columnCount));
		uk->columnCount = columnCount;
		uk->keyType = TID(keyType);
		uk->keyId = keyId;
		uk->fixed = 1;
		uk->size = 0;
		uk->varColumnCount = 0;
		for (int i = 0; i < columnCount; i++)
		{
			if (columnIds[i] >= m_columnsCount|| !columnInfos[TID(m_columns[columnIds[i]].m_columnType)].asIndex)
			{
				LOG(ERROR) << "create key failed for column is not in column list or column type can not used as index";
				free(uk);
				return nullptr;
			}
			uk->columnInfo[i].columnId = columnIds[i];
			uk->columnInfo[i].type = TID(m_columns[columnIds[i]].m_columnType);
			if (!columnInfos[uk->columnInfo[i].type].fixed)
			{
				uk->varColumnCount++;
				uk->size += sizeof(uint16_t);
				uk->fixed = 0;
			}
			else
				uk->size += columnInfos[uk->columnInfo[i].type].columnTypeSize;
		}
		return uk;
	}

	void tableMeta::buildColumnOffsetList()
	{
		m_fixedColumnCount = m_varColumnCount = 0;
		for (uint16_t i = 0; i < m_columnsCount; i++)
		{
			if (columnInfos[static_cast<int>(m_columns[i].m_columnType)].fixed)
				m_fixedColumnCount++;
			else
				m_varColumnCount++;
		}
		if (m_realIndexInRowFormat)
			delete[] m_realIndexInRowFormat;
		m_realIndexInRowFormat = new uint16_t[m_columnsCount];
		if (m_fixedColumnOffsetsInRecord)
			delete[]m_fixedColumnOffsetsInRecord;
		m_fixedColumnOffsetsInRecord = new uint16_t[m_fixedColumnCount+1];
		m_fixedColumnCount = m_varColumnCount = 0;
		uint32_t fixedOffset = 0;
		for (uint16_t i = 0; i < m_columnsCount; i++)
		{
			if (columnInfos[static_cast<int>(m_columns[i].m_columnType)].fixed)
			{
				m_fixedColumnOffsetsInRecord[m_fixedColumnCount] = fixedOffset;
				fixedOffset += columnInfos[static_cast<int>(m_columns[i].m_columnType)].columnTypeSize;
				m_realIndexInRowFormat[i] = m_fixedColumnCount++;
			}
			else
			{
				m_realIndexInRowFormat[i] = m_varColumnCount++;
			}
		}
		m_fixedColumnOffsetsInRecord[m_fixedColumnCount] = fixedOffset;
	}
	void tableMeta::updateKeysWhenColumnUpdate(int from, int to, COLUMN_TYPE newType)
	{
		if (m_primaryKey != nullptr)
			m_primaryKey->columnUpdate(from, to, newType);
		for(int i=0;i<m_uniqueKeysCount;i++)
			m_uniqueKeys[i]->columnUpdate(from, to, newType);
		for (int i = 0; i < m_indexCount; i++)
			m_indexs[i]->columnUpdate(from, to, newType);
	}

	int tableMeta::dropColumn(uint32_t columnIndex)//todo ,update key
	{
		if (columnIndex >= m_columnsCount)
			return -1;
		columnMeta * columns = new columnMeta[m_columnsCount - 1];
		for (uint32_t idx = 0; idx < columnIndex; idx++)
			columns[idx] = m_columns[idx];
		for (uint32_t idx = m_columnsCount - 1; idx > columnIndex; idx--)
		{
			columns[idx - 1] = m_columns[idx];
			columns[idx - 1].m_columnIndex--;
		}
		if (m_uniqueKeys != nullptr)
		{
			for (uint16_t idx = 0; idx < m_uniqueKeysCount; idx++)
			{
				for (uint16_t i = 0; i < m_uniqueKeys[idx]->columnCount;)
				{
					if (m_uniqueKeys[idx]->columnInfo[i].columnId > columnIndex)
						m_uniqueKeys[idx]->columnInfo[i].columnId--;
					else if (m_uniqueKeys[idx]->columnInfo[i].columnId == columnIndex)
					{
						if (!columnInfos[m_uniqueKeys[idx]->columnInfo[i].type].fixed)
							m_uniqueKeys[idx]->varColumnCount--;
						memcpy(&m_uniqueKeys[idx]->columnInfo[i].columnId, &m_uniqueKeys[idx]->columnInfo[i + 1], sizeof(uniqueKeyTypePair)*(m_uniqueKeys[idx]->columnCount - i - 1));
						m_uniqueKeys[idx]->columnCount--;

						continue;//do not do [i++]
					}
					i++;
				}
				if (m_uniqueKeys[idx]->columnCount == 0)
				{
					dropUniqueKey(m_uniqueKeyNames[idx].c_str());
				}
			}
		}
		if (m_primaryKey != nullptr)
		{
			for (uint16_t i = 0; i < m_primaryKey->columnCount;)
			{
				if (m_primaryKey->columnInfo[i].columnId > columnIndex)
					m_primaryKey->columnInfo[i].columnId--;
				else if (m_primaryKey->columnInfo[i].columnId == columnIndex)
				{
					memcpy(&m_primaryKey->columnInfo[i], &m_primaryKey->columnInfo[i + 1], sizeof(uniqueKeyTypePair) * (m_primaryKey->columnCount - i - 1));
					m_primaryKey->columnCount--;
					if (!columnInfos[m_primaryKey->columnInfo[i].type].fixed)
						m_primaryKey->varColumnCount--;
					continue;//do not do [i++]
				}
				i++;
			}
			if (m_primaryKey->columnCount == 0)
				dropPrimaryKey();
		}
		delete[]m_columns;
		m_columns = columns;
		m_columnsCount--;
		buildColumnOffsetList();
		return 0;
	}
	int tableMeta::dropColumn(const char *column)
	{
		const columnMeta * d = getColumn(column);
		if (d == nullptr)
		{
			LOG(ERROR) << "drop column " << column << " failed for column not exist";
			return -1;
		}
		return dropColumn(d->m_columnIndex);
	}
	int tableMeta::renameColumn(const char* oldName, const char* newName)
	{
		const columnMeta* c = getColumn(oldName);
		if (c == nullptr)
		{
			LOG(ERROR) << "rename column " << oldName << " failed for column not exist";
			return -1;
		}
		if (m_nameCompare.compare(oldName, newName) != 0&& getColumn(newName) != nullptr)
		{
			LOG(ERROR) << "rename column " << oldName << " failed for new column name " << newName << " exist";
			return -1;
		}
		m_columns[c->m_columnIndex].m_columnName.assign(newName);
		return 0;
	}
	int tableMeta::modifyColumn(const columnMeta* column,bool first, const char* addAfter)
	{
		const columnMeta* old = getColumn(column->m_columnName.c_str());
		if (old == nullptr)
		{
			LOG(ERROR) << "modify column " << column->m_columnName << " failed for column not exist";
			return -1;
		}
		int idx = old->m_columnIndex;

		bool isPk = old->m_isPrimary,isUk = old->m_isUnique,isIndex = old->m_isIndex;
		if(isPk||isUk||isIndex)
		{
			if(!columnInfos[static_cast<int>(column->m_columnType)].asIndex)
			{
				LOG(ERROR) << "modify column " << column->m_columnName << " failed for new column type " << static_cast<int>(column->m_columnType) << " can not use as index";
				return -1;
			}
		}

		if ((!first && addAfter == nullptr)||(first&& idx==0))
		{
			m_columns[idx] = *column;
			m_columns[idx].m_columnIndex = idx;
			m_columns[idx].m_isPrimary = isPk;
			m_columns[idx].m_isUnique = isUk;
			m_columns[idx].m_isIndex = isIndex;
			if (columnInfos[static_cast<int>(m_columns[idx].m_columnType)].stringType&& m_columns[idx].m_charset == nullptr)
			{
				m_columns[idx].m_charset = m_charset;
				if (m_columns[idx].m_size > 0)
					m_columns[idx].m_size *= m_charset->byteSizePerChar;
			}
			updateKeysWhenColumnUpdate(idx, idx, column->m_columnType);
			buildColumnOffsetList();
		}
		else
		{
			int to = idx;
			if (first)
				to = 0;
			else if (addAfter != nullptr)
			{
				const columnMeta* after = getColumn(addAfter);
				if (after == nullptr)
				{
					LOG(ERROR) << "modify column " << column->m_columnName << " failed for after column :"<< addAfter <<" not exist";
					return -1;
				}
				to = after->m_columnIndex;
			}
			if (idx < to)
			{
				for (int i = idx + 1; i <=to; i++)
				{
					m_columns[i-1] = m_columns[i];
					m_columns[i-1].m_columnIndex = i-1;
				}
			}
			else
			{
				for (int i = idx - 1; i >=to; i--)
				{
					m_columns[i+1] = m_columns[i];
					m_columns[i+1].m_columnIndex = i+1;

				}
			}
			m_columns[to] = *column;
			m_columns[to].m_columnIndex = to;
			m_columns[to].m_isPrimary = isPk;
			m_columns[to].m_isUnique = isUk;
			m_columns[to].m_isIndex = isIndex;
			if (columnInfos[static_cast<int>(m_columns[to].m_columnType)].stringType&& m_columns[to].m_charset == nullptr)
			{
				m_columns[to].m_charset = m_charset;
				if (m_columns[to].m_size > 0)
					m_columns[to].m_size *= m_charset->byteSizePerChar;
			}
			updateKeysWhenColumnUpdate(idx, to, column->m_columnType);
			buildColumnOffsetList();
		}
		return 0;
	}
	int tableMeta::changeColumn(const columnMeta* newColumn, const char* columnName, bool first, const char* addAfter)
	{
		const columnMeta* old = getColumn(columnName);
		if (old == nullptr)
		{
			LOG(ERROR) << "change column " << columnName << " failed for column not exist";
			return -1;
		}
		if (m_nameCompare.compare(columnName, newColumn->m_columnName.c_str()) != 0&& getColumn(newColumn->m_columnName.c_str()) != nullptr)
		{
			LOG(ERROR) << "change column " << columnName << " failed for new column name " << newColumn->m_columnName << " exist";
			return -1;
		}
		int idx = old->m_columnIndex;
		bool isPk = old->m_isPrimary,isUk = old->m_isUnique,isIndex = old->m_isIndex;
		if(isPk||isUk||isIndex)
		{
			if(!columnInfos[static_cast<int>(newColumn->m_columnType)].asIndex)
			{
				LOG(ERROR) << "change column " << newColumn->m_columnName << " failed for new column type " << static_cast<int>(newColumn->m_columnType) << " can not use as index";
				return -1;
			}
		}
		if ((!first && addAfter == nullptr) || (first && idx == 0))
		{
			m_columns[idx] = *newColumn;
			m_columns[idx].m_columnIndex = idx;
			m_columns[idx].m_isPrimary = isPk;
			m_columns[idx].m_isUnique = isUk;
			m_columns[idx].m_isIndex = isIndex;
			if (columnInfos[static_cast<int>(m_columns[idx].m_columnType)].stringType&& m_columns[idx].m_charset == nullptr)
			{
				m_columns[idx].m_charset = m_charset;
				if (m_columns[idx].m_size > 0)
					m_columns[idx].m_size *= m_charset->byteSizePerChar;
			}
			updateKeysWhenColumnUpdate(idx, idx, newColumn->m_columnType);
			buildColumnOffsetList();
		}
		else
		{
			int to = idx;
			if (first)
				to = 0;
			else if (addAfter != nullptr)
			{
				const columnMeta* after = getColumn(addAfter);
				if (after == nullptr)
				{
					LOG(ERROR) << "change column " << newColumn->m_columnName << " failed for after column :" << addAfter << " not exist";
					return -1;
				}
				to = after->m_columnIndex;
			}
			if (idx < to)
			{
				for (int i = idx + 1; i <= to; i++)
				{
					m_columns[i - 1] = m_columns[i];
					m_columns[i - 1].m_columnIndex = i - 1;
				}
			}
			else
			{
				for (int i = idx - 1; i >= to; i--)
				{
					m_columns[i + 1] = m_columns[i];
					m_columns[i + 1].m_columnIndex = i + 1;
				}
			}
			m_columns[to] = *newColumn;
			m_columns[to].m_columnIndex = to;
			m_columns[to].m_isPrimary = isPk;
			m_columns[to].m_isUnique = isUk;
			m_columns[to].m_isIndex = isIndex;
			if (columnInfos[static_cast<int>(m_columns[to].m_columnType)].stringType&& m_columns[to].m_charset == nullptr)
			{
				m_columns[to].m_charset = m_charset;
				if (m_columns[to].m_size > 0)
					m_columns[to].m_size *= m_charset->byteSizePerChar;
			}
			updateKeysWhenColumnUpdate(idx, to, newColumn->m_columnType);
			buildColumnOffsetList();
		}
		return 0;
	}
	int tableMeta::addColumn(const columnMeta* column,const char * addAfter, bool first)
	{
		if (getColumn(column->m_columnName.c_str()) != nullptr)
		{
			LOG(ERROR) << "add column "<< column->m_columnName <<" failed for column exist";
			return -1;
		}
		uint16_t idx = 0;
		if (addAfter != nullptr)
		{
			const columnMeta* after = getColumn(addAfter);
			if (after == nullptr)
			{
				LOG(ERROR) << "add column " << column->m_columnName << " failed for after column "<< addAfter <<" not exist";
				return -1;
			}
			idx = after->m_columnIndex;
		}
		else if (!first)
			idx = m_columnsCount;
		columnMeta* newColumns = new columnMeta[m_columnsCount + 1];
		for (uint16_t i = idx; i < m_columnsCount; i++)
		{
			newColumns[i + 1] = m_columns[i];
			newColumns[i + 1].m_columnIndex++;
		}
		for(uint16_t i = 0; i < idx; i++)
			newColumns[i] = m_columns[i];
		newColumns[idx] = *column;
		newColumns[idx].m_columnIndex = idx;
		if (columnInfos[static_cast<int>(newColumns[idx].m_columnType)].stringType&& newColumns[idx].m_charset == nullptr)
		{
			newColumns[idx].m_charset = m_charset;
			if (newColumns[idx].m_size > 0)
				newColumns[idx].m_size *= m_charset->byteSizePerChar;
		}
		delete[]m_columns;
		m_columns = newColumns;
		m_columnsCount++;
		if(idx!=m_columnsCount-1)
			updateKeysWhenColumnUpdate(idx, idx, column->m_columnType);
		buildColumnOffsetList();
		return 0;
	}
	int tableMeta::dropPrimaryKey()
	{
		if (m_primaryKey != nullptr)
		{
			for (uint16_t i = 0; i < m_primaryKey->columnCount; i++)
				((columnMeta*)getColumn(m_primaryKey->columnInfo[i].columnId))->m_isPrimary = false;
			free(m_primaryKey);
			m_primaryKey = nullptr;
		}
		return 0;
	}
	int tableMeta::createPrimaryKey(const std::list<std::string> &columns)
	{
		if (columns.size() >= 256 || columns.empty())
		{
			LOG(ERROR) << "create primary key failed column count "<< columns.size() <<" is illegal";
			return -1;
		}
		if (m_primaryKey != nullptr)
		{
			LOG(ERROR) << "create primary key failed for primary key exist";
			return -1;
		}
		uint16_t columnIds[256];
		uint16_t columnCount = 0;
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta* c = (columnMeta*)getColumn((*iter).c_str());
			if (c == nullptr)
			{
				LOG(ERROR) << "create primary key failed for column " << (*iter) << " not exist";
				return -1;
			}
			columnIds[columnCount++] = c->m_columnIndex;
		}
		m_primaryKey = createUnionKey(0, KEY_TYPE::PRIMARY_KEY, columnIds, columnCount);
		if (m_primaryKey != nullptr)
		{
			for (int i = 0; i < columnCount; i++)
				m_columns[columnIds[i]].m_isPrimary = true;
			return 0;
		}
		else
			return -1;
	}
	int tableMeta::dropUniqueKey(const char *ukName)
	{
		int idx = 0;
		for (; idx < m_uniqueKeysCount; idx++)
		{
			if (m_nameCompare.compare(m_uniqueKeyNames[idx].c_str(), ukName) == 0)
				goto DROP;
		}
		LOG(ERROR) << "drop unique key " << ukName << " failed for unique key not exist";
		return -1;

	DROP:
		if (m_uniqueKeysCount == 1)
		{
			for (uint16_t i = 0; i < m_uniqueKeys[idx]->columnCount; i++)
				((columnMeta*)getColumn(m_uniqueKeys[idx]->columnInfo[i].columnId))->m_isUnique = false;
			free(m_uniqueKeys[0]);
			free(m_uniqueKeys);
			delete[]m_uniqueKeyNames;
			m_uniqueKeysCount = 0;
			return 0;
		}

		unionKeyMeta** newUks = (unionKeyMeta**)malloc(sizeof(unionKeyMeta*)*(m_uniqueKeysCount - 1));
		std::string* newUkNames = new std::string[m_uniqueKeysCount - 1];
		for (int i = 0; i < idx; i++)
		{
			newUks[i] = m_uniqueKeys[i];
			newUkNames[i] = m_uniqueKeyNames[i];
		}
		for (int i = idx + 1; i < m_uniqueKeysCount - 1; i++)
		{
			newUks[i - 1] = m_uniqueKeys[i];
			newUkNames[i - 1] = m_uniqueKeyNames[i];
			newUks[i - 1]->keyId--;
		}

		/*update columns */
		for (uint16_t i = 0; i < m_uniqueKeys[idx]->columnCount; i++)
		{
			for (uint16_t j = 0; j < m_uniqueKeysCount - 1; j++)
			{
				for (uint32_t k = 0; k < newUks[j]->columnCount; k++)
				{
					if (m_uniqueKeys[idx]->columnInfo[i].columnId == newUks[j]->columnInfo[k].columnId)
						goto COLUMN_IS_STILL_UK;
				}
			}
			((columnMeta*)getColumn(m_uniqueKeys[idx]->columnInfo[i].columnId))->m_isUnique = false;
COLUMN_IS_STILL_UK:
			continue;
		}
		free(m_uniqueKeys[idx]);
		free(m_uniqueKeys);
		delete[]m_uniqueKeyNames;
		m_uniqueKeys = newUks;
		m_uniqueKeyNames = newUkNames;
		m_uniqueKeysCount--;
		return 0;
	}

	int tableMeta::_addIndex(uint16_t &count,unionKeyMeta** &indexs,std::string*& indexNames,unionKeyMeta*index,const char* indexName)
	{
		unionKeyMeta** newIndexs = (unionKeyMeta**)malloc(sizeof(unionKeyMeta*) * (count + 1));
		std::string* newIndexNames = new std::string[count + 1];
		newIndexs[count] = index;
		newIndexNames[count] = indexName;
		if ((count) > 0)
		{
			for (int i = 0; i < count; i++)
			{
				newIndexs[i] = indexs[i];
				newIndexNames[i] = indexNames[i];
			}
			free(indexs);
			delete[] indexNames;
		}
		indexs = newIndexs;
		indexNames = newIndexNames;
		count++;
		return 0;
	}
	int tableMeta::addIndex(const char* indexName, const std::list<std::string>& columns,KEY_TYPE keyType)
	{
		if (columns.size() >= 256 || columns.empty())
		{
			LOG(ERROR) << "add key failed column count " << columns.size() << " is illegal";
			return -1;
		}

		if(keyType!=KEY_TYPE::INDEX && keyType!=KEY_TYPE::UNIQUE_KEY)
		{
			LOG(ERROR) << "add key failed for only support index and unique key";
			return -1;
		}

		if(indexName!=nullptr&&strlen(indexName)!=0)
		{
			if (getIndex(indexName) != nullptr || getUniqueKey(indexName) != nullptr)
			{
				LOG(ERROR) << "add key " << indexName << " failed for key exist";
				return -1;
			}
		}

		uint16_t columnIds[256];
		uint16_t columnCount = 0;
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta* c = (columnMeta*)getColumn((*iter).c_str());
			if (c == nullptr)
			{
				LOG(ERROR) << "add key failed for column " << (*iter) << " not exist";
				return -1;
			}
			if(keyType == KEY_TYPE::UNIQUE_KEY)
				c->m_isUnique = true;
			else if(keyType == KEY_TYPE::INDEX)
				c->m_isIndex = true;
			columnIds[columnCount++] = c->m_columnIndex;
		}

		char * tmpName = nullptr;

		if(indexName == nullptr || strlen(indexName) == 0)
		{
			if(getIndex(columns.begin()->c_str()) == nullptr&&getUniqueKey(columns.begin()->c_str()) == nullptr)
			{
				indexName = columns.begin()->c_str();
			}
			else
			{
				tmpName = (char*)malloc(columns.begin()->size()+6);
				for(int i=2;;i++)
				{
					sprintf(tmpName,"%s_%d",columns.begin()->c_str(),i);
					if(getIndex(tmpName) == nullptr&&getUniqueKey(tmpName) == nullptr)
					{
						indexName = tmpName;
						break;
					}
				}
			}
		}

		unionKeyMeta* index = createUnionKey(m_indexCount, keyType, columnIds, columnCount);
		if (index != nullptr)
		{
			for (int i = 0; i < columnCount; i++)
				m_columns[columnIds[i]].m_isIndex = true;
		}
		else
		{
			if(tmpName != nullptr)
				free(tmpName);
			return -1;
		}

		if(keyType == KEY_TYPE::UNIQUE_KEY)
		{
			_addIndex(m_uniqueKeysCount,m_uniqueKeys,m_uniqueKeyNames,index,indexName);
		}
		else if(keyType == KEY_TYPE::INDEX)
		{
			_addIndex(m_indexCount,m_indexs,m_indexNames,index,indexName);
		}
		if(tmpName != nullptr)
			free(tmpName);
		return 0;
	}
	int tableMeta::renameIndex(const char* oldName, const char* newName)
	{
		unionKeyMeta* k = getIndex(oldName);
		if (k == nullptr)
		{
			LOG(ERROR) << "rename index " << oldName << " failed for index not exist";
			return -1;
		}
		if (m_nameCompare.compare(oldName, newName) != 0)
		{
			if (nullptr != getIndex(newName))
			{
				LOG(ERROR) << "rename index " << oldName << " failed for new name "<< newName <<" exist";
				return -1;
			}
			m_indexNames[k->keyId].assign(newName);
		}
		return 0;
	}
	int tableMeta::_dropIndex(int idx,uint16_t& indexCount,unionKeyMeta** &indexs,std::string*& indexNames,KEY_TYPE keyType)
	{
		if (indexCount == 1)
		{
			for (uint16_t i = 0; i < indexs[idx]->columnCount; i++)
				((columnMeta*)getColumn(indexs[idx]->columnInfo[i].columnId))->m_isIndex = false;
			free(indexs[0]);
			free(indexs);
			delete[]indexNames;
			indexCount = 0;
			return 0;
		}

		unionKeyMeta** newIndexs = (unionKeyMeta**)malloc(sizeof(unionKeyMeta*) * (indexCount - 1));
		std::string* newIndexNames = new std::string[indexCount - 1];
		for (int i = 0; i < idx; i++)
		{
			newIndexs[i] = indexs[i];
			newIndexNames[i] = indexNames[i];
		}
		for (int i = idx + 1; i < indexCount - 1; i++)
		{
			newIndexs[i - 1] = indexs[i];
			newIndexNames[i - 1] = indexNames[i];
			newIndexs[i - 1]->keyId--;
		}

		/*update columns */
		for (uint16_t i = 0; i < indexs[idx]->columnCount; i++)
		{
			for (uint16_t j = 0; j < indexCount - 1; j++)
			{
				for (uint32_t k = 0; k < newIndexs[j]->columnCount; k++)
				{
					if (indexs[idx]->columnInfo[i].columnId == newIndexs[j]->columnInfo[k].columnId)
						goto COLUMN_IS_STILL_INDEX;
				}
			}
			if(keyType == KEY_TYPE::UNIQUE_KEY)
				((columnMeta*)getColumn(indexs[idx]->columnInfo[i].columnId))->m_isUnique = false;
			else if(keyType == KEY_TYPE::INDEX)
				((columnMeta*)getColumn(indexs[idx]->columnInfo[i].columnId))->m_isIndex = false;
COLUMN_IS_STILL_INDEX:
			continue;
		}
		free(indexs[idx]);
		free(indexs);
		delete[]indexNames;
		indexs = newIndexs;
		indexNames = newIndexNames;
		indexCount--;
		return 0;
	}
	int tableMeta::dropIndex(const char* indexName)
	{
		for (int idx = 0; idx < m_indexCount; idx++)
		{
			if (m_nameCompare.compare(m_indexNames[idx].c_str(), indexName) == 0)
				return _dropIndex(idx,m_indexCount,m_indexs,m_indexNames,KEY_TYPE::INDEX);
		}
		for (int idx = 0; idx < m_uniqueKeysCount; idx++)
		{
			if (m_nameCompare.compare(m_uniqueKeyNames[idx].c_str(), indexName) == 0)
				return _dropIndex(idx,m_uniqueKeysCount,m_uniqueKeys,m_uniqueKeyNames,KEY_TYPE::UNIQUE_KEY);
		}
		LOG(ERROR) << "drop index " << indexName << " failed for index not exist";
		return -1;
	}
	int tableMeta::defaultCharset(const charsetInfo* charset, const char* collationName)
	{
		if (charset == nullptr)
		{
			LOG(ERROR) << "change default charset failed for new charset is null ";
			return -1;
		}
		m_charset = charset;
		return 0;
	}
	int tableMeta::convertDefaultCharset(const charsetInfo* charset, const char* collationName)
	{
		if (charset == nullptr)
		{
			LOG(ERROR) << "convert default charset failed for new charset is null ";
			return -1;
		}
		m_charset = charset;
		for (int idx = 0; idx < m_columnsCount; idx++)
		{
			if (columnInfos[static_cast<int>(m_columns[idx].m_columnType)].stringType)
				m_columns[idx].m_charset = m_charset;
		}
		return 0;
	}
	bool tableMeta::operator==(const tableMeta& dest)const
	{
		if (m_nameCompare.caseSensitive != dest.m_nameCompare.caseSensitive)
			return false;
		if (m_nameCompare.compare(m_dbName.c_str(), dest.m_dbName.c_str()) != 0 ||
			m_nameCompare.compare(m_tableName.c_str(), dest.m_tableName.c_str()) != 0||
			m_charset != dest.m_charset ||
			m_collate != dest.m_collate ||
			m_fixedColumnCount != dest.m_fixedColumnCount ||
			m_indexCount != dest.m_indexCount ||
			m_uniqueKeysCount != dest.m_uniqueKeysCount)
			return false;
		if (m_columnsCount != dest.m_columnsCount)
			return false;
		for (int idx = 0; idx < m_columnsCount; idx++)
		{
			if (m_columns[idx] != dest.m_columns[idx])
				return false;
		}
		if (*m_primaryKey != *dest.m_primaryKey)
			return false;
		for (int idx = 0; idx < m_uniqueKeysCount; idx++)
		{
			if (*m_uniqueKeys[idx] != *dest.m_uniqueKeys[idx])
				return false;
		}
		for (int idx = 0; idx < m_indexCount; idx++)
		{
			if (*m_indexs[idx] != *dest.m_indexs[idx])
				return false;
		}
		return true;
	}
	bool tableMeta::operator!=(const tableMeta& dest)const
	{
		return !(*this == dest);
	}
	std::string tableMeta::toString()const
	{
		std::string sql("CREATE TABLE `");
		sql.append(m_dbName).append("`.`");
		sql.append(m_tableName).append("` (");
		for (uint32_t idx = 0; idx < m_columnsCount; idx++)
		{
			if(idx!=0)
				sql.append(",");
			sql.append("\n\t").append(m_columns[idx].toString());
		}
		if (m_primaryKey!=nullptr)
		{
			sql.append(",\n");
			sql.append("\tPRIMARY KEY (");
			for (uint32_t idx = 0; idx < m_primaryKey->columnCount; idx++)
			{
				if (idx > 0)
					sql.append(",");
				sql.append("`").append(getColumn(m_primaryKey->columnInfo[idx].columnId)->m_columnName).append("`");
			}
			sql.append(")");
		}
		if (m_uniqueKeysCount > 0)
		{
			for (int idx = 0; idx < m_uniqueKeysCount; idx++)
			{
				sql.append(",\n");
				sql.append("\tUNIQUE KEY `").append(m_uniqueKeyNames[idx]).append("` (");
				for (uint32_t j = 0; j < m_uniqueKeys[idx]->columnCount; j++)
				{
					if (j > 0)
						sql.append(",");
					sql.append("`").append(getColumn(m_uniqueKeys[idx]->columnInfo[j].columnId)->m_columnName).append("`");
				}
				sql.append(")");
			}
		}
		sql.append(" \n) ").append("CHARACTER SET ").append(m_charset->name).append(";");
		return sql;
	}
}
