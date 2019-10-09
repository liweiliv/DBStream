#include "metaData.h"
#include "message/record.h"
#include "glog/logging.h"
namespace META {
	tableMeta::tableMeta(bool caseSensitive) :m_charset(nullptr), m_columns(nullptr),  m_realIndexInRowFormat(nullptr), m_fixedColumnOffsetsInRecord(nullptr),m_fixedColumnCount(0), m_varColumnCount(0),
		m_columnsCount(0), m_id(0),  m_uniqueKeysCount(0),m_uniqueKeys(nullptr), m_indexCount(0), m_indexs(nullptr), m_nameCompare(caseSensitive),userData(nullptr)
	{
	}
	tableMeta::tableMeta(DATABASE_INCREASE::TableMetaMessage * msg) :m_dbName(msg->database ? msg->database : ""), m_tableName(msg->table ? msg->table : ""),
			m_charset(&charsets[msg->metaHead.charsetId]), m_realIndexInRowFormat(nullptr), m_fixedColumnOffsetsInRecord(nullptr),m_fixedColumnCount(0), m_varColumnCount(0),
			 m_columnsCount(msg->metaHead.columnCount),
		m_id(msg->metaHead.tableMetaID),m_uniqueKeysCount(msg->metaHead.uniqueKeyCount),m_nameCompare(msg->metaHead.caseSensitive>0)
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
			m_columns[i].m_columnType = msg->columns[i].type;
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
				m_columns[i].m_setAndEnumValueList.m_Count = valueListSize;
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
			m_primaryKey.init("primary key", msg->metaHead.primaryKeyColumnCount, msg->primaryKeys);
			for (uint16_t i = 0; i < m_primaryKey.count; i++)
				((columnMeta*)getColumn(m_primaryKey.keyIndexs[i]))->m_isPrimary = true;
		}
		if (msg->metaHead.uniqueKeyCount > 0)
		{
			m_uniqueKeys = new keyInfo[msg->metaHead.uniqueKeyCount];
			for (uint16_t i = 0; i < msg->metaHead.uniqueKeyCount; i++)
			{
				m_uniqueKeys[i].init(msg->data + msg->uniqueKeyNameOffset[i], msg->uniqueKeyColumnCounts[i], msg->uniqueKeys[i]);
				for (uint16_t j = 0; j < m_uniqueKeys[i].count; j++)
					((columnMeta*)getColumn(m_uniqueKeys[i].keyIndexs[j]))->m_isUnique = true;
			}
			m_uniqueKeysCount = msg->metaHead.uniqueKeyCount;
		}
	}
	const char * tableMeta::createTableMetaRecord()
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
		m_primaryKey.clean();
		if (m_columns)
		{
			delete[]m_columns;
			m_columns = nullptr;
		}
		if (m_uniqueKeys != nullptr)
		{
			delete[]m_uniqueKeys;
			m_uniqueKeys = nullptr;
		}
		if (m_indexs != nullptr)
		{
			delete[]m_indexs;
			m_indexs = nullptr;
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
		m_primaryKey = t.m_primaryKey;
		/*copy unique key*/
		m_uniqueKeysCount = t.m_uniqueKeysCount;
		if (t.m_uniqueKeys != nullptr)
		{
			m_uniqueKeys = new keyInfo[m_uniqueKeysCount];
			for (int i = 0; i < m_uniqueKeysCount; i++)
				m_uniqueKeys[i] = t.m_uniqueKeys[i];
		}
		m_indexCount = t.m_indexCount;
		if (t.m_indexs != nullptr)
		{
			m_indexs = new keyInfo[m_indexCount];
			for (int i = 0; i < m_indexCount; i++)
				m_indexs[i] = t.m_indexs[i];
		}
		m_fixedColumnCount = t.m_fixedColumnCount;
		m_varColumnCount = t.m_varColumnCount;
		m_realIndexInRowFormat = new uint16_t[m_columnsCount];
		memcpy(m_realIndexInRowFormat,t.m_realIndexInRowFormat,sizeof(uint16_t)*m_columnsCount);
		m_fixedColumnOffsetsInRecord = new uint16_t[m_fixedColumnCount+1];
		memcpy(m_fixedColumnOffsetsInRecord,t.m_fixedColumnOffsetsInRecord,sizeof(uint16_t)*(m_fixedColumnCount+1));
		
		return *this;
	}
	void tableMeta::buildColumnOffsetList()
	{
		m_fixedColumnCount = m_varColumnCount = 0;
		for (uint16_t i = 0; i < m_columnsCount; i++)
		{
			if (columnInfos[m_columns[i].m_columnType].fixed)
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
			if (columnInfos[m_columns[i].m_columnType].fixed)
			{
				m_fixedColumnOffsetsInRecord[m_fixedColumnCount] = fixedOffset;
				fixedOffset += columnInfos[m_columns[i].m_columnType].columnTypeSize;
				m_realIndexInRowFormat[i] = m_fixedColumnCount++;
			}
			else
			{
				m_realIndexInRowFormat[i] = m_varColumnCount++;
			}
		}
		m_fixedColumnOffsetsInRecord[m_fixedColumnCount] = fixedOffset;
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
				for (uint16_t i = 0; i < m_uniqueKeys[idx].count;)
				{
					if (m_uniqueKeys[idx].keyIndexs[i] > columnIndex)
						m_uniqueKeys[idx].keyIndexs[i]--;
					else if (m_uniqueKeys[idx].keyIndexs[i] == columnIndex)
					{
						memcpy(&m_uniqueKeys[idx].keyIndexs[i], &m_uniqueKeys[idx].keyIndexs[i + 1], sizeof(uint16_t)*(m_uniqueKeys[idx].count - i - 1));
						m_uniqueKeys[idx].count--;
						continue;//do not do [i++]
					}
					i++;
				}
				if (m_uniqueKeys[idx].count == 0)
				{
					dropUniqueKey(m_uniqueKeys[idx].name.c_str());
				}
			}
		}
		for (uint16_t i = 0; i < m_primaryKey.count;)
		{
			if (m_primaryKey.keyIndexs[i] > columnIndex)
				m_primaryKey.keyIndexs[i]--;
			else if (m_primaryKey.keyIndexs[i] == columnIndex)
			{
				memcpy(&m_primaryKey.keyIndexs[i], &m_primaryKey.keyIndexs[i + 1], sizeof(uint16_t)*(m_primaryKey.count - i - 1));
				m_primaryKey.count--;
				continue;//do not do [i++]
			}
			i++;
		}
		if (m_primaryKey.count == 0)
			dropPrimaryKey();
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
			return -1;
		return dropColumn(d->m_columnIndex);
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
		if (!first && addAfter == nullptr)
		{
			m_columns[idx] = *column;
			m_columns[idx].m_columnIndex = idx;
			if (columnInfos[m_columns[idx].m_columnType].stringType && m_columns[idx].m_charset == nullptr)
				m_columns[idx].m_charset = m_charset;
			buildColumnOffsetList();
		}
		else
		{
			if (dropColumn(column->m_columnName.c_str()) != 0)
			{
				LOG(ERROR) << "modify column " << column->m_columnName << " failed for drop column failed";
				return -1;
			}
			if (addColumn(column, addAfter, first) != 0)
			{
				LOG(ERROR) << "modify column " << column->m_columnName << " failed for add column failed";
				return -1;
			}
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
		if (strncasecmp(columnName, newColumn->m_columnName.c_str(), newColumn->m_columnName.size()) != 0)
		{

		}
		int idx = old->m_columnIndex;
		if (!first && addAfter == nullptr)
		{
			m_columns[idx] = *newColumn;
			m_columns[idx].m_columnIndex = idx;
			if (columnInfos[m_columns[idx].m_columnType].stringType && m_columns[idx].m_charset == nullptr)
				m_columns[idx].m_charset = m_charset;
			buildColumnOffsetList();
		}
		else
		{
			if (dropColumn(columnName) != 0)
			{
				LOG(ERROR) << "modify column " << columnName << " failed for drop column failed";
				return -1;
			}
			if (addColumn(newColumn, addAfter, first) != 0)
			{
				LOG(ERROR) << "modify column " << newColumn->m_columnName << " failed for add column failed";
				return -1;
			}
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
		if (addAfter)
		{
			const columnMeta * before = getColumn(addAfter);
			if (before == nullptr)
			{
				LOG(ERROR) << "add column " << column->m_columnName << " after "<< addAfter <<" failed for column not exist";
				return -2;
			}
			columnMeta * columns = new columnMeta[m_columnsCount + 1];
			for (uint32_t idx = 0; idx <= before->m_columnIndex; idx++)
				columns[idx] = m_columns[idx];

			columns[before->m_columnIndex] = *column;
			columns[before->m_columnIndex].m_columnIndex = before->m_columnIndex + 1;
			if (columnInfos[before->m_columnType].stringType && column->m_charset == nullptr)
				columns[before->m_columnIndex].m_charset = m_charset;

			for (uint32_t idx = before->m_columnIndex + 1; idx <= m_columnsCount; idx++)
			{
				columns[idx] = m_columns[idx - 1];
				columns->m_columnIndex++;
			}
			if (m_uniqueKeys != nullptr)
			{
				for (uint16_t idx = 0; idx < m_uniqueKeysCount; idx++)
				{
					for (uint16_t i = 0; i < m_uniqueKeys[idx].count; i++)
					{
						if (m_uniqueKeys[idx].keyIndexs[i] > before->m_columnIndex)
							m_uniqueKeys[idx].keyIndexs[i]++;
					}
				}
			}
			if (m_primaryKey.count > 0)
			{
				for (uint16_t i = 0; i < m_primaryKey.count; i++)
				{
					if (m_primaryKey.keyIndexs[i] > before->m_columnIndex)
						m_primaryKey.keyIndexs[i]++;
				}
			}
		}
		else if (first)
		{
			columnMeta* columns = new columnMeta[m_columnsCount + 1];
			for (uint32_t idx = 1; idx <= m_columnsCount; idx++)
				columns[idx] = m_columns[idx-1];
			columns[0] = *column;
			columns[0].m_columnIndex = 0;
			if (columnInfos[column->m_columnType].stringType && column->m_charset == nullptr)
				columns[m_columnsCount].m_charset = m_charset;
			if (m_uniqueKeys != nullptr)
			{
				for (uint16_t idx = 0; idx < m_uniqueKeysCount; idx++)
				{
					for (uint16_t i = 0; i < m_uniqueKeys[idx].count; i++)
						m_uniqueKeys[idx].keyIndexs[i]++;
				}
			}
			if (m_primaryKey.count > 0)
			{
				for (uint16_t i = 0; i < m_primaryKey.count; i++)
					m_primaryKey.keyIndexs[i]++;
			}
			delete[]m_columns;
			m_columns = columns;
		}
		else
		{
			columnMeta * columns = new columnMeta[m_columnsCount + 1];
			for (uint32_t idx = 0; idx < m_columnsCount; idx++)
				columns[idx] = m_columns[idx];
			columns[m_columnsCount] = *column;
			columns[m_columnsCount].m_columnIndex = m_columnsCount;
			if (columnInfos[column->m_columnType].stringType && column->m_charset == nullptr)
				columns[m_columnsCount].m_charset = m_charset;
			delete []m_columns;
			m_columns = columns;
		}
		m_columnsCount++;
		buildColumnOffsetList();
		return 0;
	}
	int tableMeta::dropPrimaryKey()
	{
		for (uint16_t i = 0; i < m_primaryKey.count; i++)
			((columnMeta*)getColumn(m_primaryKey.keyIndexs[i]))->m_isPrimary = false;
		m_primaryKey.clean();
		return 0;
	}
	int tableMeta::createPrimaryKey(const std::list<std::string> &columns)
	{
		if (m_primaryKey.count != 0)
			return -1;
		m_primaryKey.clean();
		m_primaryKey.name = "primary key";
		uint32_t keySize = 0;
		m_primaryKey.keyIndexs = new uint16_t[columns.size()];
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta * c = (columnMeta*)getColumn((*iter).c_str());
			if (c == nullptr)
				goto ROLL_BACK;
			if (columnInfos[c->m_columnType].fixed)
				keySize += columnInfos[c->m_columnType].columnTypeSize;
			else
				keySize += c->m_size;
			if (keySize > MAX_KEY_SIZE)
				goto ROLL_BACK;
			c->m_isPrimary = true;
			m_primaryKey.keyIndexs[m_primaryKey.count++] = c->m_columnIndex;
		}
		return 0;
	ROLL_BACK:
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta * c = (columnMeta*)getColumn((*iter).c_str());
			if (c == nullptr)
				break;
			c->m_isPrimary = false;
		}
		m_primaryKey.clean();
		return -1;
	}
	int tableMeta::dropUniqueKey(const char *ukName)
	{
		int idx = 0;
		for (; idx < m_uniqueKeysCount; idx++)
		{
			if (strcmp(m_uniqueKeys[idx].name.c_str(), ukName) == 0)
				goto DROP;
		}
		return -1;
	DROP:
		keyInfo * newUks = new keyInfo[m_uniqueKeysCount - 1];
		for (int i = 0; i < idx; i++)
			newUks[i] = m_uniqueKeys[i];
		for (int i = idx + 1; i < m_uniqueKeysCount - 1; i++)
			newUks[i - 1] = m_uniqueKeys[i];

		/*update columns */
		for (uint16_t i = 0; i < m_uniqueKeys[idx].count; i++)
		{
			for (uint16_t j = 0; j < m_uniqueKeysCount - 1; j++)
			{
				for (uint32_t k = 0; k < newUks[j].count; k++)
				{
					if (m_uniqueKeys[idx].keyIndexs[i] == newUks[j].keyIndexs[k])
						goto COLUMN_IS_STILL_UK;
				}
			}
			((columnMeta*)getColumn(m_uniqueKeys[idx].keyIndexs[i]))->m_isUnique = false;
		COLUMN_IS_STILL_UK:
			continue;
		}
		delete[]m_uniqueKeys;
		m_uniqueKeys = newUks;
		m_uniqueKeysCount--;
		return 0;
	}
	int tableMeta::addUniqueKey(const char *ukName, const std::list<std::string> &columns)
	{
		if (getUniqueKey(ukName) != nullptr)
			return -1;
		/*check*/
		uint32_t keySize = 0;
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			const columnMeta * column = getColumn((*iter).c_str());
			if (column == nullptr)
				return -1;
			if (columnInfos[column->m_columnType].fixed)
				keySize += columnInfos[column->m_columnType].columnTypeSize;
			else
				keySize += column->m_size;
			if (keySize > MAX_KEY_SIZE)
				return -1;
		}
		/*copy data*/
		keyInfo * newUks = new keyInfo[m_uniqueKeysCount - 1];
		newUks[m_uniqueKeysCount].keyIndexs = new uint16_t[columns.size()];
		newUks[m_uniqueKeysCount].name = ukName;
		for (std::list<std::string>::const_iterator iter = columns.begin(); iter != columns.end(); iter++)
		{
			columnMeta * column = (columnMeta*)getColumn((*iter).c_str());
			newUks[m_uniqueKeysCount].keyIndexs[newUks[m_uniqueKeysCount].count++] = column->m_columnIndex;
			if (!column->m_isUnique)
				column->m_isUnique = true;
		}
		for (int i = 0; i < m_uniqueKeysCount; i++)
			newUks[i] = m_uniqueKeys[i];
		delete[]m_uniqueKeys;
		m_uniqueKeys = newUks;
		m_uniqueKeysCount++;
		return 0;
	}
	std::string tableMeta::toString()
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
		if (m_primaryKey.count > 0)
		{
			sql.append(",\n");
			sql.append("\tPRIMARY KEY (");
			for (uint32_t idx = 0; idx < m_primaryKey.count; idx++)
			{
				if (idx > 0)
					sql.append(",");
				sql.append("`").append(getColumn(m_primaryKey.keyIndexs[idx])->m_columnName).append("`");
			}
			sql.append(")");
		}
		if (m_uniqueKeysCount > 0)
		{
			for (int idx = 0; idx < m_uniqueKeysCount; idx++)
			{
				sql.append(",\n");
				sql.append("\tUNIQUE KEY `").append(m_uniqueKeys[idx].name).append("` (");
				for (uint32_t j = 0; j < m_uniqueKeys[idx].count; j++)
				{
					if (j > 0)
						sql.append(",");
					sql.append("`").append(getColumn(m_uniqueKeys[idx].keyIndexs[j])->m_columnName).append("`");
				}
				sql.append(")");
			}
		}
		sql.append(" \n) ").append("CHARACTER SET ").append(m_charset->name).append(";");
		return sql;
	}
}
