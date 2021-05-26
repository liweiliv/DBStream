#include "oracleMetaDataCollection.h"
namespace DATA_SOURCE
{
	struct indexInfo
	{
		uint64_t indexObjectId;
		uint64_t tableObjectId;
		std::string indexName;
		META::unionKeyMeta* keyMeta;
		indexInfo() :indexObjectId(0), tableObjectId(0), keyMeta(nullptr) {}
		indexInfo(const indexInfo& i) :indexObjectId(i.indexObjectId), tableObjectId(i.tableObjectId), indexName(i.indexName), keyMeta(i.keyMeta) {}
		indexInfo& operator=(const indexInfo& i)
		{
			indexObjectId = i.indexObjectId;
			tableObjectId = i.tableObjectId;
			indexName = i.indexName;
			keyMeta = i.keyMeta;
		}
	};

	constexpr static auto SELECT_CHARSET = "SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER=:1";
	constexpr static auto NLS_NCHAR_CHARSET_PARAMETER = "NLS_NCHAR_CHARACTERSET ";
	constexpr static auto NLS_CHARSET_PARAMETER = "NLS_CHARACTERSET";

	constexpr static auto SELECT_USERS = "SELECT USERNAME, USER_ID, ORACLE_MAINTAINED FROM SYS.ALL_USERS";
	constexpr static auto SELECT_OBJECTS = "SELECT OBJ#, NAME, TYPE# FROM SYS.OBJ$ WHERE OWNER#=:1 AND TYPE# IN (1, 2, 19, 34)";//1:index ,2 table, 19 partition, 34 subpartition 
	constexpr static auto SELECT_COLUMNS = "SELECT OBJ#, COL#, SEGCOL#, SEGCOLLENGTH, NAME, TYPE#, LENGTH, PRECISION#, SCALE, NULL$, CHARSETID, CHARSETFORM FROM SYS.COL$ WHERE OBJ# ";
	constexpr static auto SELECT_INDEXS = "SELECT OBJ#, BO#, COLS, PROPERTY FROM SYS.IND$ WHERE TYPE#=1 AND OBJ# ";
	constexpr static auto SELECT_INDEX_COLS = "SELECT OBJ#, BO#, COL#, POS# FROM SYS.ICOL$ WHERE OBJ# ";

	DS oracleMetaDataCollection::createConnect()
	{
		dsReturn(connect(m_connector, m_conn));
	}
	DS oracleMetaDataCollection::connect(occiConnect* connector, oracle::occi::Connection*& conn)
	{
		if (conn != nullptr)
			dsOk();
		dsReturn(connector->connect(conn));
	}
	DS oracleMetaDataCollection::getUserList(std::list<std::pair<std::string, int32_t>>& userList)
	{
		dsReturnIfFailed(createConnect());
		dsReturn(occiConnect::query(m_connector, m_conn, 128, connect,
			[&userList, this](oracle::occi::ResultSet* rs) ->DS {
				bool hasGetSys = false;
				do {
					std::string user = rs->getString(1);
					if (user.compare("SYS") != 0)
					{
						if (rs->getString(3).compare("Y") == 0)//ignore system user
							continue;
						if (!m_whiteList->contain(user.c_str()) || m_blackList->contain(user.c_str()))
							continue;
					}
					else
						hasGetSys = true;
					userList.push_back(std::pair<std::string, uint32_t>(user, rs->getInt(2)));
				} while (rs->next());
				if (!hasGetSys)
					dsFailedAndLogIt(1, "do not get user:SYS from SYS.ALL_USERS", ERROR);
				dsOk();
			}, SELECT_USERS));
	}
	DS oracleMetaDataCollection::getUserObjectList(int ownerId, std::string& owner, std::map<uint64_t, std::string>& tableList,
		std::map<uint64_t, std::string>& indexList, std::map<std::string, std::list<uint64_t>*>& partitionsList)
	{
		dsReturnIfFailed(createConnect());
		dsReturn(occiConnect::query(m_connector, m_conn, 128, connect,
			[ownerId, &owner, &tableList, &indexList, &partitionsList, this](oracle::occi::ResultSet* rs) ->DS {
				do {
					std::string name = rs->getString(2);
					int type = rs->getInt(3);
					switch (type)
					{
					case 1:
						indexList.insert(std::pair<int, std::string>(rs->getInt(1), name));
						break;
					case 2:
						if (m_whiteList->contain(owner.c_str(), name.c_str()) && !m_blackList->contain(owner.c_str(), name.c_str()))
							tableList.insert(std::pair<int, std::string>(rs->getInt(1), name));
						break;
					case 19:
					case 34:
						if (m_whiteList->contain(owner.c_str(), name.c_str()) && !m_blackList->contain(owner.c_str(), name.c_str()))
						{
							std::map<std::string, std::list<uint64_t>*>::iterator piter = partitionsList.find(name);
							std::list<uint64_t>* partitionList = piter == partitionsList.end() ? partitionsList.insert(std::pair<std::string, std::list<uint64_t>*>(name, new std::list<uint64_t>)).first->second : piter->second;
							partitionList->push_back(rs->getInt(1));
						}
						break;
					default:
						dsFailedAndLogIt(1, "unkown type " << type << " of object:" << owner << "." << name, ERROR);
					}
				} while (rs->next());
				dsOk();
			}, SELECT_OBJECTS, { ownerId }));
	}

	DS oracleMetaDataCollection::getSysTableInfo(int ownerId, std::map<uint64_t, std::string>& tableList)
	{
		dsReturnIfFailed(createConnect());
		String sql = SELECT_OBJECTS;
		sql.append(" AND NAME IN('OBJ$', 'COL$')");
		dsReturn(occiConnect::query(m_connector, m_conn, 0, connect,
			[ownerId, &tableList](oracle::occi::ResultSet* rs) ->DS {
				do {
					tableList.insert(std::pair<int, std::string>(rs->getInt(1), rs->getString(2)));
				} while (rs->next());
				if (tableList.size() != 2)
					dsFailedAndLogIt(1, "do not get SYS.OBJ$ and SYS.COL$ from SYS.OBJ$", ERROR);
				dsOk();
			}, sql, { ownerId }));
	}


	DS oracleMetaDataCollection::translateCharset(const std::string& src, CHARSET& dest)
	{
		const char* s = src.c_str();
		while (*s != '\0' && (*s < '0' || *s>'9'))
			s++;
		if (*s == '\0')
		{
			s = src.c_str();
		}
		else
		{
			while (*s <= '0' && *s <= '9')
				s++;
		}
		if (strcasecmp(s, "GBK") == 0)
			dest = CHARSET::gbk;
		else if (strcasecmp(s, "UTF8") == 0 || strcasecmp(s, "UTF-8") == 0)
			dest = CHARSET::utf8;
		else if (strcasecmp(s, "UTF16") == 0 || strcasecmp(s, "UTF-16") == 0)
			dest = CHARSET::utf16;
		else if (strcasecmp(s, "UTF16LE") == 0 || strcasecmp(s, "UTF-16LE") == 0)
			dest = CHARSET::utf16le;
		else if (strcasecmp(s, "UTF-32") == 0 || strcasecmp(s, "UTF-32") == 0)
			dest = CHARSET::utf32;
		else if (strcasecmp(s, "BIG5") == 0)
			dest = CHARSET::big5;
		else if (strcasecmp(s, "GB18030") == 0)
			dest = CHARSET::gb18030;
		else if (strcasecmp(s, "ISO-8859-1") == 0)
			dest = CHARSET::latin1;
		else if (strcasecmp(s, "ISO-8859-15") == 0)
			dest = CHARSET::latin9;
		else if (strcasecmp(s, "ASCII") == 0 || strcasecmp(s, "US-ASCII") == 0)
			dest = CHARSET::ascii;
		else if (strcasecmp(s, "windows-1250") == 0 || strcasecmp(s, "mswin1250") == 0)
			dest = CHARSET::cp1250;
		else if (strcasecmp(s, "windows-1256") == 0 || strcasecmp(s, "mswin1256") == 0)
			dest = CHARSET::cp1256;
		else if (strcasecmp(s, "windows-1257") == 0 || strcasecmp(s, "mswin1257") == 0)
			dest = CHARSET::cp1257;
		else if (strcasecmp(s, "sjis") == 0)
			dest = CHARSET::sjis;
		else if (strcasecmp(s, "GB2312") == 0)
			dest = CHARSET::gb2312;
		else
			dsFailedAndLogIt(1, "do not support charset:" << src, ERROR);
		dsOk();
	}

	void oracleMetaDataCollection::setCharset(META::columnMeta* meta, int charsetForm)
	{
		switch (static_cast<ORACLE_COLUMN_TYPE>(meta->m_srcColumnType))
		{
		case ORACLE_COLUMN_TYPE::Char:
		case ORACLE_COLUMN_TYPE::varchar:
		case ORACLE_COLUMN_TYPE::varchar2:
		case ORACLE_COLUMN_TYPE::clob:
			if (charsetForm == 2)
				meta->m_charset = &charsets[static_cast<int>(m_ncharCharsetId)];
			else
				meta->m_charset = &charsets[static_cast<int>(m_charsetId)];
			break;
		case ORACLE_COLUMN_TYPE::Long:
			meta->m_charset = &charsets[static_cast<int>(m_charsetId)];
			break;
		case ORACLE_COLUMN_TYPE::rowId:
		case ORACLE_COLUMN_TYPE::urowId:
			meta->m_charset = &charsets[static_cast<int>(CHARSET::ascii)];
			break;
		default:
			break;
		}
	}

	DS oracleMetaDataCollection::getDbCharset()
	{
		dsReturnIfFailed(createConnect());
		dsReturn(occiConnect::query(m_connector, m_conn, 0, connect,
			[this](oracle::occi::ResultSet* rs) ->DS {
				m_charset.assign(rs->getString(1));
				dsOk();
			}, SELECT_CHARSET, { NLS_CHARSET_PARAMETER }));
		dsReturn(occiConnect::query(m_connector, m_conn, 0, connect,
			[this](oracle::occi::ResultSet* rs) ->DS {
				m_ncharCharset.assign(rs->getString(1));
				dsOk();
			}, SELECT_CHARSET, { NLS_NCHAR_CHARSET_PARAMETER }));
		if (m_charset.empty() || m_ncharCharset.empty())
			dsFailedAndLogIt(1, "get charset from oracle failed for charset is empty", ERROR);
		dsReturnIfFailed(translateCharset(m_charset, m_charsetId));
		dsReturnIfFailed(translateCharset(m_ncharCharset, m_ncharCharsetId));
		LOG(INFO) << "oracle " << NLS_CHARSET_PARAMETER << " is " << m_charset;
		LOG(INFO) << "oracle " << NLS_NCHAR_CHARSET_PARAMETER << " is " << m_ncharCharset;
		dsOk();
	}

	META::columnMeta* oracleMetaDataCollection::readColumnInfo(oracle::occi::ResultSet* rs)
	{
		META::columnMeta* col = new META::columnMeta();
		col->m_columnIndex = rs->getInt(2) - 1;
		col->m_segmentStartId = rs->getInt(3);
		col->m_size = rs->getInt(4);
		col->m_columnName = rs->getString(5);
		int type = rs->getInt(7);
		col->m_precision = rs->getInt(8);
		col->m_decimals = rs->getInt(9);
		if (rs->getInt(10) == 1)
			col->setFlag(COL_FLAG_NOT_NULL);
		/*
		if (!rs->isNull(14))
		{
			int defaultLength = rs->getInt(13);
			std::string defaultValue = rs->getString(14);
			col->m_default.m_defaultValue = new char[(col->m_default.m_defaultValueSize = defaultValue.length()) + 1];
			strcpy((char*)col->m_default.m_defaultValue, defaultValue.c_str());
		}
		*/
		int charsetId = rs->getInt(11);
		int charsetForm = rs->getInt(12);
		col->m_columnType = translateType(col);
		setCharset(col, charsetForm);
		return col;
	}

	void clearUserColumnsList(userColumnsList* userColumns)
	{
		for (auto uiter : *userColumns)
		{
			for (auto titer : *uiter.second)
				delete titer.second;
			delete uiter.second;
		}
		userColumns->clear();
	}
	void clearAllUserColumnsList(allUserColumnsList& columns)
	{
		for (auto iter : columns)
		{
			clearUserColumnsList(iter.second);
			delete iter.second;
		}
		columns.clear();
	}

	DS oracleMetaDataCollection::getColumns(std::map<uint64_t, std::string>& tables, userColumnsList* userColumns)
	{
		std::map<uint64_t, std::string>::iterator iter = tables.begin();
		for (int i = 0; i <= tables.size() / 512; i++)
		{
			String sql = SELECT_COLUMNS;
			sql.append(" IN (");
			int count = std::min<int>(tables.size() - i * 512, 512);
			for (int c = 0; c < count; c++)
			{
				if (c != 0)
					sql.append(",");
				sql.append(iter->first);
				iter++;
			}
			sql.append(")");
			dsReturnIfFailedWithOp(occiConnect::query(m_connector, m_conn, 0, connect,
				[&userColumns, this](oracle::occi::ResultSet* rs) ->DS {
					do {
						uint64_t objectId = (uint64_t)rs->getNumber(1).operator long();
						META::columnMeta* col = readColumnInfo(rs);
						userColumnsList::iterator iter = userColumns->find(objectId);
						tableColumnsMap* t = (iter == userColumns->end()) ?
							userColumns->insert(std::pair<uint64_t, tableColumnsMap*>(objectId, new tableColumnsMap())).first->second : iter->second;
						t->insert(std::pair<std::string, META::columnMeta*>(col->m_columnName, col));
					} while (rs->next());
					dsOk();
				}, sql), clearUserColumnsList(userColumns));
		}
		dsOk();
	}

	DS oracleMetaDataCollection::initUserTableMeta(std::string& ownerName, int32_t ownerId, std::map<uint64_t, std::string>& tables)
	{
		userColumnsList userColumns;
		dsReturnIfFailed(getColumns(tables, &userColumns));
		for (auto iter : userColumns)
		{
			std::map < uint64_t, std::string>::iterator titer = tables.find(iter.first);
			if (titer == tables.end())
			{
				clearUserColumnsList(&userColumns);
				dsFailedAndLogIt(1, "get an unknown table object id:" << iter.first, ERROR);
			}
			string tableName = titer->second;
			int realColumnCount = 0;
			int maxColumnId = 0;
			std::list<int> hiddenColumnSegIds;
			META::columnMeta* cols[2048] = { 0 };
			for (auto citer : *iter.second)
			{
				META::columnMeta* col = citer.second;
				if (col->m_columnIndex < 0)//unused columns
				{
					hiddenColumnSegIds.push_back(col->m_segmentStartId);
					delete col;
					continue;
				}
				if (cols[col->m_columnIndex] != nullptr)//has multi sub columns type, like xml,sdo_geometry and some user defined type
				{
					if (cols[col->m_columnIndex]->m_segmentStartId < col->m_segmentStartId)
					{
						if (cols[col->m_columnIndex]->m_columnType == META::COLUMN_TYPE::T_XML)//xml type may stored by blob and clob, determined by the second column
						{
							cols[col->m_columnIndex]->m_segmentStartId = col->m_segmentStartId;
							cols[col->m_columnIndex]->m_srcColumnType = static_cast<uint8_t>(col->m_columnType);
						}
						else
						{
							if (col->m_segmentStartId >= cols[col->m_columnIndex]->m_segmentStartId + cols[col->m_columnIndex]->m_segmentCount)
								cols[col->m_columnIndex]->m_segmentCount = col->m_segmentStartId - cols[col->m_columnIndex]->m_segmentStartId + 1;
						}
						delete col;
						continue;
					}
					else
					{
						if (col->m_columnType == META::COLUMN_TYPE::T_XML)//xml type may stored by blob and clob, determined by the second column
						{
							col->m_segmentStartId = cols[col->m_columnIndex]->m_segmentStartId;
							cols[col->m_columnIndex]->m_srcColumnType = static_cast<uint8_t>(col->m_columnType);
						}
						else
						{
							col->m_segmentCount = cols[col->m_columnIndex]->m_segmentStartId + cols[col->m_columnIndex]->m_segmentCount - col->m_segmentStartId + 1;
						}
						delete cols[col->m_columnIndex];
						cols[col->m_columnIndex] = col;
						continue;
					}
				}
				else
				{
					col->m_segmentCount = 1;
					cols[col->m_columnIndex] = col;
					realColumnCount++;
					maxColumnId = std::max<int>(maxColumnId, col->m_columnIndex);
				}
			}
			iter.second->clear();
			if (realColumnCount != maxColumnId + 1)
			{
				clearUserColumnsList(&userColumns);
				dsFailedAndLogIt(1, "table:" << ownerName << "." << tableName << " column count " << realColumnCount << "and max column id " << maxColumnId + 1 << "is not match", ERROR);
			}
			META::tableMeta* table = new META::tableMeta(true);
			table->m_charset = &charsets[static_cast<int>(m_charsetId)];
			table->m_dbName = ownerName;
			table->m_tableName = tableName;
			table->m_columnsCount = realColumnCount;
			table->m_columns = new META::columnMeta[realColumnCount];
			for (int i = 0; i < realColumnCount; i++)
			{
				table->m_columns[i] = *cols[i];
				delete cols[i];
			}
			table->buildColumnOffsetList();
			put(table->m_dbName.c_str(), table->m_tableName.c_str(), table, 0);
			META::MetaTimeline<META::tableMeta>* tableInfo = _getTableInfo(table->m_dbName.c_str(), table->m_tableName.c_str());
			m_tables.insert(std::pair<uint64_t, META::MetaTimeline<META::tableMeta>*>(titer->first, tableInfo));
		}
		clearUserColumnsList(&userColumns);
	}

	DS oracleMetaDataCollection::initPartitons(std::string& owner, std::map<std::string, std::list<uint64_t>*>& partitionList)
	{
		for (auto iter : partitionList)
		{
			META::tableMeta* t = get(owner.c_str(), iter.first.c_str());
			if (t == nullptr)
				dsFailedAndLogIt(1, "can not find table " << owner << "." << iter.first << " in metadata collecton", ERROR);
			spp::sparse_hash_map<uint64_t, META::MetaTimeline<META::tableMeta>*>::const_iterator titer = m_tables.find(t->m_objectIdInDB);
			if (titer == m_tables.end())
				dsFailedAndLogIt(1, "can not find table " << owner << "." << iter.first << " in metadata collecton", ERROR);
			t->m_subObjectIdInDBList = new uint64_t[iter.second->size()];
			for (auto id : *iter.second)
			{
				t->m_subObjectIdInDBList[t->m_subObjectIdInDBListSize++] = id;
				m_tables.insert(std::pair<uint64_t, META::MetaTimeline<META::tableMeta>*>(id, titer->second));
			}
		}
	}

	void clearPartitionsList(std::map<std::string, std::list<uint64_t>*>& partitionList)
	{
		for (auto iter : partitionList)
			delete iter.second;
		partitionList.clear();
	}

	void clearIndexList(std::map<uint64_t, indexInfo*>& neededIndexs)
	{
		for (auto iter : neededIndexs)
		{
			if (iter.second->keyMeta != nullptr)
				delete iter.second->keyMeta;
			delete iter.second;
		}
		neededIndexs.clear();
	}

	DS oracleMetaDataCollection::getAllNeedIndexInfo(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList,
		std::map<uint64_t, indexInfo*>& neededIndexs, std::map<uint64_t, std::list<indexInfo*>>& tableIndexInfo)
	{
		std::map<uint64_t, std::string>::const_iterator iter = indexList.begin();
		for (int i = 0; i <= indexList.size() / 512; i++)
		{
			String sql = SELECT_INDEXS;
			sql.append(" IN (");
			int count = std::min<int>(indexList.size() - i * 512, 512);
			for (int c = 0; c < count; c++)
			{
				if (c != 0)
					sql.append(",");
				sql.append(iter->first);
				iter++;
			}
			sql.append(")");
			dsReturnIfFailedWithOp(occiConnect::query(m_connector, m_conn, 0, connect,
				[&owner, &tables, &indexList, &neededIndexs, &tableIndexInfo, this](oracle::occi::ResultSet* rs) ->DS {
					do {
						int property = rs->getInt(4);
						META::KEY_TYPE keyType;
						if (property == 0)
							keyType = META::KEY_TYPE::INDEX;
						else if (property == 1)
							keyType = META::KEY_TYPE::UNIQUE_KEY;
						else if (property == 4097)
							keyType = META::KEY_TYPE::PRIMARY_KEY;
						else
							continue;
						indexInfo* index = new indexInfo();
						index->indexObjectId = (uint64_t)rs->getNumber(1).operator long();
						index->tableObjectId = (uint64_t)rs->getNumber(2).operator long();
						auto tIter = tables.find(index->tableObjectId);
						if (tIter == tables.end()) //not what we need
							continue;
						auto iIter = indexList.find(index->indexObjectId);
						if (iIter == indexList.end())
							dsFailedAndLogIt(1, "can not find index " << index->indexObjectId << "of table:" << owner << "." << tIter->second << " in index list", ERROR);
						index->indexName = iIter->second;
						int columnCount = rs->getInt(3);
						index->keyMeta = new (malloc(sizeof(META::unionKeyMeta) + columnCount * sizeof(META::uniqueKeyTypePair))) META::unionKeyMeta(columnCount);
						index->keyMeta->keyType = TID(keyType);
						neededIndexs.insert(std::pair<uint64_t, indexInfo*>(index->indexObjectId, index));
						auto tiIter = tableIndexInfo.find(index->tableObjectId);
						if (tiIter == tableIndexInfo.end())
							tableIndexInfo.insert(std::pair<uint64_t, std::list<indexInfo*>>(index->tableObjectId, std::list<indexInfo*>{ index }));
						else
							tiIter->second.push_back(index);
					} while (rs->next());
					dsOk();
				}, sql), clearIndexList(neededIndexs));
		}
		dsOk();
	}

	DS oracleMetaDataCollection::getColumnInfoOfIndexs(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList,
		std::map<uint64_t, indexInfo*>& neededIndexs)
	{
		std::map<uint64_t, indexInfo*>::iterator iter = neededIndexs.begin();
		for (int i = 0; i <= neededIndexs.size() / 512; i++)
		{
			String sql = SELECT_INDEX_COLS;
			sql.append(" IN (");
			int count = std::min<int>(neededIndexs.size() - i * 512, 512);
			for (int c = 0; c < count; c++)
			{
				if (c != 0)
					sql.append(",");
				sql.append(iter->first);
				iter++;
			}
			sql.append(")");
			dsReturnIfFailedWithOp(occiConnect::query(m_connector, m_conn, 0, connect,
				[&neededIndexs, this](oracle::occi::ResultSet* rs) ->DS {
					do {
						uint64_t objectId = (uint64_t)rs->getNumber(1).operator long();
						std::map<uint64_t, indexInfo*>::iterator iter = neededIndexs.find(objectId);
						if (iter == neededIndexs.end())
							dsFailedAndLogIt(1, "can not find index " << objectId << " in index list", ERROR);
						uint64_t tableObjectId = (uint64_t)rs->getNumber(2).operator long();
						if (iter->second->tableObjectId != tableObjectId)
							dsFailedAndLogIt(1, "table object id " << iter->second->tableObjectId << " of index " << iter->second->indexName << " in index info is not match with in SYS.ICOL$ " << tableObjectId, ERROR);
						int columnId = rs->getInt(3);
						int pos = rs->getInt(4);
						if (iter->second->keyMeta->columnCount < pos)
							dsFailedAndLogIt(1, "find column pos " << pos << " of table index " << iter->second->indexName << " is greater than column count " << iter->second->keyMeta->columnCount, ERROR);
						iter->second->keyMeta->columnInfo[pos - 1].columnId = columnId;
					} while (rs->next());
					dsOk();
				}, sql), clearIndexList(neededIndexs));
		}
		dsOk();
	}

	DS oracleMetaDataCollection::initIndexs(const std::string& owner, const std::map<uint64_t, std::string>& tables, const std::map<uint64_t, std::string>& indexList)
	{
		std::map<uint64_t, indexInfo*> neededIndexs;
		std::map<uint64_t, std::list<indexInfo*>> tableIndexInfo;
		dsReturnIfFailed(getAllNeedIndexInfo(owner, tables, indexList, neededIndexs, tableIndexInfo));
		dsReturnIfFailed(getColumnInfoOfIndexs(owner, tables, indexList, neededIndexs));
		for (auto iter : tableIndexInfo)
		{
			int pkCount = 0, ukCount = 0, indexCount = 0;
			META::tableMeta* meta = getMetaByObjectId(iter.first);
			if (meta == nullptr)
			{
				clearIndexList(neededIndexs);
				dsFailedAndLogIt(1, "can not find index " << iter.first << " in table meta list", ERROR);
			}
			for (auto titer : iter.second)
			{
				dsReturnIfFailedWithOp(meta->prepareUnionKey(titer->keyMeta), clearIndexList(neededIndexs));
				if (titer->keyMeta->keyType == TID(META::KEY_TYPE::PRIMARY_KEY))
					pkCount++;
				else if (titer->keyMeta->keyType == TID(META::KEY_TYPE::UNIQUE_KEY))
					ukCount++;
				else
					indexCount++;
			}

			if (pkCount > 1)
			{
				clearIndexList(neededIndexs);
				dsFailedAndLogIt(1, "table " << meta->m_dbName << "." << meta->m_tableName << " has more than one primary key", ERROR);
			}
			if (ukCount > 0)
			{
				meta->m_uniqueKeyNames = new std::string[ukCount];
				meta->m_uniqueKeys = new  META::unionKeyMeta * [ukCount];
			}
			if (indexCount > 0)
			{
				meta->m_indexNames = new std::string[indexCount];
				meta->m_indexs = new  META::unionKeyMeta * [indexCount];
			}

			for (auto titer : iter.second)
			{
				if (titer->keyMeta->keyType == TID(META::KEY_TYPE::PRIMARY_KEY))
				{
					meta->m_primaryKey = titer->keyMeta;
					meta->m_primaryKeyName = titer->indexName;
				}
				else if (titer->keyMeta->keyType == TID(META::KEY_TYPE::UNIQUE_KEY))
				{
					meta->m_uniqueKeys[meta->m_uniqueKeysCount] = titer->keyMeta;
					meta->m_uniqueKeyNames[meta->m_uniqueKeysCount] = titer->indexName;
					meta->m_uniqueKeysCount++;
				}
				else
				{
					meta->m_indexs[meta->m_indexCount] = titer->keyMeta;
					meta->m_indexNames[meta->m_indexCount] = titer->indexName;
					meta->m_indexCount++;
				}
				titer->keyMeta = nullptr;
			}
		}
		clearIndexList(neededIndexs);
		dsOk();
	}

	DLL_EXPORT oracleMetaDataCollection::oracleMetaDataCollection(occiConnect* connector, const tableList* whiteList, const tableList* blackList) :metaDataCollection("utf8", true, nullptr, nullptr),
		m_connector(connector), m_conn(nullptr), m_whiteList(whiteList), m_blackList(blackList)
	{
	}
	DLL_EXPORT oracleMetaDataCollection::~oracleMetaDataCollection()
	{
		if (m_conn != nullptr)
			m_connector->close(m_conn);
	}
	DLL_EXPORT inline META::tableMeta* oracleMetaDataCollection::getMetaByObjectId(uint64_t objectId, uint64_t originCheckPoint)
	{
		spp::sparse_hash_map<uint64_t, META::MetaTimeline<META::tableMeta>*>::const_iterator titer = m_tables.find(objectId);
		if (titer == m_tables.end())
			return nullptr;
		return titer->second->get(originCheckPoint);
	}
	DLL_EXPORT DS oracleMetaDataCollection::init(META::metaDataCollection* collection)
	{
		DS s;
		createConnect();
		std::list<std::pair<std::string, int32_t>> userList;
		if (!dsCheck(s = getUserList(userList)))
			dsReturn(s);
		else if (s != 0)
			dsFailedAndLogIt(1, "do not get user:SYS from SYS.ALL_USERS", ERROR);
		for (auto iter : userList)
		{
			std::map<uint64_t, std::string> tableList;
			std::map<uint64_t, std::string> indexList;
			std::map<std::string, std::list<uint64_t>*> partitionList;
			if (iter.first.compare("SYS") == 0)
			{
				if (!dsCheck(s = getSysTableInfo(iter.second, tableList)))
					dsReturn(s);
				else if (s != 0)
					dsFailedAndLogIt(1, "do not get SYS.OBJ$ and SYS.COL$ from SYS.OBJ$", ERROR);
				put(iter.first.c_str(), &charsets[static_cast<int>(m_charsetId)], 0);
				dsReturnIfFailed(initUserTableMeta(iter.first, iter.second, tableList));
			}
			else
			{
				put(iter.first.c_str(), &charsets[static_cast<int>(m_charsetId)], 0);
				dsReturnIfFailedWithOp(getUserObjectList(iter.second, iter.first, tableList, indexList, partitionList), clearPartitionsList(partitionList));
				dsReturnIfFailedWithOp(initUserTableMeta(iter.first, iter.second, tableList), clearPartitionsList(partitionList));
				dsReturnIfFailedWithOp(initPartitons(iter.first, partitionList), clearPartitionsList(partitionList));
				clearPartitionsList(partitionList);
				dsReturnIfFailed(initIndexs(iter.first, tableList, indexList));
			}
		}
		dsOk();
	}

}