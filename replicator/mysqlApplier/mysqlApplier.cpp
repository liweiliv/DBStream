#include "mysqlApplier.h"
#include "replicator/tableInfo.h"
#include "meta/columnType.h"
#include "util/dtoa.h"
namespace REPLICATOR
{
#define TXN_TABLE_TAIL  "ON DUPLICATE KEY UPDATE checkpoint=VALUES(checkpoint),timestamp=VALUES(timestamp);\n"
#define NOT_FIXED_DEC 31
	int mysqlApplier::optimize(transaction* t, char*& sqlBuffer)
	{
		spp::sparse_hash_map<uint64_t, replicatorRecord*> insertMaps;
		spp::sparse_hash_map<uint64_t, replicatorRecord*> deleteMaps;
		spp::sparse_hash_map<uint64_t, replicatorRecord*> updateMaps;

		transaction* current = t;
		while (current != nullptr)
		{
			replicatorRecord* record = current->firstRecord;
			if (likely(record != nullptr))
			{
				while (true)
				{
					if (record->mergeNext == nullptr || record->mergeNext->record->head->recordId > record->mergePrev->record->head->recordId)
					{
						if (!m_batchCommit && unlikely(runSql(sqlBuffer) != 0))
							return -1;
					}
					if (record == current->lastRecord)
						break;
					record = record->nextInTrans;
				}
			}
			current = t->mergeNext;
		}
	}
	void mysqlApplier::createTxnTableSql(char * sqlBuf,const DATABASE_INCREASE::record* record)
	{
		memcpy(sqlBuf + m_sqlBufferPos, m_txnTableSqlHead.c_str(), m_txnTableSqlHead.size());
		m_sqlBufferPos += u32toa_sse2(m_id, sqlBuf + m_sqlBufferPos);
		sqlBuf[m_sqlBufferPos++] = ',';
		m_sqlBufferPos += u64toa_sse2(record->head->logOffset, sqlBuf + m_sqlBufferPos);
		sqlBuf[m_sqlBufferPos++] = ',';
		m_sqlBufferPos += u64toa_sse2(record->head->timestamp, sqlBuf + m_sqlBufferPos);
		sqlBuf[m_sqlBufferPos++] = ')';
		memcpy(sqlBuf + m_sqlBufferPos, TXN_TABLE_TAIL, sizeof(TXN_TABLE_TAIL) - 1);
		m_sqlBufferPos += sizeof(TXN_TABLE_TAIL) - 1;
	}
	/*pic an unique key which all columns of it are not null*/
	static const META::keyInfo* picUniqueKey(const tableInfo* table, const DATABASE_INCREASE::DMLRecord* dml,bool newOrOld)
	{
		const META::keyInfo* key = nullptr,*prev = nullptr;
		for (int idx = 0; idx < table->destMeta->m_uniqueKeysCount; idx++)
		{
			if (table->destMeta->m_uniqueKeys[idx].count == 1)
			{
				if (newOrOld)
				{
					if (!dml->columnIsNull(table->columnMap[table->destMeta->m_uniqueKeys[idx].keyIndexs[0]]))
					{
						return &table->destMeta->m_uniqueKeys[idx];
					}
				}
				else
				{
					if (!dml->oldColumnIsNull(table->columnMap[table->destMeta->m_uniqueKeys[idx].keyIndexs[0]]))
					{
						return &table->destMeta->m_uniqueKeys[idx];
					}
				}
			}
			else if(prev == nullptr)
			{
				bool hasNullValue = false;
				for (int i = 0; i < table->destMeta->m_uniqueKeys[idx].count; i++)
				{
					if (newOrOld)
					{
						if (dml->columnIsNull(table->columnMap[table->destMeta->m_uniqueKeys[idx].keyIndexs[i]]))
						{
							hasNullValue = true;
							break;
						}
					}
					else
					{
						if (dml->oldColumnIsNull(table->columnMap[table->destMeta->m_uniqueKeys[idx].keyIndexs[i]]))
						{
							hasNullValue = true;
							break;
						}
					}
				}
				if (!hasNullValue)
					prev = &table->destMeta->m_uniqueKeys[idx];
			}
		}
		return prev;
	}
	int mysqlApplier::createWhereConditionInSql(char*& sqlBuffer, DATABASE_INCREASE::DMLRecord* dml)
	{
		const tableInfo* table = static_cast<const tableInfo*>(dml->meta->userData);
		memcpy(sqlBuffer + m_sqlBufferPos, " WHERE ", sizeof(" WHERE ") - 1);
		m_sqlBufferPos += sizeof(" WHERE ") - 1;
		const META::keyInfo* key = nullptr;
		if (table->destMeta->m_primaryKey.count > 0)
			key = &table->destMeta->m_primaryKey;
		else if (table->destMeta->m_uniqueKeysCount > 0)
			key = picUniqueKey(table, dml, dml->head->minHead.type == DATABASE_INCREASE::R_DELETE);
		if (key != nullptr)
		{
			for (int idx = 0; idx < key->count; idx++)
			{
				if (idx != 0)
				{
					memcpy(sqlBuffer + m_sqlBufferPos, " AND ", sizeof(" AND ") - 1);
					m_sqlBufferPos += sizeof(" AND ") - 1;
				}
				memcpy(sqlBuffer + m_sqlBufferPos, table->escapedColumnNames[key->keyIndexs[idx]].c_str(), table->escapedColumnNames[key->keyIndexs[idx]].size());
				m_sqlBufferPos += table->escapedColumnNames[key->keyIndexs[idx]].size();

				if (TEST_BITMAP(dml->nullBitmap, table->columnMap[idx]))
				{
					LOG(ERROR)<<"unexpect null value in record key column:"<<table->meta->m_columns[table->columnMap[idx]].m_columnName<<",record checkpoint is:"<<dml->head->logOffset;
					return -1;
				}
				else
				{
					sqlBuffer[m_sqlBufferPos++] = '=';
					if (0 != addColumnValue(sqlBuffer, &dml->meta->m_columns[table->columnMap[key->keyIndexs[idx]]], dml,dml->head->minHead.type==DATABASE_INCREASE::R_DELETE))
					{
						return -1;
					}
				}
			}
		}
		else // no pk ,and no uk or all uk has null value,put all columns to where condition  
		{
			bool columnSetted = false,notSetLobColumn = true;
			int idx = 0;
RESET:
			for (idx = 0; idx < table->destMeta->m_columnsCount; idx++)
			{
				/*often we do not add lob,text column to where condition for it is too slow and will increase pressure of database server */
				if (notSetLobColumn&&(dml->meta->m_columns[table->columnMap[idx]].m_columnType == META::T_BLOB||
					dml->meta->m_columns[table->columnMap[idx]].m_columnType == META::T_TEXT))
					continue;
				if (columnSetted)
				{
					memcpy(sqlBuffer + m_sqlBufferPos, " AND ", sizeof(" AND ") - 1);
					m_sqlBufferPos += sizeof(" AND ") - 1;
				}
				else
					columnSetted = true;

				memcpy(sqlBuffer + m_sqlBufferPos, table->escapedColumnNames[idx].c_str(), table->escapedColumnNames[idx].size());
				m_sqlBufferPos += table->escapedColumnNames[idx].size();

				if (TEST_BITMAP(dml->nullBitmap, table->columnMap[idx]))
				{
					memcpy(sqlBuffer + m_sqlBufferPos, "is null", sizeof("is null") - 1);
					m_sqlBufferPos += sizeof("is null") - 1;
				}
				else
				{
					sqlBuffer[m_sqlBufferPos++] = '=';
					if (0 != addColumnValue(sqlBuffer, &dml->meta->m_columns[table->columnMap[idx]], dml, dml->head->minHead.type == DATABASE_INCREASE::R_DELETE))
					{
						return -1;
					}
				}
			}
			if (!columnSetted)//all column is lob or text,we have to add them to where condition
			{
				notSetLobColumn = false;
				goto RESET;
			}
			memcpy(sqlBuffer + m_sqlBufferPos, " LIMIT 1", sizeof(" LIMIT 1") - 1);
			m_sqlBufferPos += sizeof(" LIMIT 1") - 1;
		}

		return 0;
	}

	int mysqlApplier::createSingleDMLSql(replicatorRecord* record, char*& sqlBuffer)
	{
		DATABASE_INCREASE::DMLRecord* dml = static_cast<DATABASE_INCREASE::DMLRecord*>(record->record);
		const tableInfo* table = static_cast<const tableInfo*>(dml->meta->userData);
		if (m_sqlBufferPos + record->record->head->minHead.size * 3 < m_sqlBufferSize)
			reallocSqlBuf(sqlBuffer, record->record->head->minHead.size * 3);
		if (record->record->head->minHead.type == DATABASE_INCREASE::R_INSERT)
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "INSERT INTO ", sizeof("INSERT INTO ") - 1);
			m_sqlBufferPos += sizeof("INSERT INTO ") - 1;
			memcpy(sqlBuffer + m_sqlBufferPos, table->escapedTableNmae.c_str(), table->escapedTableNmae.size());
			m_sqlBufferPos += table->escapedTableNmae.size();
			memcpy(sqlBuffer + m_sqlBufferPos, " SET ", sizeof(" SET ") - 1);
			m_sqlBufferPos += sizeof(" SET ") - 1;
			for (int idx = 0; idx < table->destMeta->m_columnsCount; idx++)
			{
				if (table->destMeta->m_columns[idx].m_generated)
					continue;
				memcpy(sqlBuffer + m_sqlBufferPos, table->escapedColumnNames[idx].c_str(), table->escapedColumnNames[idx].size());
				m_sqlBufferPos += table->escapedColumnNames[idx].size();
				sqlBuffer[m_sqlBufferPos++] = '=';
				if (TEST_BITMAP(dml->nullBitmap, table->columnMap[idx]))
				{
					memcpy(sqlBuffer + m_sqlBufferPos, "null", sizeof("null") - 1);
					m_sqlBufferPos += sizeof("null") - 1;
				}
				else if (0 != addColumnValue(sqlBuffer, &dml->meta->m_columns[table->columnMap[idx]], dml,true))
				{
					return -1;
				}
				sqlBuffer[m_sqlBufferPos++] = ',';
			}
			m_sqlBufferPos--;//delete last [,]
			return 0;
		}
		else if (record->record->head->minHead.type == DATABASE_INCREASE::R_DELETE)
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "DELETE ", sizeof("DELETE ") - 1);
			m_sqlBufferPos += sizeof("DELETE ") - 1;
			memcpy(sqlBuffer + m_sqlBufferPos, table->escapedTableNmae.c_str(), table->escapedTableNmae.size());
			m_sqlBufferPos += table->escapedTableNmae.size();
			return createWhereConditionInSql(sqlBuffer, dml);
		}
		else
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "UPDATE ", sizeof("UPDATE ") - 1);
			m_sqlBufferPos += sizeof("UPDATE ") - 1;
			memcpy(sqlBuffer + m_sqlBufferPos, table->escapedTableNmae.c_str(), table->escapedTableNmae.size());
			m_sqlBufferPos += table->escapedTableNmae.size();
			memcpy(sqlBuffer + m_sqlBufferPos, " SET ", sizeof(" SET ") - 1);
			m_sqlBufferPos += sizeof(" SET ") - 1;
			uint16_t updatedColumnCount = 0;
			for (int idx = 0; idx < table->destMeta->m_columnsCount; idx++)
			{
				if (TEST_BITMAP(dml->updatedBitmap, table->columnMap[idx])&& !table->destMeta->m_columns[idx].m_generated)
				{
					memcpy(sqlBuffer + m_sqlBufferPos, table->escapedColumnNames[idx].c_str(), table->escapedColumnNames[idx].size());
					m_sqlBufferPos += table->escapedColumnNames[idx].size();

					sqlBuffer[m_sqlBufferPos++] = '=';
					if (TEST_BITMAP(dml->nullBitmap, table->columnMap[idx]))
					{
						memcpy(sqlBuffer + m_sqlBufferPos, "null", sizeof("null") - 1);
						m_sqlBufferPos += sizeof("null") - 1;
					}
					else if (0 != addColumnValue(sqlBuffer, &dml->meta->m_columns[table->columnMap[idx]], dml,true))
					{
						return -1;
					}
					sqlBuffer[m_sqlBufferPos++] = ',';
					updatedColumnCount++;
				}
			}
			if (updatedColumnCount == 0)//no column updated ,ignore
			{
				m_sqlBufferPos = 0;
				return 0;
			}
			m_sqlBufferPos--;//delete last [,]
			return createWhereConditionInSql(sqlBuffer, dml);
		}
	}
	int mysqlApplier::createMergedDMLSql(replicatorRecord* record, char*& sqlBuffer)
	{
		return 0;
	}
	int mysqlApplier::addColumnValue(char* sqlBuffer, const META::columnMeta* column, DATABASE_INCREASE::DMLRecord* dml,bool newOrOld)
	{
		const char* value = newOrOld ? dml->column(column->m_columnIndex) : dml->oldColumnOfUpdateType(column->m_columnIndex);
		switch (column->m_columnType)
		{
		case  META::T_INT32:
		{
			m_sqlBufferPos += i32toa_sse2(*(const int32_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_UINT32:
		{
			m_sqlBufferPos += u32toa_sse2(*(const uint32_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_INT64:
		{
			m_sqlBufferPos += i64toa_sse2(*(const int64_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_DATETIME:
		{
			META::dateTime t;
			t.time = *(const int64_t*)value;
			m_sqlBufferPos += t.toString(sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_TIMESTAMP:
		{
			META::timestamp t;
			t.time = *(const uint64_t*)value;
			memcpy(sqlBuffer + m_sqlBufferPos, "FROM_UNIXTIME(", sizeof("FROM_UNIXTIME(") - 1);
			m_sqlBufferPos += sizeof("FROM_UNIXTIME(") - 1;
			m_sqlBufferPos += u32toa_sse2(t.seconds, sqlBuffer + m_sqlBufferPos);
			if (t.nanoSeconds != 0)
			{
				sqlBuffer[m_sqlBufferPos++] = '.';
				m_sqlBufferPos += u32toa_sse2(t.nanoSeconds / 1000, sqlBuffer + m_sqlBufferPos);
			}
			sqlBuffer[m_sqlBufferPos++] = ')';
			break;
		}
		case META::T_STRING:
		case META::T_TEXT:
			if (column->m_charset != nullptr)
			{
				sqlBuffer[m_sqlBufferPos++] = '_';
				memcpy(sqlBuffer + m_sqlBufferPos, column->m_charset->name, column->m_charset->nameSize);
				m_sqlBufferPos += column->m_charset->nameSize;
			}
		case META::T_BLOB:
		case  META::T_BINARY:
		{
			sqlBuffer[m_sqlBufferPos++] = '\'';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, value, newOrOld ? dml->varColumnSize(column->m_columnIndex) : dml->oldVarColumnSizeOfUpdateType(column->m_columnIndex, value));
			sqlBuffer[m_sqlBufferPos++] = '\'';
			break;
		}
		case META::T_JSON://todo
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "CONVERT(", sizeof("CONVERT(") - 1);
			m_sqlBufferPos += sizeof("CONVERT(") - 1;
			sqlBuffer[m_sqlBufferPos++] = '\'';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, value, newOrOld?dml->varColumnSize(column->m_columnIndex): dml->oldVarColumnSizeOfUpdateType(column->m_columnIndex,value));
			sqlBuffer[m_sqlBufferPos++] = '\'';
			sqlBuffer[m_sqlBufferPos++] = ',';
			memcpy(sqlBuffer + m_sqlBufferPos, "JSON", sizeof("JSON") - 1);
			m_sqlBufferPos += sizeof("JSON") - 1;
			sqlBuffer[m_sqlBufferPos++] = ')';
			break;
		}
		case META::T_DECIMAL:
		case META::T_BIG_NUMBER:
		{
			memcpy(sqlBuffer + m_sqlBufferPos, value, dml->varColumnSize(column->m_columnIndex));
			m_sqlBufferPos += dml->varColumnSize(column->m_columnIndex);
			break;
		}
		case META::T_ENUM:
		{
			uint16_t idx = *(const uint16_t*)value;
			if (idx >= column->m_setAndEnumValueList.m_count)
			{
				m_errno = -1;
				m_error = "invalid enum type value,enum size is less then index";
				return -1;
			}
			sqlBuffer[m_sqlBufferPos++] = '\'';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, column->m_setAndEnumValueList.m_array[idx], strlen(column->m_setAndEnumValueList.m_array[idx]));
			sqlBuffer[m_sqlBufferPos++] = '\'';
			break;
		}
		case META::T_GEOMETRY:
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "ST_GeomCollFromWKB(", sizeof("ST_GeomCollFromWKB(") - 1);
			m_sqlBufferPos += sizeof("ST_GeomCollFromWKB(") - 1;
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, value, newOrOld ? dml->varColumnSize(column->m_columnIndex) : dml->oldVarColumnSizeOfUpdateType(column->m_columnIndex, value));
			sqlBuffer[m_sqlBufferPos++] = ')';
			break;
		}
		case META::T_FLOAT:
		{
			float f = *(const float*)value;
			if (column->m_decimals >= NOT_FIXED_DEC)
				m_sqlBufferPos += my_gcvt(f, MY_GCVT_ARG_FLOAT, 10,
					sqlBuffer + m_sqlBufferPos, nullptr);
			else
				m_sqlBufferPos += my_fcvt(f, column->m_decimals,
					sqlBuffer + m_sqlBufferPos, nullptr);
			break;
		}
		case META::T_DOUBLE:
		{
			m_sqlBufferPos+=my_fcvt(*(const double*) value, column->m_decimals, sqlBuffer + m_sqlBufferPos, nullptr);
			break;
		}
		case  META::T_DATE:
		{
			META::Date t;
			t.time = *(const int32_t*)dml->column(column->m_columnIndex);
			m_sqlBufferPos += t.toString(sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_TIME:
		{
			META::Time t;
			t.time = *(const int64_t*)value;
			m_sqlBufferPos += t.toString(sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_SET:
		case META::T_UINT64:
		{
			m_sqlBufferPos += u64toa_sse2(*(const uint64_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_INT8:
		{
			m_sqlBufferPos += i32toa_sse2(value[0], sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_UINT8:
		{
			m_sqlBufferPos += u32toa_sse2(((const uint8_t*)value)[0], sqlBuffer + m_sqlBufferPos);
			break;
		}
		case  META::T_INT16:
		{
			m_sqlBufferPos += i32toa_sse2(*(const int16_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		case  META::T_YEAR:
		case META::T_UINT16:
		{
			m_sqlBufferPos += u32toa_sse2(*(const uint16_t*)value, sqlBuffer + m_sqlBufferPos);
			break;
		}
		default:
		{
			m_errno = -2;
			m_error = "unspport column type to write to mysql";
			return -1;
		}
		}
		return 0;
	}
}