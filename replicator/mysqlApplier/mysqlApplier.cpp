#include "mysqlApplier.h"
#include "meta/columnType.h"
#include "util/dtoa.h"
namespace REPLICATOR
{
#define NOT_FIXED_DEC 31
	int mysqlApplier::createSingleDMLSql(replicatorRecord* record, char*& sqlBuffer, int64_t& sqlBufferSize)
	{
		DATABASE_INCREASE::DMLRecord* dml = static_cast<DATABASE_INCREASE::DMLRecord*>(record->record);
		if (m_sqlBufferPos + record->record->head->minHead.size * 3 < sqlBufferSize)
			reallocSqlBuf(sqlBuffer, sqlBufferSize, record->record->head->minHead.size * 3);
		if (record->record->head->minHead.type == DATABASE_INCREASE::R_INSERT)
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "INSERT INTO ", sizeof("INSERT INTO ") - 1);
			m_sqlBufferPos += sizeof("INSERT INTO ") - 1;
			addTableName(sqlBuffer, dml->meta);
			memcpy(sqlBuffer + m_sqlBufferPos, " SET ", sizeof(" SET ") - 1);
			m_sqlBufferPos += sizeof(" SET ") - 1;
			for (int idx = 0; idx < dml->meta->m_columnsCount; idx++)
			{

			}
		}
		else if (record->record->head->minHead.type == DATABASE_INCREASE::R_DELETE)
		{

		}
		else
		{

		}
		return 0;
	}
	int mysqlApplier::addColumnValue(char* sqlBuffer, const META::columnMeta* column, DATABASE_INCREASE::DMLRecord* dml)
	{
		switch (column->m_columnType)
		{
		case  META::T_INT32:
		{
			m_sqlBufferPos += i32toa_sse2(*(const int32_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_UINT32:
		{
			m_sqlBufferPos += u32toa_sse2(*(const uint32_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_INT64:
		{
			m_sqlBufferPos += i64toa_sse2(*(const int64_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_DATETIME:
		{
			META::dateTime t;
			t.time = *(const int64_t*)dml->column(column->m_columnIndex);
			m_sqlBufferPos += t.toString(sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_TIMESTAMP:
		{
			META::timestamp t;
			t.time = *(const uint64_t*)dml->column(column->m_columnIndex);
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
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, dml->column(column->m_columnIndex), dml->varColumnSize(column->m_columnIndex));
			sqlBuffer[m_sqlBufferPos++] = '\'';
			break;
		}
		case META::T_JSON://todo
		{
			memcpy(sqlBuffer + m_sqlBufferPos, "CONVERT(", sizeof("CONVERT(") - 1);
			m_sqlBufferPos += sizeof("CONVERT(") - 1;
			sqlBuffer[m_sqlBufferPos++] = '\'';
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, dml->column(column->m_columnIndex), dml->varColumnSize(column->m_columnIndex));
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
			memcpy(sqlBuffer + m_sqlBufferPos, dml->column(column->m_columnIndex), dml->varColumnSize(column->m_columnIndex));
			m_sqlBufferPos += dml->varColumnSize(column->m_columnIndex);
			break;
		}
		case META::T_ENUM:
		{
			uint16_t idx = *(const uint16_t*)dml->column(column->m_columnIndex);
			if (idx >= column->m_setAndEnumValueList.m_Count)
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
			m_sqlBufferPos += mysql_real_escape_string(m_connect, sqlBuffer + m_sqlBufferPos, dml->column(column->m_columnIndex), dml->varColumnSize(column->m_columnIndex));
			sqlBuffer[m_sqlBufferPos++] = ')';
			break;
		}
		case META::T_FLOAT:
		{
			float f = *(const float*)dml->column(column->m_columnIndex);
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
			m_sqlBufferPos+=my_fcvt(*(const double*)dml->column(column->m_columnIndex), column->m_decimals, sqlBuffer + m_sqlBufferPos, nullptr);
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
			t.time = *(const int64_t*)dml->column(column->m_columnIndex);
			m_sqlBufferPos += t.toString(sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_SET:
		case META::T_UINT64:
		{
			m_sqlBufferPos += u64toa_sse2(*(const uint64_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_INT8:
		{
			m_sqlBufferPos += i32toa_sse2(dml->column(column->m_columnIndex)[0], sqlBuffer + m_sqlBufferPos);
			break;
		}
		case META::T_UINT8:
		{
			m_sqlBufferPos += u32toa_sse2(((const uint8_t*)dml->column(column->m_columnIndex))[0], sqlBuffer + m_sqlBufferPos);
			break;
		}
		case  META::T_INT16:
		{
			m_sqlBufferPos += i32toa_sse2(*(const int16_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
			break;
		}
		case  META::T_YEAR:
		case META::T_UINT16:
		{
			m_sqlBufferPos += u32toa_sse2(*(const uint16_t*)dml->column(column->m_columnIndex), sqlBuffer + m_sqlBufferPos);
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