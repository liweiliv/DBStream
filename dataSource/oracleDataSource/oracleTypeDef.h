#pragma once
#include "meta/columnType.h"
#include "meta/metaData.h"
namespace DATA_SOURCE
{
	enum class ORACLE_OBJ_TYPE {
		nextObject = 0,
		index = 1,
		table = 2,
		cluster = 3,
		view = 4,
		synonym = 5,
		sequence = 6,
		procedure = 7,
		function = 8,
		package = 9,
		packageBody = 11,
		trigger = 12,
		type = 13,
		typeBody = 14,
		tablePartition = 19,
		indexPartition = 20,
		lob = 21,
		library = 22,
		directory = 23,
		queue = 24,
		javaSource = 28,
		javaClass = 29,
		javaResource = 30,
		indextype = 32,
		Operator = 33,
		tableSubpartition = 34,
		indexSubpartition = 35,
		lobPartition = 40,
		lobSubpartition = 41,
		dimension = 43,
		context = 44,
		ruleSet = 46,
		resourcePlan = 47,
		consumerGroup = 48,
		subscription = 51,
		location = 52,
		xmlSchema = 55,
		javaData = 56,
		edition = 57,
		rule = 59,
		capture = 60,
		apply = 61,
		evaluationContext = 62,
		job = 66,
		program = 67,
		jobClass = 68,
		windows = 69,
		schedulerGroup = 72,
		schedule = 74,
		chain = 79,
		fileGroup = 81,
		miningModel = 82,
		credentia = 90,
		cubeDimension = 92,
		cube = 93,
		measureFolder = 94,
		cubeBuildProcess = 95,
		fileWatcher = 100,
		destination = 101
	};

	enum class ORACLE_COLUMN_TYPE
	{
		varchar2 = 1,//if charsetform  = 2, type is nvarchar2, otherwise type is varchar2
		number = 2,
		Long = 8,
		varchar = 9,//if charsetform  = 2, type is nchar varying, otherwise type is varchar
		date = 12,
		raw = 23,
		longRaw = 24,
		userDef = 58,
		rowId = 69,
		Char = 96,//if charsetform  = 2, type is nchar, otherwise type is char
		binaryFloat = 100,
		binaryDouble = 101,
		mlslabel = 105,
		mlslabel_1 = 106,
		userDef_1 = 111,
		clob = 112,// if charsetform  = 2, type is nclob, otherwise type is clob
		blob = 113,
		bfile = 114,
		cfile = 115,
		userDef_2 = 121,
		userDef_3 = 122,
		userDef_4 = 123,
		Time = 178,
		timeWithTimeZone = 179,
		timestamp = 180,
		timestampWithTimeZone = 181,
		intervalYearToMonth = 182,
		intervalDayToSecond = 183,
		urowId = 208,
		timestampWithTimeLocalZone = 231
	};
	static inline META::COLUMN_TYPE translateType(META::ColumnMeta* col)
	{
		switch (static_cast<ORACLE_COLUMN_TYPE>(col->m_srcColumnType))
		{
		case ORACLE_COLUMN_TYPE::varchar2:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::number:
			if (col->m_decimals == 0)
			{
				if (col->m_size < 2)
					return META::COLUMN_TYPE::T_INT8;
				else if (col->m_size < 5)
					return META::COLUMN_TYPE::T_INT16;
				else if (col->m_size < 10)
					return META::COLUMN_TYPE::T_INT32;
				else if (col->m_size <= 22)//oracle int is number(22,0), its range is granter than T_INT64, but we still use T_INT64 to store number(22,0) for performence
					return META::COLUMN_TYPE::T_INT64;
				else
					return META::COLUMN_TYPE::T_BIG_NUMBER;
			}
			else
			{
				return META::COLUMN_TYPE::T_BIG_NUMBER;
			}
		case ORACLE_COLUMN_TYPE::Long:
			return  META::COLUMN_TYPE::T_TEXT;
		case ORACLE_COLUMN_TYPE::varchar:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::date:
			return META::COLUMN_TYPE::T_DATETIME;
		case ORACLE_COLUMN_TYPE::raw:
			return META::COLUMN_TYPE::T_BINARY;
		case ORACLE_COLUMN_TYPE::longRaw:
			return  META::COLUMN_TYPE::T_BLOB;
		case ORACLE_COLUMN_TYPE::userDef:
			return  META::COLUMN_TYPE::T_XML;
		case ORACLE_COLUMN_TYPE::rowId:
			return  META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::Char:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::binaryFloat:
			return META::COLUMN_TYPE::T_FLOAT;
		case ORACLE_COLUMN_TYPE::binaryDouble:
			return META::COLUMN_TYPE::T_DOUBLE;
		case ORACLE_COLUMN_TYPE::mlslabel://not support
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::mlslabel_1:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::userDef_1:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::clob:
			return META::COLUMN_TYPE::T_TEXT;
		case ORACLE_COLUMN_TYPE::blob:
			return META::COLUMN_TYPE::T_BLOB;
		case ORACLE_COLUMN_TYPE::bfile:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::cfile:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::userDef_2:
			return META::COLUMN_TYPE::T_GEOMETRY;
		case ORACLE_COLUMN_TYPE::userDef_3:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::userDef_4:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::Time:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		case ORACLE_COLUMN_TYPE::timeWithTimeZone:
			return META::COLUMN_TYPE::T_DATETIME_ZERO_TZ;
		case ORACLE_COLUMN_TYPE::intervalYearToMonth:
			return META::COLUMN_TYPE::T_INTERVER_YEAR_TO_MONTH;
		case ORACLE_COLUMN_TYPE::intervalDayToSecond:
			return META::COLUMN_TYPE::T_INTERVER_DAY_TO_SECOND;
		case ORACLE_COLUMN_TYPE::urowId:
			return META::COLUMN_TYPE::T_STRING;
		case ORACLE_COLUMN_TYPE::timestampWithTimeLocalZone:
			return META::COLUMN_TYPE::T_DATETIME_ZERO_TZ;
		default:
			return META::COLUMN_TYPE::T_MAX_TYPE;
		}
	}
}