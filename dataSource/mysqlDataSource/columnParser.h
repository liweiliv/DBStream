/*
 * columnParser.h
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */

#ifndef COLUMNPARSER_H_
#define COLUMNPARSER_H_
#include "meta/metaData.h"
#include "message/record.h"
#include <assert.h>
namespace DATA_SOURCE {
	int parse_MYSQL_TYPE_TINY(const META::ColumnMeta* colMeta, RPC::DMLRecord * record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_SHORT(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_INT24(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_LONG(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_LONGLONG(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_FLOAT(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_DOUBLE(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_NEWDECIMAL(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_TIMESTAMP(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_TIMESTAMP2(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_DATETIME2(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_DATETIME(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_TIME2(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_NEWDATE(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_YEAR(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_STRING(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_BIT(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_VAR_STRING(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_BLOB(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_JSON(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_SET(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_ENUM(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
	int parse_MYSQL_TYPE_GEOMETRY(const META::ColumnMeta* colMeta, RPC::DMLRecord* record,
		const char*& data, bool newOrOld);
}
#endif /* COLUMNPARSER_H_ */

