/*
 * mysqlTypes.h
 *
 *  Created on: 2018年12月5日
 *      Author: liwei
 */

#ifndef MYSQLTYPES_H_
#define MYSQLTYPES_H_
#include "mysql.h"
#include "columnType.h"
	constexpr uint8_t mysqlTypeMaps[] = { T_DECIMAL,T_INT8,T_INT16,T_INT32,T_FLOAT,T_DOUBLE,T_MAX_TYPE,T_TIMESTAMP,T_INT64,T_INT32,T_DATE,T_TIME,T_DATETIME,T_YEAR,T_DATE,T_STRING,T_BYTE,T_TIMESTAMP,T_DATETIME,T_TIME,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,
	T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_MAX_TYPE,T_JSON,T_DECIMAL,T_ENUM,T_SET,T_BLOB,T_BLOB,T_BLOB,T_BLOB,T_STRING,T_STRING,T_GEOMETRY
	};


#endif /* MYSQLTYPES_H_ */
