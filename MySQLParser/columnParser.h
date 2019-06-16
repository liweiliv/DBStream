/*
 * columnParser.h
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */

#ifndef COLUMNPARSER_H_
#define COLUMNPARSER_H_

/*
 * columnParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <assert.h>
#include "BR.h"
#include "MD.h"
struct _memp_ring;
class parseMem;
int parse_MYSQL_TYPE_TINY(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_SHORT(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_INT24(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_LONG(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_LONGLONG(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_FLOAT(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_DOUBLE(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_NEWDECIMAL(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_TIMESTAMP(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_TIMESTAMP2(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_DATETIME2(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_DATETIME(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_TIME2(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_NEWDATE(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_YEAR(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_STRING(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_BIT(IColMeta * colMeta, const uint8_t *meta, parseMem * mem, const char *& data,
		char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_VAR_STRING(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_BLOB(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_JSON(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_SET(IColMeta * colMeta, const uint8_t *meta, parseMem * mem, const char *& data,
		char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_ENUM(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);
int parse_MYSQL_TYPE_GEOMETRY(IColMeta * colMeta, const uint8_t *meta, parseMem * mem,
		const char *& data, char *& parsedData, uint32_t &parsedDataSize);

uint32_t lengthOf_MYSQL_TYPE_NEWDECIMAL(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_TIMESTAMP2(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_DATETIME2(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_TIME2(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_BIT(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_JSON(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_SET(IColMeta * colMeta, const uint8_t *meta,const char * data);
uint32_t lengthOf_MYSQL_TYPE_ENUM(IColMeta * colMeta, const uint8_t *meta,const char * data);
#endif /* COLUMNPARSER_H_ */
