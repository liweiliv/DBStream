#pragma once
#define T_UNION 0
#define T_UINT8 1
#define T_INT8  2
#define T_UINT16 3
#define T_INT16 4
#define T_UINT32 5
#define T_INT32 6
#define T_UINT64 7
#define T_INT64 8
#define T_BIG_NUMBER 9
#define T_FLOAT 10
#define T_DOUBLE 11
#define T_DECIMAL 12
#define T_TIMESTAMP 13
#define T_DATETIME 14
#define T_DATE 15
#define T_YEAR 16
#define T_TIME 17
#define T_BLOB 18
#define T_STRING 19
#define T_JSON 20
#define T_XML 21
#define T_GEOMETRY 22
#define T_SET 23
#define T_ENUM 24
#define T_MAX_TYPE 255
struct columnTypeInfo
{
	uint8_t type;
	uint8_t columnTypeSize;
	bool asIndex;
	bool fixed;
};
constexpr static columnTypeInfo columnInfos[] = {
{T_UNION,4,true,false,},
{T_UINT8, 1,true,true},
{T_INT8,1 ,true,true},
{T_UINT16,2,true,true},
{T_INT16, 2,true,true},
{T_UINT32,4,true,true},
{T_INT32,4 ,true,true},
{T_UINT64,8,true,true},
{T_INT64,8,true,true},
{T_BIG_NUMBER,4 ,false,false},
{T_FLOAT,4,true,true},
{T_DOUBLE,8,true,true},
{T_DECIMAL,4,true,true},
{T_TIMESTAMP,8 ,true,true},
{T_DATETIME,8,false,true},
{T_DATE,2,false,true},
{T_YEAR,1,false,true},
{T_TIME,4,false,true},
{T_BLOB,4,false,true},
{T_STRING,4,false,true},
{T_JSON,4,false,true},
{T_XML,4,false,true},
{T_GEOMETRY,4,false,true},
{T_SET,8,false,true},
{T_ENUM,2,false,true}
};
