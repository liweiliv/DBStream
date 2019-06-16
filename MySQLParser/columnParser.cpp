/*
 * columnParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <assert.h>
#include "BR.h"
#include "MD.h"
#include "MsgWrapper.h"
#include "my_time.h"
#include "my_decimal.h"
#include <decimal.h>
#include <m_string.h>
#include "json_binary.h"
#include "json_binary_to_string.h"
#include "MempRing.h"
#include "itoa.h"
#include "parseMem.h"
#include "Log_r.h"
#define test_bitmap(m,idx) ((m)[(idx)>>3]&(1<<((idx)&0x07)))
static uint16_t MAT[] =
{ 0x3030, 0x3130, 0x3230, 0x3330, 0x3430, 0x3530, 0x3630, 0x3730, 0x3830,
        0x3930, 0x3031, 0x3131, 0x3231, 0x3331, 0x3431, 0x3531, 0x3631, 0x3731,
        0x3831, 0x3931, 0x3032, 0x3132, 0x3232, 0x3332, 0x3432, 0x3532, 0x3632,
        0x3732, 0x3832, 0x3932, 0x3033, 0x3133, 0x3233, 0x3333, 0x3433, 0x3533,
        0x3633, 0x3733, 0x3833, 0x3933, 0x3034, 0x3134, 0x3234, 0x3334, 0x3434,
        0x3534, 0x3634, 0x3734, 0x3834, 0x3934, 0x3035, 0x3135, 0x3235, 0x3335,
        0x3435, 0x3535, 0x3635, 0x3735, 0x3835, 0x3935, 0x3036, 0x3136, 0x3236,
        0x3336, 0x3436, 0x3536, 0x3636, 0x3736, 0x3836, 0x3936, 0x3037, 0x3137,
        0x3237, 0x3337, 0x3437, 0x3537, 0x3637, 0x3737, 0x3837, 0x3937, 0x3038,
        0x3138, 0x3238, 0x3338, 0x3438, 0x3538, 0x3638, 0x3738, 0x3838, 0x3938,
        0x3039, 0x3139, 0x3239, 0x3339, 0x3439, 0x3539, 0x3639, 0x3739, 0x3839,
        0x3939 };

/**
 *   Calculate on-disk size of a timestamp value.
 *
 *     @param  dec  Precision.
 *
 *       The same formula is used to find the binary size of the packed numeric time
 *         in libbinlogevents/src/value.cpp calc_field_size().
 *           If any modification is done here the same needs to be done in the
 *             aforementioned method in libbinlogevents also.
 *             */
static inline  unsigned int my_timestamp_binary_length(unsigned int dec)
{
  assert(dec <= DATETIME_MAX_DECIMALS);
  return 4 + (dec + 1) / 2;
}
/**
 *   Calculate binary size of packed datetime representation.
 *     @param dec  Precision.
 *
 *       The same formula is used to find the binary size of the packed numeric time
 *         in libbinlogevents/src/value.cpp calc_field_size().
 *           If any modification is done here the same needs to be done in the
 *             aforementioned method in libbinlogevents also.
 *             */
static inline unsigned int my_datetime_binary_length(unsigned int dec)
{
	assert(dec <= DATETIME_MAX_DECIMALS);
	return 5 + (dec + 1) / 2;
}
/**
 *   Calculate binary size of packed numeric time representation.
 *
 *     @param   dec   Precision.
 *       The same formula is used to find the binary size of the packed numeric time
 *         in libbinlogevents/src/value.cpp calc_field_size().
 *           If any modification is done here the same needs to be done in the
 *             aforementioned method in libbinlogevents also.
 *             */
static inline unsigned int my_time_binary_length(unsigned int dec)
{
  assert(dec <= DATETIME_MAX_DECIMALS);
  return 3 + (dec + 1) / 2;
}

int parse_MYSQL_TYPE_TINY(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(4); //8 bit int max is 255
	if (colMeta->isSigned())
	{
		parsedDataSize = sitoa(*(int8_t* )(data), parsedData);
	}
	else
	{
		parsedDataSize = sutoa(*(uint8_t* )(data), parsedData);
	}
	data += 1;
	return 0;
}
int parse_MYSQL_TYPE_SHORT(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(6); //16 bit int max is 65535
	if (colMeta->isSigned())
	{
		parsedDataSize = sitoa(sint2korr(data), parsedData);
	}
	else
	{
		parsedDataSize = sutoa(uint2korr(data), parsedData);
	}
	data += 2;
	return 0;
}
int parse_MYSQL_TYPE_INT24(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(9); //24 bit int max is 16777215
	if (colMeta->isSigned())
	{
		parsedDataSize = itoa(sint3korr(data), parsedData);
	}
	else
	{
		parsedDataSize = utoa(uint3korr(data), parsedData);
	}
	data += 3;
	return 0;
}
int parse_MYSQL_TYPE_LONG(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(11); //32 bit int max is 4294967295
	if (colMeta->isSigned())
	{
		parsedDataSize = itoa(sint4korr(data), parsedData);
	}
	else
	{
		parsedDataSize = utoa(uint4korr(data), parsedData);
	}
	data += 4;
	return 0;
}
int parse_MYSQL_TYPE_LONGLONG(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(21); //64 bit int max is 18446744073709551615
	if (colMeta->isSigned())
	{
		parsedDataSize = ltoa(sint8korr(data), parsedData);
	}
	else
	{
		parsedDataSize = ultoa(uint8korr(data), parsedData);
	}
	data += 8;
	return 0;
}
int parse_MYSQL_TYPE_FLOAT(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	float f;
	float4get(&f, (const unsigned char*) data);
	parsedData = mem->alloc(11);
	if (colMeta->getDecimals() >= NOT_FIXED_DEC)
		parsedDataSize = my_gcvt(f, MY_GCVT_ARG_FLOAT, 10,
				parsedData, NULL);
	else
		parsedDataSize = my_fcvt(f, colMeta->getDecimals(),
				parsedData, NULL);
	data += 4;
	return 0;
}
int parse_MYSQL_TYPE_DOUBLE(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	double d;
	float8get(&d, (const unsigned char*) data);
	parsedData = mem->alloc(21);
	if (colMeta->getDecimals() >= NOT_FIXED_DEC)
		parsedDataSize = my_gcvt(d, MY_GCVT_ARG_DOUBLE, 20,
				parsedData, NULL);
	else
		parsedDataSize = my_fcvt(d, colMeta->getDecimals(),
				parsedData, NULL);
	data += 8;
	return 0;
}
uint32_t lengthOf_MYSQL_TYPE_NEWDECIMAL(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    uint32_t colLength = colMeta->getLength();
    uint8_t decs = colMeta->getDecimals();
    uint8_t prec = (decs == 0) ? (colLength - 1) : (colLength - 2);
    if (!colMeta->isSigned())
        prec++;
    return my_decimal_get_binary_size(prec, decs);
}
int parse_MYSQL_TYPE_NEWDECIMAL(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint32_t colLength = colMeta->getLength();
	parsedData = mem->alloc(parsedDataSize=201);
	uint8_t decs = colMeta->getDecimals();
	uint8_t prec = (decs == 0) ? (colLength - 1) : (colLength - 2);
	if (!colMeta->isSigned())
		prec++;
	decimal_digit_t dec_buf[10];
	decimal_t dec =
	{ 0, 0, 10, colMeta->isSigned(), dec_buf };
	bin2decimal((const uint8_t*) data, &dec, prec, decs);
	decimal2string(&dec, parsedData, (int*)&parsedDataSize, 0, 0, 0);
	parsedData[parsedDataSize] = 0;
	mem->revertMem(201 - parsedDataSize);
	data += my_decimal_get_binary_size(prec, decs);
	return 0;
}
int parse_MYSQL_TYPE_TIMESTAMP(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(14);
	parsedDataSize = utoa(*(uint32_t*)data,parsedData);
	data += 4;
	return 0;
}

uint32_t lengthOf_MYSQL_TYPE_TIMESTAMP2(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    return my_datetime_binary_length(colMeta->getDecimals());
}
int parse_MYSQL_TYPE_TIMESTAMP2(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint8_t decs = colMeta->getDecimals();
	parsedData = mem->alloc(14 + decs);
	struct timeval tm;
	my_timestamp_from_binary(&tm, (const uchar *) data, decs);
	parsedDataSize = my_timeval_to_str(&tm, parsedData, decs);
	data += my_timestamp_binary_length(decs);
	return 0;
}
uint32_t lengthOf_MYSQL_TYPE_DATETIME2(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    return my_datetime_binary_length(colMeta->getDecimals());
}
int parse_MYSQL_TYPE_DATETIME2(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint8_t decs = colMeta->getDecimals();
	parsedData = mem->alloc(22 + decs);
	MYSQL_TIME ltime;
	long long packed = my_datetime_packed_from_binary((const uchar *) data,
			decs);
	TIME_from_longlong_datetime_packed(&ltime, packed);
	parsedDataSize = my_datetime_to_str(&ltime, parsedData, decs);
	data += my_datetime_binary_length(decs);
	return 0;
}
int parse_MYSQL_TYPE_DATETIME(IColMeta * colMeta, const uint8_t *meta,
		parseMem * mem, const char *& data, char *& parsedData,
		uint32_t &parsedDataSize) 
{
	uint8_t decs = colMeta->getDecimals();
	parsedData = mem->alloc(22 + decs);
	long long i = *(long long*) (data);
	uint32_t A = i / 100000000;
	uint32_t YYYY = A / 100;
	if (YYYY > 1000 && YYYY < 9999) //YYYY
			{
		uint16_t B = ((uint16_t) YYYY) / 100;
		*(uint16_t *) parsedData = MAT[B];
		*(uint16_t *) (parsedData + 2) = MAT[YYYY - B * 100];
		parsedDataSize = 4;
	} else {
		parsedDataSize = itoa(YYYY, parsedData);
	}
	parsedData[parsedDataSize] = '-';
	uint32_t _A = i - A * 100000000;
	char *p = parsedData + parsedDataSize;
	*(uint16_t *) p = MAT[A - YYYY * 100]; //MM
	p[2] = '-';
	p += 3;
	register uint16_t C;
	uint16_t H = _A / 10000, L = _A - 10000 * H;
	C = H / 100;
	*(uint16_t *) p = MAT[C]; //DD
	p[2] = ' ';
	*(uint16_t *) (p + 3) = MAT[H - 100 * C]; //HH
	p[5] = ':';
	C = L / 100;
	*(uint16_t *) (p + 6) = MAT[C]; //mm
	p[8] = ':';
	*(uint16_t *) (p + 9) = MAT[L - 100 * C]; //SS
	parsedDataSize += 15;
	data+=8;
	return 0;
}
uint32_t lengthOf_MYSQL_TYPE_TIME2(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    return my_time_binary_length(colMeta->getDecimals());
}
int parse_MYSQL_TYPE_TIME2(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint8_t decs = colMeta->getDecimals();
	parsedData = mem->alloc(11 + decs);
	MYSQL_TIME ltime;
	long long packed = my_time_packed_from_binary((const uchar *) data, decs);
	TIME_from_longlong_time_packed(&ltime, packed);
	parsedDataSize = my_time_to_str(&ltime, parsedData, decs);
	data += my_time_binary_length(decs);
	return 0;
}
int parse_MYSQL_TYPE_NEWDATE(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint32 tmp = uint3korr(data);
	int part;
	parsedData = mem->alloc(11);
	memset(parsedData, 0, 11);
	char *pos = parsedData + 9;  // start from '\0' to the beginning
	part = (int) (tmp & 31);
	*pos-- = (char) ('0' + part % 10);
	*pos-- = (char) ('0' + part / 10);
	*pos-- = '-';
	part = (int) (tmp >> 5 & 15);
	*pos-- = (char) ('0' + part % 10);
	*pos-- = (char) ('0' + part / 10);
	*pos-- = '-';
	part = (int) (tmp >> 9);
	*pos-- = (char) ('0' + part % 10);
	part /= 10;
	*pos-- = (char) ('0' + part % 10);
	part /= 10;
	*pos-- = (char) ('0' + part % 10);
	part /= 10;
	*pos = (char) ('0' + part);
	parsedDataSize = 10;
	data += 3;
	return 0;
}
int parse_MYSQL_TYPE_YEAR(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = mem->alloc(5);
	sutoa((uint16_t )(1900 + (uint8_t )data[0]), parsedData);
	parsedDataSize = 4;
	data += 1;
	return 0;
}
int parse_MYSQL_TYPE_STRING(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedData = (char*)data;
	data += (parsedDataSize = colMeta->getLength());
	return 0;
}
uint32_t lengthOf_MYSQL_TYPE_BIT(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    return (colMeta->getLength()+7)>>3;
}

int parse_MYSQL_TYPE_BIT(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint32_t colLength = colMeta->getLength();
	uint64_t bitValue;
	uint8_t bitSize = (colLength + 7) >> 3;
	memcpy(&bitValue, data, bitSize);
	bitValue >>= (64 - colLength);
	parsedData = mem->alloc(21); //64 bit int max is 18446744073709551615
	parsedDataSize = ultoa(bitValue, parsedData);
	data += bitSize;
	return 0;
}
int parse_MYSQL_TYPE_VAR_STRING(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	parsedDataSize = colMeta->getLength();
	if (parsedDataSize < 256)
	{
		parsedDataSize = (uint) (*(uint8 *)(data) & 0xFF);
		data++;
	}
	else
	{
		parsedDataSize= uint2korr(data);
		data += 2;
	}
	parsedData = (char*)data;
	data += parsedDataSize;
	return 0;
}
int parse_MYSQL_TYPE_BLOB(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
                const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
        switch(meta[0])
	{
	case 1:
		parsedDataSize = *((const uint8_t*)data);
		data+=1;
		break;
	case 2:
		parsedDataSize = uint2korr(data);
		data+=2;
		break;
	case 3:
		parsedDataSize = uint3korr(data);
		data+=3;
		break;
	case 4:
		parsedDataSize = uint4korr(data);
		data+=4;
		break;
	default:
		return -1;
	}
        parsedData = (char*)data;
        data += parsedDataSize;
        return 0;
}
uint32_t lengthOf_MYSQL_TYPE_JSON(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    return uint4korr(data)+meta[0];
}
uint64_t  bjson2Str(const char * bjson,uint64_t bjsonSize,char * jsonStr);
int parse_MYSQL_TYPE_JSON(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint32_t size = uint4korr(data);
	parsedData = mem->alloc(size*8);
	parsedDataSize = bjson2Str(data+meta[0],size,parsedData);
	mem->revertMem(size*8 - parsedDataSize);
	data += size+meta[0];
	return 0;
}
uint32_t lengthOf_MYSQL_TYPE_SET(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    uint16_t bitmapSize ;
    IStrArray* setValues = colMeta->getValuesOfEnumSet();
    if (setValues != NULL)
    {
        bitmapSize=setValues->size();
        delete setValues;
        if(bitmapSize!= 0)
        {
            if(bitmapSize>32)
                return 8;
            else
                return ((bitmapSize+7)>>3);
        }
    }
    return 0;
}
int parse_MYSQL_TYPE_SET(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	const char * bitmap = data;
	uint16_t bitmapSize ;
	IStrArray* setValues = colMeta->getValuesOfEnumSet();
	if (setValues != NULL && (bitmapSize=setValues->size()) != 0)
	{
		if(bitmapSize>32)
			data += 8;
		else
			data += ((bitmapSize+7)>>3);
		uint32_t setSize = colMeta->getLength();
		const char * sv;
		uint64_t ss;
		parsedData = mem->alloc(setSize+bitmapSize);
		parsedDataSize = 0;
		for(uint8_t idx =0;idx<bitmapSize;idx++)
		{
			if(test_bitmap(bitmap,idx))
			{
				setValues->elementAt(idx , sv, ss);
				memcpy(parsedData+parsedDataSize,sv,--ss);//todo
				parsedDataSize+=ss;
				parsedData[parsedDataSize++] = ',';
			}
		}
		parsedDataSize--;//ignore last ','
		mem->revertMem(setSize+bitmapSize-parsedDataSize);
		delete setValues;
		return 0;
	}
	else
	{
		if(setValues != NULL)
			delete setValues;
		Log_r::Error("enum value sets is empty,meta is wrong");
		return -1;
	}
}
uint32_t lengthOf_MYSQL_TYPE_ENUM(IColMeta * colMeta, const uint8_t *meta,const char * data)
{
    if (colMeta->getLength() > 255)
        return 2;
    else
        return 1;
}
int parse_MYSQL_TYPE_ENUM(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
	uint16_t enumValueIndex = 0;
	if (colMeta->getLength() > 255)
	{
		enumValueIndex = uint2korr(data);
		data += 2;
	}
	else
	{
		enumValueIndex = *(data);
		data ++;
	}
	if (enumValueIndex == 0)
	{
		parsedData = NULL;
		parsedDataSize = 0;
	}
	IStrArray* setValues = colMeta->getValuesOfEnumSet();
	if (setValues != NULL && setValues->size() != 0)
	{
		const char * sv;
		uint64_t ss;
		setValues->elementAt(enumValueIndex - 1, sv, ss);
		parsedData = (char*) sv;
		parsedDataSize = ss-1;//todo
		delete setValues;
		return 0;
	}
	else
	{
		if(setValues != NULL)
			delete setValues;
		Log_r::Error("enum value sets is empty,meta is wrong");
		return -1;
	}
}
int parse_MYSQL_TYPE_GEOMETRY(IColMeta * colMeta, const uint8_t *meta,parseMem * mem,
		const char *& data,char *& parsedData,uint32_t &parsedDataSize)
{
    parsedDataSize = uint4korr(data);
    data += 4;
    parsedData = (char*)data;
    data += parsedDataSize;
    return 0;
}
#define JSONB_TYPE_SMALL_OBJECT   0x0
#define JSONB_TYPE_LARGE_OBJECT   0x1
#define JSONB_TYPE_SMALL_ARRAY    0x2
#define JSONB_TYPE_LARGE_ARRAY    0x3
#define JSONB_TYPE_LITERAL        0x4
#define JSONB_TYPE_INT16          0x5
#define JSONB_TYPE_UINT16         0x6
#define JSONB_TYPE_INT32          0x7
#define JSONB_TYPE_UINT32         0x8
#define JSONB_TYPE_INT64          0x9
#define JSONB_TYPE_UINT64         0xA
#define JSONB_TYPE_DOUBLE         0xB
#define JSONB_TYPE_STRING         0xC
#define JSONB_TYPE_OPAQUE         0xF
#define JSONB_NULL_LITERAL        '\x00'
#define JSONB_TRUE_LITERAL        '\x01'
#define JSONB_FALSE_LITERAL       '\x02'


/*
  The size of offset or size fields in the small and the large storage
  format for JSON objects and JSON arrays.
*/
#define SMALL_OFFSET_SIZE         2
#define LARGE_OFFSET_SIZE         4

/*
  The size of key entries for objects when using the small storage
  format or the large storage format. In the small format it is 4
  bytes (2 bytes for key length and 2 bytes for key offset). In the
  large format it is 6 (2 bytes for length, 4 bytes for offset).
*/
#define KEY_ENTRY_SIZE_SMALL      (2 + SMALL_OFFSET_SIZE)
#define KEY_ENTRY_SIZE_LARGE      (2 + LARGE_OFFSET_SIZE)

/*
  The size of value entries for objects or arrays. When using the
  small storage format, the entry size is 3 (1 byte for type, 2 bytes
  for offset). When using the large storage format, it is 5 (1 byte
  for type, 4 bytes for offset).
*/
#define VALUE_ENTRY_SIZE_SMALL    (1 + SMALL_OFFSET_SIZE)
#define VALUE_ENTRY_SIZE_LARGE    (1 + LARGE_OFFSET_SIZE)

/**
  Read a variable length written by append_variable_length().

  @param[in] data  the buffer to read from
  @param[in] data_length  the maximum number of bytes to read from data
  @param[out] length  the length that was read
  @param[out] num  the number of bytes needed to represent the length
  @return  false on success, true on error
*/
static inline bool read_variable_length(const char *data, size_t data_length,
                                 size_t *length, size_t *num)
{
  /*
    It takes five bytes to represent UINT_MAX32, which is the largest
    supported length, so don't look any further.
  */
  const size_t max_bytes= std::min(data_length, static_cast<size_t>(5));

  size_t len= 0;
  for (size_t i= 0; i < max_bytes; i++)
  {
    // Get the next 7 bits of the length.
    len|= (data[i] & 0x7f) << (7 * i);
    if ((data[i] & 0x80) == 0)
    {
      // The length shouldn't exceed 32 bits.
      if (len > UINT_MAX32)
        return true;                          /* purecov: inspected */

      // This was the last byte. Return successfully.
      *num= i + 1;
      *length= len;
      return false;
    }
  }

  // No more available bytes. Return true to signal error.
  return true;                                /* purecov: inspected */
}
size_t double_quote(const unsigned char *cptr, size_t length, unsigned char *buf);
static inline uint64_t parseJ_SMALL_OBJECT(const char * data,size_t dataSize,char * jsonStr);
static inline uint64_t parseJ_LARGE_OBJECT(const char * data,size_t dataSize,char * jsonStr);
static inline uint64_t parseJ_SMALL_ARRAY(const char * data,size_t dataSize,char * jsonStr);
static inline uint64_t parseJ_LARGE_ARRAY(const char * data,size_t dataSize,char * jsonStr);
static inline uint64_t parseJ_LITERAL(const char * data,size_t dataSize,char * jsonStr)
{
	if (dataSize < 1)
		return -1;
	switch (static_cast<uint8>(*data)) {
	case JSONB_NULL_LITERAL:
	{
		memcpy(jsonStr, "null", 4);
		return 4;
	}
	case JSONB_TRUE_LITERAL:
	{
		memcpy(jsonStr, "true", 4);
		return 4;
	}
	case JSONB_FALSE_LITERAL:
	{
		memcpy(jsonStr, "false", 5);
		return 5;
	}
	default:
		return -1;
	}
}
static inline uint64_t  parseJ_DOUBLE(const char * data,size_t dataSize,char * jsonStr)
{
    double d;
    float8get(&d, data);
    return my_gcvt(d, MY_GCVT_ARG_DOUBLE, MY_GCVT_MAX_FIELD_WIDTH,jsonStr,NULL);
}
static inline uint64_t   parseJ_INT16(const char * data,size_t dataSize,char * jsonStr)
{
	return sitoa(sint2korr(data), jsonStr);
}
static inline uint64_t   parseJ_UINT16(const char * data,size_t dataSize,char * jsonStr)
{
	return sutoa(uint2korr(data), jsonStr);
}
static inline uint64_t   parseJ_INT32(const char * data,size_t dataSize,char * jsonStr)
{
	return itoa(sint4korr(data), jsonStr);
}
static inline uint64_t   parseJ_UINT32(const char * data,size_t dataSize,char * jsonStr)
{
	return utoa(uint4korr(data), jsonStr);
}
static inline uint64_t   parseJ_INT64(const char * data,size_t dataSize,char * jsonStr)
{
	return ltoa(sint8korr(data), jsonStr);
}
static inline uint64_t   parseJ_UINT64(const char * data,size_t dataSize,char * jsonStr)
{
	return ultoa(uint8korr(data), jsonStr);
}
static inline uint64_t   parseJ_STRING(const char * data,size_t dataSize,char * jsonStr)
{
	size_t val_len;
	size_t n;
	if (read_variable_length(data, dataSize, &val_len, &n))
		return -1;
	return double_quote((const unsigned char *)data+n,val_len,(unsigned char *)jsonStr);
}
static bool convert_from_binary_to_decimal(const char *bin, size_t len,
                                       my_decimal *dec)
{
  bool error= (len < 2);

  if (!error)
  {
    int precision= bin[0];
    int scale= bin[1];

    size_t bin_size= my_decimal_get_binary_size(precision, scale);
    error=
      (bin_size != len - 2) ||
      (binary2my_decimal(E_DEC_ERROR,
    		  (const uchar*)(bin) + 2,
                         dec, precision, scale) != E_DEC_OK);
  }
  return error;
}
static inline uint64_t   parseJ_OPAQUE(const char * data,size_t dataSize,char * jsonStr)
{
    uint8 type_byte= static_cast<uint8>(*data);
    enum_field_types field_type= static_cast<enum_field_types>(type_byte);

    // Then there's the length of the value.
    size_t val_len;
    size_t n;
    if (read_variable_length(data + 1, dataSize - 1, &val_len, &n))
      return -1;                       /* purecov: inspected */
    if (dataSize < 1 + n + val_len)
      return -1;                         /* purecov: inspected */
    switch (field_type)
    {
    case MYSQL_TYPE_NEWDECIMAL:
    {
        int length= DECIMAL_MAX_STR_LENGTH + 1;
        my_decimal m;
        if (convert_from_binary_to_decimal(data+1+n,val_len,&m) ||
            decimal2string(&m, jsonStr, &length, 0, 0, 0))
          return -1;                           /* purecov: inspected */
        return  length;
    }

    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
    {
        MYSQL_TIME t;
        TIME_from_longlong_datetime_packed(&t, sint8korr (data+1+n));
        jsonStr[0] = '"'; /* purecov: inspected */
        uint8_t size = 1 + my_TIME_to_str(&t, jsonStr + 1, 6);
        jsonStr[size++] = '"';/* purecov: inspected */
        return size;
    }
    case MYSQL_TYPE_DATE:
    {
        MYSQL_TIME t;
        TIME_from_longlong_date_packed(&t, sint8korr (data+1+n));
        jsonStr[0] = '"'; /* purecov: inspected */
        uint8_t size = 1 + my_TIME_to_str(&t, jsonStr + 1, 6);
        jsonStr[size++] = '"';/* purecov: inspected */
        return size;
    }
    case MYSQL_TYPE_TIME:
    {
    	MYSQL_TIME t;
    	TIME_from_longlong_datetime_packed(&t, sint8korr (data+1+n));
    	jsonStr[0] = '"'; /* purecov: inspected */
    	uint8_t size = 1 + my_TIME_to_str(&t, jsonStr + 1, 6);
    	jsonStr[size++] = '"';/* purecov: inspected */
    	return size;
    }
    default:
      return -1;
    }
}

static  uint64_t (*parseJsonType[32])(const char * data,size_t dataSize,char * jsonStr) ={
		parseJ_SMALL_OBJECT,//JSONB_TYPE_SMALL_OBJECT
		parseJ_LARGE_OBJECT,//JSONB_TYPE_LARGE_OBJECT
		parseJ_SMALL_ARRAY,//JSONB_TYPE_SMALL_ARRAY
		parseJ_LARGE_ARRAY,//JSONB_TYPE_LARGE_ARRAY
		parseJ_LITERAL,//JSONB_TYPE_LITERAL
		parseJ_INT16,//JSONB_TYPE_INT16
		parseJ_UINT16,//JSONB_TYPE_UINT16
		parseJ_INT32,//JSONB_TYPE_INT32
		parseJ_UINT32,//JSONB_TYPE_UINT32
		parseJ_INT64,//JSONB_TYPE_INT64
		parseJ_UINT64,//JSONB_TYPE_UINT64
		parseJ_DOUBLE,//JSONB_TYPE_DOUBLE
		parseJ_STRING,//JSONB_TYPE_STRING
		parseJ_OPAQUE//JSONB_TYPE_OPAQUE
};

size_t double_quote(const unsigned char *cptr, size_t length, unsigned char *buf)
{
  buf[0] = '"';
  uint32_t size = 1;
  for (size_t i= 0; i < length; i++)
  {
	  if(cptr[i]>0x1f&&cptr[i]!='\\')
	  {
    	  buf[size++]=cptr[i];
    	  continue;
	  }

    bool done= true;
    char esc;
    switch (cptr[i])
    {
    case '"' :
    case '\\' :
      break;
    case '\b':
      esc= 'b';
      break;
    case '\f':
      esc= 'f';
      break;
    case '\n':
      esc= 'n';
      break;
    case '\r':
      esc= 'r';
      break;
    case '\t':
      esc= 't';
      break;
    default:
      done= false;
    }

    if (done)
    {
    	buf[size++] = '\\';
    	buf[size++] = esc;
    }
    else if (((cptr[i] & ~0x7f) == 0) && // bit 8 not set
             (cptr[i] < 0x1f))
    {
      /*
        Unprintable control character, use hex a hexadecimal number.
        The meaning of such a number determined by ISO/IEC 10646.
      */
    	memcpy(buf+size,"\\u00",sizeof("\\u00")-1);
    	size += sizeof("\\u00")-1;
    	buf[size++]=_dig_vec_lower[(cptr[i] & 0xf0) >> 4];
    	buf[size++]=_dig_vec_lower[(cptr[i] & 0x0f)];
    }
    else
    {
    	  buf[size++]=cptr[i];
    }
  }
  buf[size++]='"';
  return size;
}
template<typename SIZE_TYPE>
struct __attribute__((__packed__)) jsonObjectKey
{
	SIZE_TYPE offset;
	uint16_t size;
}__attribute__((aligned(1))) ;
template<typename SIZE_TYPE>
struct __attribute__((__packed__))jsonObjectValue
{
	uint8_t type;
	SIZE_TYPE offset;
}__attribute__((aligned(1)));
template<typename SIZE_TYPE>
struct  __attribute__((__packed__))jsonObject
{
	SIZE_TYPE element_count;
	SIZE_TYPE bytes;
	jsonObjectKey<SIZE_TYPE> *keys;
	jsonObjectValue<SIZE_TYPE> * values;
}__attribute__((aligned(1)));
template<typename SIZE_TYPE>
struct  __attribute__((__packed__))jsonArray
{
	SIZE_TYPE element_count;
	SIZE_TYPE bytes;
	jsonObjectValue<SIZE_TYPE> * values;
}__attribute__((aligned(1)));
template<typename SIZE_TYPE>
int64_t object2Str(const char * data,uint64_t dataSize,char * jsonStr)
{
	jsonObject<SIZE_TYPE> object;
	memcpy(&object,data,offsetof(jsonObject<SIZE_TYPE>,keys));
	object.keys = (jsonObjectKey<SIZE_TYPE>*)(data+sizeof(SIZE_TYPE)*2);
	object.values = (jsonObjectValue<SIZE_TYPE>*)(&object.keys[object.element_count]);
	if((const char*)(&object.values[object.element_count])>data+dataSize)
		return -1;
	jsonStr[0] = '{';
	uint64_t jsonStrSize = 1;
	uint8_t maxInlineType;//JSONB_TYPE_LITERAL is min InlineType
	if(sizeof(SIZE_TYPE)==4)
		maxInlineType = JSONB_TYPE_UINT32;
	else
		maxInlineType = JSONB_TYPE_UINT16;
	int64_t subLength = 0;
	for(uint32_t i=0;i<object.element_count;i++)
	{
		jsonStrSize+=double_quote((const uint8_t*)(data+object.keys[i].offset),object.keys[i].size,(unsigned char*)jsonStr+jsonStrSize);
		*(uint16_t*)(jsonStr+jsonStrSize) = 0x203a;//0x203a = ': '
		jsonStrSize+=2;
		if(object.values[i].type>=JSONB_TYPE_LITERAL&&object.values[i].type<=maxInlineType)
		{
			if(0>(subLength = parseJsonType[object.values[i].type]((const char*)&object.values[i].offset,sizeof(SIZE_TYPE),jsonStr+jsonStrSize)))
					return -1;
			jsonStrSize+=subLength;
		}
		else
		{
			if(0>(subLength = parseJsonType[object.values[i].type](data+object.values[i].offset,dataSize-object.values[i].offset,jsonStr+jsonStrSize)))
					return -1;
			jsonStrSize+=subLength;
		}
		*(uint16_t*)(jsonStr+jsonStrSize) = 0x202cu;//0x2c20u = ", "
		jsonStrSize+=2;
	}
	jsonStr[jsonStrSize-2]='}';
	return jsonStrSize-1;
}
template<typename SIZE_TYPE>
int64_t array2Str(const char * data,uint64_t dataSize,char * jsonStr)
{
	jsonArray<SIZE_TYPE> array;
	memcpy(&array,data,offsetof(jsonObject<SIZE_TYPE>,keys));
	array.values = (jsonObjectValue<SIZE_TYPE>*)(data+offsetof(jsonObject<SIZE_TYPE>,keys));
	if((const char*)(&array.values[array.element_count])>data+dataSize)
		return -1;
	jsonStr[0] = '[';
	uint64_t jsonStrSize = 1;
	uint8_t maxInlineType;//JSONB_TYPE_LITERAL is min InlineType
	if(sizeof(SIZE_TYPE)==4)
		maxInlineType = JSONB_TYPE_UINT32;
	else
		maxInlineType = JSONB_TYPE_UINT16;
	int64_t subLength = 0;
	for(uint32_t i=0;i<array.element_count;i++)
	{
		if(array.values[i].type>=JSONB_TYPE_LITERAL&&array.values[i].type<=maxInlineType)
		{
			if(0>(subLength = parseJsonType[array.values[i].type]((const char*)&array.values[i].offset,sizeof(SIZE_TYPE),jsonStr+jsonStrSize)))
				return -1;
			jsonStrSize+=subLength;
		}
		else
		{
			if(0>(subLength = parseJsonType[array.values[i].type](data+array.values[i].offset,dataSize-array.values[i].offset,jsonStr+jsonStrSize)))
				return -1;
			jsonStrSize+=subLength;
		}
		*(uint16_t*)(jsonStr+jsonStrSize) = 0x202cu;//0x2c20u = ", "
		jsonStrSize+=2;
	}
	jsonStr[jsonStrSize-2]=']';
	return jsonStrSize-1;
}
uint64_t parseJ_SMALL_OBJECT(const char * data,size_t dataSize,char * jsonStr)
{
	return object2Str<uint16_t>(data,dataSize,jsonStr);
}
uint64_t parseJ_LARGE_OBJECT(const char * data,size_t dataSize,char * jsonStr)
{
	return object2Str<uint32_t>(data,dataSize,jsonStr);
}
uint64_t parseJ_SMALL_ARRAY(const char * data,size_t dataSize,char * jsonStr)
{
	return array2Str<uint16_t>(data,dataSize,jsonStr);
}
uint64_t parseJ_LARGE_ARRAY(const char * data,size_t dataSize,char * jsonStr)
{
	return array2Str<uint32_t>(data,dataSize,jsonStr);
}
uint64_t bjson2Str(const char * bjson,uint64_t bjsonSize,char * jsonStr)
{
	return parseJsonType[bjson[0]](bjson+1,bjsonSize-1,jsonStr);
}
