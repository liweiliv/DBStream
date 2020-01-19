/*
 * columnParser.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include <stddef.h>
#include <assert.h>
#include <stdint.h>
#include <float.h>
#include <mysql.h>
#include "columnParser.h"
#include "util/itoaSse.h"
#include "util/dtoa.h"
#include "util/likely.h"
namespace DATA_SOURCE {
	constexpr const int DATETIME_MAX_DECIMALS = 6;
#define test_bitmap(m,idx) ((m)[(idx)>>3]&(1<<((idx)&0x07)))
	char _dig_vec_lower[] = "0123456789abcdefghijklmnopqrstuvwxyz";
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
	static inline uint32_t uint3korr(const uint8_t* A) {
		return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
			(((uint32_t)(A[2])) << 16));
	}
	static inline int32_t sint3korr(const uint8_t* A) {
		return ((int32_t)(((A[2]) & 128)
			? (((int32_t)255L << 24) | (((int32_t)A[2]) << 16) |
			(((int32_t)A[1]) << 8) | ((int32_t)A[0]))
			: (((int32_t)A[2]) << 16) | (((int32_t)A[1]) << 8) |
			((int32_t)A[0])));
	}
#define MAX_DECPT_FOR_F_FORMAT DBL_DIG

#define MY_GCVT_MAX_FIELD_WIDTH \
		(DBL_DIG + 4 + (5>MAX_DECPT_FOR_F_FORMAT?5:MAX_DECPT_FOR_F_FORMAT))


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
	static inline uint32_t mi_uint4korr(const uint8_t* A) {
		return (uint32_t)((uint32_t)A[3] + ((uint32_t)A[2] << 8) + ((uint32_t)A[1] << 16) +
			((uint32_t)A[0] << 24));
	}
	static inline uint16_t mi_uint2korr(const uint8_t* A) {
		return (uint16_t)((uint16_t)A[1]) + ((uint16_t)A[0] << 8);
	}

	static inline int8_t mi_sint1korr(const uint8_t* A) { return *A; }
	static inline int16_t mi_sint2korr(const uint8_t* A) {
		return (int16_t)((uint32_t)(A[1]) + ((uint32_t)(A[0]) << 8));
	}
	static inline int32_t mi_sint3korr(const uint8_t* A) {
		return (int32_t)((A[0] & 128) ? ((255U << 24) | ((uint32_t)(A[0]) << 16) |
			((uint32_t)(A[1]) << 8) | ((uint32_t)A[2]))
			: (((uint32_t)(A[0]) << 16) |
			((uint32_t)(A[1]) << 8) | ((uint32_t)(A[2]))));
	}
	static inline uint32_t mi_uint3korr(const uint8_t* A) {
		return (uint32_t)((uint32_t)A[2] + ((uint32_t)A[1] << 8) + ((uint32_t)A[0] << 16));
	}

	static inline int32_t mi_sint4korr(const uint8_t* A) {
		return (int32_t)((uint32_t)(A[3]) + ((uint32_t)(A[2]) << 8) +
			((uint32_t)(A[1]) << 16) + ((uint32_t)(A[0]) << 24));
	}

	static inline uint64_t mi_uint5korr(const uint8_t* A) {
		return (uint64_t)((uint32_t)A[4] + ((uint32_t)A[3] << 8) + ((uint32_t)A[2] << 16) +
			((uint32_t)A[1] << 24)) +
			((uint64_t)A[0] << 32);
	}
	static inline uint64_t mi_uint6korr(const uint8_t* A) {
		return (uint64_t)((uint32_t)A[5] + ((uint32_t)A[4] << 8) + ((uint32_t)A[3] << 16) +
			((uint32_t)A[2] << 24)) +
			(((uint64_t)((uint32_t)A[1] + ((uint32_t)A[0] << 8))) << 32);
	}

#define TIMEF_OFS 0x800000000000LL
#define TIMEF_INT_OFS 0x800000LL

	/**
  Convert on-disk time representation to in-memory packed numeric
  representation.

  @param   ptr  The pointer to read the value at.
  @param   dec  Precision.
  @return       Packed numeric time representation.
*/
	void my_time_packed_from_binary(const uint8_t* ptr, uint32_t dec, int64_t& intpart, int& frac) {
		assert(dec <= DATETIME_MAX_DECIMALS);
		intpart = 0;
		frac = 0;
		switch (dec) {
		case 0:
		default: {
			intpart = mi_uint3korr(ptr) - TIMEF_INT_OFS;
			return;
		}
		case 1:
		case 2: {
			intpart = mi_uint3korr(ptr) - TIMEF_INT_OFS;
			frac = static_cast<uint32_t>(ptr[3]);
			if (intpart < 0 && frac) {
				/*
				   Negative values are stored with
				   reverse fractional part order,
				   for binary sort compatibility.

					Disk value  intpart frac   Time value   Memory value
					800000.00    0      0      00:00:00.00  0000000000.000000
					7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
					7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
					7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
					7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
					7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

					Formula to convert fractional part from disk format
					(now stored in "frac" variable) to absolute value: "0x100 - frac".
					To reconstruct in-memory value, we shift
					to the next integer value and then substruct fractional part.
				*/
				intpart++;     /* Shift to the next integer value */
				frac -= 0x100; /* -(0x100 - frac) */
			}
			frac *= 10000;
			return;
		}

		case 3:
		case 4: {
			int64_t intpart = mi_uint3korr(ptr) - TIMEF_INT_OFS;
			int frac = mi_uint2korr(ptr + 3);
			if (intpart < 0 && frac) {
				/*
				  Fix reverse fractional part order: "0x10000 - frac".
				  See comments for FSP=1 and FSP=2 above.
				*/
				intpart++;       /* Shift to the next integer value */
				frac -= 0x10000; /* -(0x10000-frac) */
			}
			frac *= 100;
			return;
		}

		case 5:
		case 6:
		{
			int64_t tm = (static_cast<int64_t>(mi_uint6korr(ptr))) - TIMEF_OFS;
			intpart = tm >> 24;
			frac = tm & 0xffffff;
			return;
		}
		}
	}
#define NOT_FIXED_DEC 31

#define DIG_PER_DEC1 9

	static const int dig2bytes[DIG_PER_DEC1 + 1] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

	static const uint32_t digMasks[DIG_PER_DEC1 + 1] = { 0,0xff, 0xff, 0xffff, 0xffff, 0xffffff, 0xffffff, 0xffffffff, 0xffffffff, 0xffffffff };

#define DATETIMEF_INT_OFS 0x8000000000LL




	int parse_MYSQL_TYPE_TINY(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_signed)
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(int8_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(int8_t*)(data));
		}
		else
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(uint8_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(uint8_t*)(data));
		}
		data += 1;
		return 0;
	}
	int parse_MYSQL_TYPE_SHORT(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_signed)
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(int16_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(int16_t*)(data));
		}
		else
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(uint16_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(uint16_t*)(data));
		}
		data += 2;
		return 0;
	}
	int parse_MYSQL_TYPE_INT24(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_signed)
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, sint3korr((const uint8_t*)data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, sint3korr((const uint8_t*)data));
		}
		else
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, uint3korr((const uint8_t*)data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, uint3korr((const uint8_t*)data));
		}
		data += 3;
		return 0;
	}
	int parse_MYSQL_TYPE_LONG(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_signed)
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(int32_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(int32_t*)(data));
		}
		else
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(uint32_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(uint32_t*)(data));
		}
		data += 4;
		return 0;
	}
	int parse_MYSQL_TYPE_LONGLONG(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_signed)
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(int64_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(int64_t*)(data));
		}
		else
		{
			if (newOrOld)
				record->setFixedColumn(colMeta->m_columnIndex, *(uint64_t*)(data));
			else
				record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(uint64_t*)(data));
		}
		data += 8;
		return 0;
	}
	int parse_MYSQL_TYPE_FLOAT(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, *(const float*)data);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(const float*)data);
		data += 4;
		return 0;
	}
	int parse_MYSQL_TYPE_DOUBLE(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, *(const double*)data);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, *(const double*)data);
		data += 8;
		return 0;
	}
	static char zeroBuffer[] = { '0','0','0','0','0','0','0','0','0','0' };
	inline int32_t getPartNumberFromDecimal(uint8_t size, const char* data)
	{
		switch (size)
		{
		case 1:
			return mi_sint1korr((const uint8_t*)data);
		case 2:
			return mi_sint2korr((const uint8_t*)data);
		case 3:
			return mi_sint3korr((const uint8_t*)data);
		case 4:
			return mi_sint4korr((const uint8_t*)data);
		default:
			abort();
		}
	}

	static inline int  parseDecimalToString(const char*& data, char* to, uint8_t prec, uint8_t scale)
	{

		int32_t mask = ((*(uint8_t*)data) & 0x80) ? 0 : -1;
		uint8_t intSize = prec - scale;
		char* pos = to;
		uint8_t intFullPartSize = intSize / 9, intFirstPartNumSize = intSize - intFullPartSize * 9, intFirstPartSize = dig2bytes[intFirstPartNumSize];
		uint8_t fracFullPartSize = scale / 9, fracFirstPartNumSize = scale - fracFullPartSize * 9, fracFirstPartSize = dig2bytes[fracFirstPartNumSize];
		bool sign = !(data[0] & 0x80);

		if (!sign)
			((uint8_t*)data)[0] &= (~0x80);
		else
			((uint8_t*)data)[0] |= 0x80;

		if (sign)
			*pos++ = '-';

		int num = 0;
		bool isZero = true;

		if (intFirstPartSize > 0)
		{
			num = (getPartNumberFromDecimal(intFirstPartSize, data) ^ mask) & digMasks[intFirstPartNumSize];
			if (num != 0)
			{
				pos += i32toa_sse2(num, pos) - 1;
				isZero = false;
			}
			data += intFirstPartSize;
		}
		for (uint8_t i = 0; i < intFullPartSize; i++)
		{
			if (0 != (num = (mi_sint4korr((const uint8_t*)data)) ^ mask))
			{
				if (isZero)
				{
					pos += i32toa_sse2(num, pos) - 1;
					isZero = false;
				}
				else
				{
					uint8_t len = i32toa_sse2b(num, pos + 9) - 1;
					if (len != 9 && (i > 0 || intFirstPartSize > 0))
						memcpy(pos, zeroBuffer, 9 - len);
					pos += 9;
				}
			}
			else if (!isZero)
			{
				memcpy(pos, zeroBuffer, 9);
				pos += 9;
			}
			data += 4;
		}
		if (isZero)
			*pos++ = '0';
		if (scale > 0)
		{
			*pos++ = '.';
			for (uint8_t i = 0; i < fracFullPartSize; i++)
			{
				if (0 != (num = (mi_sint4korr((const uint8_t*)data) ^ mask)))
				{
					uint8_t len = i32toa_sse2b(num, pos + 9) - 1;
					memcpy(pos, zeroBuffer, 9 - len);
				}
				else
				{
					memcpy(pos, zeroBuffer, 9);
				}
				pos += 9;
				data += 4;
			}
			if (fracFirstPartSize > 0)
			{
				if (0 != (num = (getPartNumberFromDecimal(fracFirstPartSize, data) ^ mask) & digMasks[intFirstPartNumSize]))
				{
					uint8_t len = i32toa_sse2b(num, pos + fracFirstPartNumSize) - 1;
					memcpy(pos, zeroBuffer, fracFirstPartNumSize - len);
				}
				else
				{
					memcpy(pos, zeroBuffer, fracFirstPartNumSize);
				}
				pos += fracFirstPartNumSize;
				data += fracFirstPartSize;
			}
		}
		*pos++ = '\0';
		return pos - to;
	}
	int parse_MYSQL_TYPE_NEWDECIMAL(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{

		char* buffer;
		if (newOrOld)
			buffer = record->allocVarColumn();
		else
			buffer = record->allocVardUpdatedColumn();
		uint16_t length = parseDecimalToString(data, buffer, colMeta->m_precision, colMeta->m_decimals);
		if (newOrOld)
			record->filledVarColumns(colMeta->m_columnIndex, length);
		else
			record->filledVardUpdatedColumn(colMeta->m_columnIndex, length);
		return 0;
	}
	int parse_MYSQL_TYPE_TIMESTAMP(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, META::timestamp::create(*(uint32_t*)data, 0));
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, META::timestamp::create(*(uint32_t*)data, 0));
		data += 4;
		return 0;
	}

	uint32_t lengthOf_MYSQL_TYPE_TIMESTAMP2(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		return my_datetime_binary_length(colMeta->m_decimals);
	}
	int parse_MYSQL_TYPE_TIMESTAMP2(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t seconds, nanoSeconds = 0;
		assert(colMeta->m_precision <= DATETIME_MAX_DECIMALS);
		seconds = mi_uint4korr((const uint8_t*)data);
		switch (colMeta->m_precision) {
		case 0:
		default:
			break;
		case 1:
		case 2:
			nanoSeconds = (static_cast<int>(data[4])) * 10000000;
			break;
		case 3:
		case 4:
			nanoSeconds = mi_sint2korr((uint8_t*)data + 4) * 100000;
			break;
		case 5:
		case 6:
			nanoSeconds = mi_sint3korr((uint8_t*)data + 4) * 1000;
		}
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, META::timestamp::create(seconds, nanoSeconds));
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, META::timestamp::create(seconds, nanoSeconds));

		data += my_timestamp_binary_length(colMeta->m_precision);
		return 0;
	}
	int parse_MYSQL_TYPE_DATETIME2(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint64_t intpart = mi_uint5korr((const uint8_t*)data) - DATETIMEF_INT_OFS;
		int frac = 0;
		switch (colMeta->m_precision) {
		case 0:
		default:
			break;
		case 1:
		case 2:
			frac = (static_cast<int>(data[5])) * 10000;
			break;
		case 3:
		case 4:
			frac = mi_sint2korr((const uint8_t*)data + 5) * 100;
			break;
		case 5:
		case 6:
			frac = mi_sint3korr((const uint8_t*)data + 5);
			break;
		}
		int64_t ymd = intpart >> 17;
		int64_t hms = intpart % (1 << 17);
		int64_t ym = ymd >> 5;
		int64_t tm = META::dateTime::createDate(static_cast<uint32_t>(ym / 13), ym % 13, ymd % (1 << 5), static_cast<uint32_t>(hms >> 12),
			(hms >> 6) % (1 << 6), hms % (1 << 6), frac);
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, tm);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, tm);
		data += my_datetime_binary_length(colMeta->m_precision);
		return 0;
	}
	int parse_MYSQL_TYPE_DATETIME(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		int64_t rawTime = *(int64_t*)(data);
		uint32_t high = rawTime / 100000000;
		uint32_t low = rawTime % 100000000;
		uint16_t lowH = low / 10000;
		uint16_t lowL = low % 10000;
		int64_t tm = META::dateTime::createDate(high / 100, high % 100, lowH / 100, lowH % 100, lowL / 100, lowL % 100, 0);
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, tm);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, tm);
		data += 8;
		return 0;
	}
	int parse_MYSQL_TYPE_TIME2(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		int64_t intpart;
		int frac;
		my_time_packed_from_binary((const uint8_t*)data, colMeta->m_decimals, intpart, frac);
		bool neg = intpart < 0 ? (intpart = -intpart, true) : false;
		int32_t hour = (intpart >> 12) % (1 << 10);
		if (neg)
			hour = -hour;
		int64_t tm = META::Time::createTime(hour, (intpart >> 6) % (1 << 6), (intpart) % (1 << 6), frac);
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, tm);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, tm);
		data += my_time_binary_length(colMeta->m_decimals);
		return 0;
	}
	int parse_MYSQL_TYPE_NEWDATE(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t intpart = uint3korr((const uint8_t*)data);
		uint32_t date = META::Date::createDate(intpart >> 9, intpart >> 5 & 0x0f, intpart & 0x1f);
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, date);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, date);
		data += 3;
		return 0;
	}
	int parse_MYSQL_TYPE_YEAR(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, (uint16_t)(1900 + *(unsigned short*)data));
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, (uint16_t)(1900 + *(unsigned short*)data));
		data += 1;
		return 0;
	}
	int parse_MYSQL_TYPE_STRING(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (colMeta->m_size >= 0xffu)
		{
			if (newOrOld)
				record->setVarColumn(colMeta->m_columnIndex, data + 2, *(uint16_t*)data);
			else
				record->setVardUpdatedColumn(colMeta->m_columnIndex, data + 2, *(uint16_t*)data);
			data += (*(uint16_t*)data) + 2;
		}
		else
		{
			if (newOrOld)
				record->setVarColumn(colMeta->m_columnIndex, data + 1, *(uint8_t*)data);
			else
				record->setVardUpdatedColumn(colMeta->m_columnIndex, data + 1, *(uint8_t*)data);
			data += (*(uint8_t*)data) + 1;
		}
		return 0;
	}
	uint32_t lengthOf_MYSQL_TYPE_BIT(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		return (colMeta->m_size + 7) >> 3;
	}

	int parse_MYSQL_TYPE_BIT(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint64_t bitValue;
		uint8_t bitSize = (colMeta->m_size + 7) >> 3;
		memcpy(&bitValue, data, bitSize);
		bitValue >>= (64 - colMeta->m_size);
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, bitValue);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, bitValue);
		data += bitSize;
		return 0;
	}
	int parse_MYSQL_TYPE_VAR_STRING(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t size = colMeta->m_size;
		if (size < 256)
		{
			size = (*(uint8_t*)(data) & 0xFF);
			data++;
		}
		else
		{
			size = *(uint16_t*)data;
			data += 2;
		}
		if (newOrOld)
			record->setVarColumn(colMeta->m_columnIndex, data, size);
		else
			record->setVardUpdatedColumn(colMeta->m_columnIndex, data, size);
		data += size;
		return 0;
	}
	int parse_MYSQL_TYPE_BLOB(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t size;
		switch (colMeta->m_srcColumnType)
		{
		case MYSQL_TYPE_TINY_BLOB:
			size = *((const uint8_t*)data);
			data += 1;
			break;
		case MYSQL_TYPE_BLOB:
			size = *(const uint16_t*)data;
			data += 2;
			break;
		case MYSQL_TYPE_MEDIUM_BLOB:
			size = uint3korr((const uint8_t*)data);
			data += 3;
			break;
		case MYSQL_TYPE_LONG_BLOB:
			size = *(const uint32_t*)(data);
			data += 4;
			break;
		default:
			return -1;
		}
		if (newOrOld)
			record->setVarColumn(colMeta->m_columnIndex, data, size);
		else
			record->setVardUpdatedColumn(colMeta->m_columnIndex, data, size);
		data += size;
		return 0;
	}
	uint64_t  bjson2Str(const char* bjson, uint64_t bjsonSize, char* jsonStr);

	int parse_MYSQL_TYPE_JSON(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t size = *(const uint32_t*)(data);
		if (newOrOld)
			record->filledVarColumns(colMeta->m_columnIndex, bjson2Str(data + sizeof(uint32_t), size, record->allocVarColumn()));
		else
			record->filledVardUpdatedColumn(colMeta->m_columnIndex, bjson2Str(data + sizeof(uint32_t), size, record->allocVardUpdatedColumn()));
		data += size + sizeof(uint32_t);
		return 0;
	}
	int parse_MYSQL_TYPE_SET(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint8_t bitmapSize = 0;
		if (colMeta->m_setAndEnumValueList.m_count > 0)
		{
			if (colMeta->m_setAndEnumValueList.m_count > 32)
				bitmapSize = 8;
			else
				bitmapSize = ((colMeta->m_setAndEnumValueList.m_count + 7) >> 3);
			uint64_t value = 0;
			memcpy(&value, data, bitmapSize);
			record->setFixedColumn(colMeta->m_columnIndex, value);
			data += bitmapSize;
			return 0;
		}
		else
			return -1;
	}
	int parse_MYSQL_TYPE_ENUM(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		uint32_t enumValueIndex = 0;
		if (colMeta->m_size > 255)
		{
			enumValueIndex = *(uint16_t*)(data);
			data += 2;
		}
		else
		{
			enumValueIndex = *(data);
			data++;
		}
		if (newOrOld)
			record->setFixedColumn(colMeta->m_columnIndex, enumValueIndex);
		else
			record->setFixedUpdatedColumn(colMeta->m_columnIndex, enumValueIndex);
		return 0;
	}
	int parse_MYSQL_TYPE_GEOMETRY(const META::columnMeta* colMeta, DATABASE_INCREASE::DMLRecord* record,
		const char*& data, bool newOrOld)
	{
		if (newOrOld)
			record->setVarColumn(colMeta->m_columnIndex, data + sizeof(uint32_t), *(const uint32_t*)(data));
		else
			record->setVardUpdatedColumn(colMeta->m_columnIndex, data + sizeof(uint32_t), *(const uint32_t*)(data));
		data += *(const uint32_t*)(data)+sizeof(uint32_t);
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
	static inline bool read_variable_length(const char* data, size_t data_length,
		size_t* length, size_t* num)
	{
		/*
		  It takes five bytes to represent UINT_MAX32, which is the largest
		  supported length, so don't look any further.
		*/
		const size_t max_bytes = data_length > 5 ? 5 : data_length;

		size_t len = 0;
		for (size_t i = 0; i < max_bytes; i++)
		{
			// Get the next 7 bits of the length.
			len |= (data[i] & 0x7f) << (7 * i);
			if ((data[i] & 0x80) == 0)
			{
				// The length shouldn't exceed 32 bits.
				if (len > 0xfffffffful)
					return true;                          /* purecov: inspected */

				  // This was the last byte. Return successfully.
				*num = i + 1;
				*length = len;
				return false;
			}
		}

		// No more available bytes. Return true to signal error.
		return true;                                /* purecov: inspected */
	}
	size_t double_quote(const unsigned char* cptr, size_t length, unsigned char* buf);
	static inline uint64_t parseJ_SMALL_OBJECT(const char* data, size_t dataSize, char* jsonStr);
	static inline uint64_t parseJ_LARGE_OBJECT(const char* data, size_t dataSize, char* jsonStr);
	static inline uint64_t parseJ_SMALL_ARRAY(const char* data, size_t dataSize, char* jsonStr);
	static inline uint64_t parseJ_LARGE_ARRAY(const char* data, size_t dataSize, char* jsonStr);
	static inline uint64_t parseJ_LITERAL(const char* data, size_t dataSize, char* jsonStr)
	{
		if (dataSize < 1)
			return -1;
		switch (static_cast<uint8_t>(*data)) {
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
	static inline uint64_t  parseJ_DOUBLE(const char* data, size_t dataSize, char* jsonStr)
	{
		double d = *(double*)data;
		return my_gcvt(d, MY_GCVT_ARG_DOUBLE, MY_GCVT_MAX_FIELD_WIDTH, jsonStr, NULL);
	}
	static inline uint64_t   parseJ_INT16(const char* data, size_t dataSize, char* jsonStr)
	{
		return u32toa_sse2(*(uint16_t*)data, jsonStr);
	}
	static inline uint64_t   parseJ_UINT16(const char* data, size_t dataSize, char* jsonStr)
	{
		return i32toa_sse2(*(int16_t*)data, jsonStr);
	}
	static inline uint64_t   parseJ_INT32(const char* data, size_t dataSize, char* jsonStr)
	{
		return i32toa_sse2(*(int32_t*)(data), jsonStr);
	}
	static inline uint64_t   parseJ_UINT32(const char* data, size_t dataSize, char* jsonStr)
	{
		return u32toa_sse2(*(uint32_t*)data, jsonStr);
	}
	static inline uint64_t   parseJ_INT64(const char* data, size_t dataSize, char* jsonStr)
	{
		return i64toa_sse2(*(int64_t*)data, jsonStr);
	}
	static inline uint64_t   parseJ_UINT64(const char* data, size_t dataSize, char* jsonStr)
	{
		return u64toa_sse2(*(uint64_t*)data, jsonStr);
	}
	static inline uint64_t   parseJ_STRING(const char* data, size_t dataSize, char* jsonStr)
	{
		size_t val_len;
		size_t n;
		if (read_variable_length(data, dataSize, &val_len, &n))
			return -1;
		return double_quote((const unsigned char*)data + n, val_len, (unsigned char*)jsonStr);
	}

	static inline uint64_t   parseJ_OPAQUE(const char* data, size_t dataSize, char* jsonStr)
	{
		uint8_t type_byte = static_cast<uint8_t>(*data);
		enum_field_types field_type = static_cast<enum_field_types>(type_byte);

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
			uint8_t prec = data[1 + n];
			int decim = data[1 + n + 1];
			data += 1 + n + 2;
			return parseDecimalToString(data, jsonStr, prec, decim);
		}

		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIMESTAMP:
		{
			uint64_t intpart = (*(uint64_t*)(data + 1 + n)) >> 24;
			int frac = (*(uint64_t*)(data + 1 + n)) & 0xffffff;
			int64_t ymd = intpart >> 17;
			int64_t hms = intpart % (1 << 17);
			int64_t ym = ymd >> 5;
			META::dateTime tm;
			tm.set(static_cast<uint32_t>(ym / 13), ym % 13, ymd % (1 << 5), static_cast<uint32_t>(hms >> 12),
				(hms >> 6) % (1 << 6), hms % (1 << 6), frac);
			jsonStr[0] = '"'; /* purecov: inspected */
			uint8_t size = tm.toString(jsonStr);
			jsonStr[size] = '"';/* purecov: inspected */
			return size + 1;
		}
		case MYSQL_TYPE_DATE:
		{
			uint64_t intpart = (*(uint64_t*)(data + 1 + n)) >> 24;
			int64_t ymd = intpart >> 17;
			int64_t ym = ymd >> 5;
			META::dateTime tm;
			tm.set(static_cast<uint32_t>(ym / 13), ym % 13, ymd % (1 << 5), 0,
				0, 0, 0);
			jsonStr[0] = '"'; /* purecov: inspected */
			uint8_t size = tm.toDateString(jsonStr);
			jsonStr[size] = '"';/* purecov: inspected */
			return size + 1;
		}
		case MYSQL_TYPE_TIME:
		{
			int64_t intpart = (*(int64_t*)(data + 1 + n)) >> 24;
			int frac = (*(uint64_t*)(data + 1 + n)) & 0xffffff;
			int64_t hms = intpart % (1 << 17);
			META::dateTime tm;
			tm.set(intpart > 0 ? 0 : -1, 0, 0, static_cast<uint32_t>(hms >> 12),
				(hms >> 6) % (1 << 6), hms % (1 << 6), frac);
			jsonStr[0] = '"'; /* purecov: inspected */
			uint8_t size = tm.toTimeString(jsonStr);
			jsonStr[size] = '"';/* purecov: inspected */
			return size + 1;
		}
		default:
			return -1;
		}
	}

	static  uint64_t(*parseJsonType[32])(const char* data, size_t dataSize, char* jsonStr) = {
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

	size_t double_quote(const unsigned char* cptr, size_t length, unsigned char* buf)
	{
		buf[0] = '"';
		uint32_t size = 1;
		for (size_t i = 0; i < length; i++)
		{
			if (cptr[i] > 0x1f && cptr[i] != '\\')
			{
				buf[size++] = cptr[i];
				continue;
			}

			bool done = true;
			char esc = 0;
			switch (cptr[i])
			{
			case '"':
			case '\\':
				break;
			case '\b':
				esc = 'b';
				break;
			case '\f':
				esc = 'f';
				break;
			case '\n':
				esc = 'n';
				break;
			case '\r':
				esc = 'r';
				break;
			case '\t':
				esc = 't';
				break;
			default:
				done = false;
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
				memcpy(buf + size, "\\u00", sizeof("\\u00") - 1);
				size += sizeof("\\u00") - 1;
				buf[size++] = _dig_vec_lower[(cptr[i] & 0xf0) >> 4];
				buf[size++] = _dig_vec_lower[(cptr[i] & 0x0f)];
			}
			else
			{
				buf[size++] = cptr[i];
			}
		}
		buf[size++] = '"';
		return size;
	}
#pragma pack(1)
	template<typename SIZE_TYPE>
	struct jsonObjectKey
	{
		SIZE_TYPE offset;
		uint16_t size;
	};
	template<typename SIZE_TYPE>
	struct jsonObjectValue
	{
		uint8_t type;
		SIZE_TYPE offset;
	};
	template<typename SIZE_TYPE>
	struct jsonObject
	{
		SIZE_TYPE element_count;
		SIZE_TYPE bytes;
		jsonObjectKey<SIZE_TYPE>* keys;
		jsonObjectValue<SIZE_TYPE>* values;
	};
	template<typename SIZE_TYPE>
	struct jsonArray
	{
		SIZE_TYPE element_count;
		SIZE_TYPE bytes;
		jsonObjectValue<SIZE_TYPE>* values;
	};
	template<typename SIZE_TYPE>
	int64_t object2Str(const char* data, uint64_t dataSize, char* jsonStr)
	{
		jsonObject<SIZE_TYPE> object;
		memcpy(&object, data, offsetof(jsonObject<SIZE_TYPE>, keys));
		object.keys = (jsonObjectKey<SIZE_TYPE>*)(data + sizeof(SIZE_TYPE) * 2);
		object.values = (jsonObjectValue<SIZE_TYPE>*)(&object.keys[object.element_count]);
		if ((const char*)(&object.values[object.element_count]) > data + dataSize)
			return -1;
		jsonStr[0] = '{';
		uint64_t jsonStrSize = 1;
		uint8_t maxInlineType;//JSONB_TYPE_LITERAL is min InlineType
		if (sizeof(SIZE_TYPE) == 4)
			maxInlineType = JSONB_TYPE_UINT32;
		else
			maxInlineType = JSONB_TYPE_UINT16;
		int64_t subLength = 0;
		for (uint32_t i = 0; i < object.element_count; i++)
		{
			jsonStrSize += double_quote((const uint8_t*)(data + object.keys[i].offset), object.keys[i].size, (unsigned char*)jsonStr + jsonStrSize);
			*(uint16_t*)(jsonStr + jsonStrSize) = 0x203a;//0x203a = ': '
			jsonStrSize += 2;
			if (object.values[i].type >= JSONB_TYPE_LITERAL && object.values[i].type <= maxInlineType)
			{
				if (0 > (subLength = parseJsonType[object.values[i].type]((const char*)&object.values[i].offset, sizeof(SIZE_TYPE), jsonStr + jsonStrSize)))
					return -1;
				jsonStrSize += subLength;
			}
			else
			{
				if (0 > (subLength = parseJsonType[object.values[i].type](data + object.values[i].offset, dataSize - object.values[i].offset, jsonStr + jsonStrSize)))
					return -1;
				jsonStrSize += subLength;
			}
			*(uint16_t*)(jsonStr + jsonStrSize) = 0x202cu;//0x2c20u = ", "
			jsonStrSize += 2;
		}
		jsonStr[jsonStrSize - 2] = '}';
		return jsonStrSize - 1;
	}
	template<typename SIZE_TYPE>
	int64_t array2Str(const char* data, uint64_t dataSize, char* jsonStr)
	{
		jsonArray<SIZE_TYPE> array;
		memcpy(&array, data, offsetof(jsonObject<SIZE_TYPE>, keys));
		array.values = (jsonObjectValue<SIZE_TYPE>*)(data + offsetof(jsonObject<SIZE_TYPE>, keys));
		if ((const char*)(&array.values[array.element_count]) > data + dataSize)
			return -1;
		jsonStr[0] = '[';
		uint64_t jsonStrSize = 1;
		uint8_t maxInlineType;//JSONB_TYPE_LITERAL is min InlineType
		if (sizeof(SIZE_TYPE) == 4)
			maxInlineType = JSONB_TYPE_UINT32;
		else
			maxInlineType = JSONB_TYPE_UINT16;
		int64_t subLength = 0;
		for (uint32_t i = 0; i < array.element_count; i++)
		{
			if (array.values[i].type >= JSONB_TYPE_LITERAL && array.values[i].type <= maxInlineType)
			{
				if (0 > (subLength = parseJsonType[array.values[i].type]((const char*)&array.values[i].offset, sizeof(SIZE_TYPE), jsonStr + jsonStrSize)))
					return -1;
				jsonStrSize += subLength;
			}
			else
			{
				if (0 > (subLength = parseJsonType[array.values[i].type](data + array.values[i].offset, dataSize - array.values[i].offset, jsonStr + jsonStrSize)))
					return -1;
				jsonStrSize += subLength;
			}
			*(uint16_t*)(jsonStr + jsonStrSize) = 0x202cu;//0x2c20u = ", "
			jsonStrSize += 2;
		}
		jsonStr[jsonStrSize - 2] = ']';
		return jsonStrSize - 1;
	}
	uint64_t parseJ_SMALL_OBJECT(const char* data, size_t dataSize, char* jsonStr)
	{
		return object2Str<uint16_t>(data, dataSize, jsonStr);
	}
	uint64_t parseJ_LARGE_OBJECT(const char* data, size_t dataSize, char* jsonStr)
	{
		return object2Str<uint32_t>(data, dataSize, jsonStr);
	}
	uint64_t parseJ_SMALL_ARRAY(const char* data, size_t dataSize, char* jsonStr)
	{
		return array2Str<uint16_t>(data, dataSize, jsonStr);
	}
	uint64_t parseJ_LARGE_ARRAY(const char* data, size_t dataSize, char* jsonStr)
	{
		return array2Str<uint32_t>(data, dataSize, jsonStr);
	}
	uint64_t bjson2Str(const char* bjson, uint64_t bjsonSize, char* jsonStr)
	{
		return parseJsonType[(uint8_t)bjson[0]](bjson + 1, bjsonSize - 1, jsonStr);
	}
#pragma pack()

}
