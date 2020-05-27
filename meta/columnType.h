#pragma once
#include "util/itoaSse.h"
#include "util/winDll.h"
#include <string.h>
#include <stdlib.h>
#include <algorithm>
namespace DATABASE_INCREASE {
	struct DMLRecord;
}
namespace META {
	static constexpr uint16_t numToStrMap[] = { 0x3030,0x3130,0x3230,0x3330,0x3430,0x3530,0x3630,0x3730,0x3830,0x3930,
												0x3031,0x3131,0x3231,0x3331,0x3431,0x3531,0x3631,0x3731,0x3831,0x3931,
												0x3032,0x3132,0x3232,0x3332,0x3432,0x3532,0x3632,0x3732,0x3832,0x3932,
												0x3033,0x3133,0x3233,0x3333,0x3433,0x3533,0x3633,0x3733,0x3833,0x3933,
												0x3034,0x3134,0x3234,0x3334,0x3434,0x3534,0x3634,0x3734,0x3834,0x3934,
												0x3035,0x3135,0x3235,0x3335,0x3435,0x3535,0x3635,0x3735,0x3835,0x3935,
												0x3036,0x3136,0x3236 };
	enum class COLUMN_TYPE {
		T_UNION = 0,
		T_UINT8 = 1,
		T_INT8 = 2,
		T_UINT16 = 3,
		T_INT16 = 4,
		T_UINT32 = 5,
		T_INT32 = 6,
		T_UINT64 = 7,
		T_INT64 = 8,
		T_BIG_NUMBER = 9,
		T_FLOAT = 10,
		T_DOUBLE = 11,
		T_DECIMAL = 12,
		T_TIMESTAMP = 13,
		T_DATETIME = 14,
		T_DATE = 15,
		T_YEAR = 16,
		T_TIME = 17,
		T_BLOB = 18,
		T_STRING = 19,
		T_JSON = 20,
		T_XML = 21,
		T_GEOMETRY = 22,
		T_SET = 23,
		T_ENUM = 24,
		T_BYTE = 25,
		T_BINARY = 26,
		T_TEXT = 27,
		T_BOOL = 28,
		T_CURRENT_VERSION_MAX_TYPE = 29,
		T_MAX_TYPE = 255
	};
#define TID(t) static_cast<uint8_t>(t)
	struct columnTypeInfo
	{
		COLUMN_TYPE type;
		uint8_t columnTypeSize;
		bool asIndex;
		bool fixed;
		bool stringType;
	};
	constexpr static columnTypeInfo columnInfos[] = {
	{COLUMN_TYPE::T_UNION,4,true,false,false},
	{COLUMN_TYPE::T_UINT8, 1,true,true,false},
	{COLUMN_TYPE::T_INT8,1 ,true,true,false},
	{COLUMN_TYPE::T_UINT16,2,true,true,false},
	{COLUMN_TYPE::T_INT16, 2,true,true,false},
	{COLUMN_TYPE::T_UINT32,4,true,true,false},
	{COLUMN_TYPE::T_INT32,4 ,true,true,false},
	{COLUMN_TYPE::T_UINT64,8,true,true,false},
	{COLUMN_TYPE::T_INT64,8,true,true,false},
	{COLUMN_TYPE::T_BIG_NUMBER,4 ,false,false,false},
	{COLUMN_TYPE::T_FLOAT,4,true,true,false},
	{COLUMN_TYPE::T_DOUBLE,8,true,true,false},
	{COLUMN_TYPE::T_DECIMAL,4,false,false,true},
	{COLUMN_TYPE::T_TIMESTAMP,8 ,true,true,false},
	{COLUMN_TYPE::T_DATETIME,8,true,true,false},
	{COLUMN_TYPE::T_DATE,4,true,true,false},
	{COLUMN_TYPE::T_YEAR,2,true,true,false},
	{COLUMN_TYPE::T_TIME,8,true,true,false},
	{COLUMN_TYPE::T_BLOB,4,true,true,false},
	{COLUMN_TYPE::T_STRING,4,true,false,true},
	{COLUMN_TYPE::T_JSON,4,false,true,true},
	{COLUMN_TYPE::T_XML,4,false,true,true},
	{COLUMN_TYPE::T_GEOMETRY,4,false,true,false},
	{COLUMN_TYPE::T_SET,8,false,true,true},
	{COLUMN_TYPE::T_ENUM,2,false,true,true},
	{COLUMN_TYPE::T_BYTE,8,false,true,false},
	{COLUMN_TYPE::T_BINARY,4,true,false,false},
	{COLUMN_TYPE::T_TEXT,4,true,false,true}
	};
#pragma pack(1)
	enum class KEY_TYPE {
		PRIMARY_KEY,
		UNIQUE_KEY,
		INDEX
	};
	struct uniqueKeyTypePair
	{
		uint8_t type;
		uint16_t columnId;
	};
	struct unionKeyMeta
	{
		uint16_t columnCount;
		uint16_t size;
		uint8_t fixed;
		uint8_t keyType;
		uint8_t keyId;
		uint8_t varColumnCount;
		uniqueKeyTypePair columnInfo[1];
		static inline uint32_t memSize(uint16_t keyCount)
		{
			return sizeof(unionKeyMeta) + sizeof(uniqueKeyTypePair) * (keyCount - 1);
		}
		unionKeyMeta()
		{
		}
		unionKeyMeta(const unionKeyMeta& dest)
		{
			memcpy(&columnCount, &dest.columnCount, memSize(dest.columnCount));
		}
		unionKeyMeta& operator=(const unionKeyMeta& dest)
		{
			memcpy(&columnCount, &dest.columnCount, memSize(dest.columnCount));
			return *this;
		}
		inline bool operator==(const unionKeyMeta& dest)
		{
			if (columnCount != dest.columnCount || memcmp(&columnCount, &dest.columnCount, memSize(columnCount) != 0))
				return false;
			else
				return true;
		}
		inline bool operator!=(const unionKeyMeta& dest)
		{
			return !(*this == dest);
		}
		void columnUpdate(uint16_t from, uint16_t to, COLUMN_TYPE newType)
		{
			for (int i = 0; i < columnCount; i++)
			{
				if (columnInfo[i].columnId<std::min<uint16_t>(from, to) || columnInfo[i].columnId > std::max<uint16_t>(from, to))
				{
					continue;
				}
				else if (columnInfo[i].columnId == from)
				{
					if (columnInfos[columnInfo[i].type].fixed && !columnInfos[static_cast<int>(newType)].fixed)
					{
						size -= columnInfos[columnInfo[i].type].columnTypeSize;
						size += sizeof(uint16_t);
						varColumnCount++;
					}
					else if (!columnInfos[columnInfo[i].type].fixed && columnInfos[static_cast<int>(newType)].fixed)
					{
						size -= sizeof(uint16_t);
						size += columnInfos[TID(newType)].columnTypeSize;
						varColumnCount--;
					}
					else if (columnInfos[columnInfo[i].type].fixed)
					{
						size -= columnInfos[columnInfo[i].type].columnTypeSize;
						size += columnInfos[TID(newType)].columnTypeSize;
					}
					columnInfo[i].columnId = to;
					columnInfo[i].type = TID(newType);
				}
				else
				{
					if (from > to)
						columnInfo[i].columnId++;
					else
						columnInfo[i].columnId--;
				}
			}
			if (varColumnCount > 0 && fixed > 0)
			{
				size += sizeof(uint16_t);
				fixed = 0;
			}
			else if (varColumnCount == 0 && fixed == 0)
			{
				size -= sizeof(uint16_t);
				fixed = 1;
			}
		}
	};
	struct binaryType {
		uint16_t size;
		const char* data;
		DLL_EXPORT binaryType();
		DLL_EXPORT binaryType(const char* _data, uint16_t _size);
		DLL_EXPORT binaryType(const binaryType& dest);
		DLL_EXPORT binaryType operator=(const binaryType& dest);
		inline bool operator< (const binaryType& dest) const
		{
			return  compare(dest) < 0;
		}
		inline bool operator==(const binaryType& dest) const
		{
			return compare(dest) == 0;
		}
		inline bool operator!=(const binaryType& dest) const
		{
			return compare(dest) != 0;
		}
		DLL_EXPORT int compare(const binaryType& dest) const;
		inline bool operator> (const binaryType& dest) const
		{
			return  compare(dest) > 0;
		}
	};
	struct unionKey {
		const char* key;
		const META::unionKeyMeta* meta;
		DLL_EXPORT unionKey();
		DLL_EXPORT unionKey(const char* key, const META::unionKeyMeta* meta) :key(key), meta(meta) {};
		DLL_EXPORT unionKey(const unionKey& dest);
		DLL_EXPORT int compare(const unionKey& dest) const;
		inline bool operator> (const unionKey& dest) const
		{
			return compare(dest) > 0;
		}
		inline bool operator< (const unionKey& dest) const
		{
			return compare(dest) < 0;
		}
		inline bool operator== (const unionKey& dest) const
		{
			return compare(dest) == 0;
		}
		inline bool operator!= (const unionKey& dest) const
		{
			return compare(dest) != 0;
		}
		static inline int externSize(const META::unionKeyMeta* meta)
		{
			return meta->varColumnCount * sizeof(uint16_t);
		}
		inline uint16_t startOffset()
		{
			return meta->fixed ? 0 : sizeof(uint16_t);
		}
		inline uint16_t appendValue(const void* value, uint16_t length, uint16_t columnId, uint16_t offset)
		{
			if (columnInfos[meta->columnInfo[columnId].type].fixed)
			{
				memcpy((char*)key + offset, value, columnInfos[meta->columnInfo[columnId].type].columnTypeSize);
				return offset + columnInfos[meta->columnInfo[columnId].type].columnTypeSize;
			}
			else
			{
				*(uint16_t*)(key + offset) = length;
				memcpy((char*)key + offset + sizeof(uint16_t), value, length);
				return offset + sizeof(uint16_t) + length;
			}
		}
		inline void setVarSize(uint16_t size)
		{
			*(uint16_t*)(key) = size;
		}
		DLL_EXPORT static uint16_t memSize(const DATABASE_INCREASE::DMLRecord* r, const META::unionKeyMeta* meta, bool keyUpdated);
		DLL_EXPORT static void initKey(char* key, uint16_t size, const META::unionKeyMeta* keyMeta, const DATABASE_INCREASE::DMLRecord* r, bool keyUpdated = false);
	};
	struct timestamp
	{
		union
		{
			/*Little-Endian */
			struct {
				uint32_t nanoSeconds : 30;
				uint64_t seconds : 34;
			};
			uint64_t time;
		};
		static inline uint64_t create(uint64_t seconds, uint32_t nanoSeconds)
		{
			timestamp t;
			t.seconds = seconds;
			t.nanoSeconds = nanoSeconds;
			return t.time;
		}
		inline uint8_t toString(char* str)
		{
			uint8_t len = u64toa_sse2(seconds, str);
			if (nanoSeconds != 0)
			{
				str[len - 1] = '.';
				char usec[8];
				*(uint64_t*)(str + len) = 3458817497345568816ull;//{'0','0','0','0','0','0','\0','0'}
				uint8_t secLen = u32toa_sse2(nanoSeconds / 1000, usec);
				memcpy(str + len + (6 - secLen + 1), usec, secLen);
				return len + 6;
			}
			else
				return len;

		}
	};
	/*[20byte usec][6 byte second][6 byte min][5 byte hour][5 byte day][4 byte month][18 bit year]*/
	struct dateTime
	{
		union
		{
			int64_t time;
			struct
			{
				uint32_t usec : 20;
				uint8_t sec : 6;
				uint8_t min : 6;
				uint8_t hour : 5;
				uint8_t day : 5;
				uint8_t month : 4;
				int32_t year : 18;
			};
		};
		static inline int64_t createDate(int32_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t mi, uint8_t sec, uint32_t usec)
		{
			dateTime d;
			d.set(year, month, day, hour, mi, sec, usec);
			return d.time;
		}
		inline void set(int32_t _year, uint8_t _month, uint8_t _day, uint8_t _hour, uint8_t _mi, uint8_t _sec, uint32_t _usec)
		{
			usec = _usec;
			sec = _sec;
			min = _mi;
			hour = _hour;
			day = _day;
			month = _month;
			year = _year;
		}
		inline uint8_t toString(char* str)
		{
			uint8_t yearLength = i32toa_sse2(year, str);
			str[yearLength - 1] = '-';
			*(uint16_t*)&str[yearLength] = numToStrMap[month];
			str[yearLength + 2] = '-';
			*(uint16_t*)&str[yearLength + 3] = numToStrMap[day];
			str[yearLength + 5] = ' ';
			*(uint16_t*)&str[yearLength + 6] = numToStrMap[hour];
			str[yearLength + 8] = ':';
			*(uint16_t*)&str[yearLength + 9] = numToStrMap[min];
			str[yearLength + 11] = ':';
			*(uint16_t*)&str[yearLength + 12] = numToStrMap[sec];
			if (usec != 0)
			{
				str[yearLength + 14] = '.';
				char usecBuffer[8];
				*(uint64_t*)(str + yearLength + 15) = 3458817497345568816ull;//{'0','0','0','0','0','0','\0','0'}
				uint8_t secLen = u32toa_sse2(usec, usecBuffer);
				memcpy(str + yearLength + 15 + 6 - secLen + 1, usecBuffer, secLen);
				return yearLength + 15 + 6 + 1;
			}
			else
			{
				str[yearLength + 14] = '\0';
				return yearLength + 15;
			}
		}
		inline uint8_t toDateString(char* str)
		{
			uint8_t yearLength = i32toa_sse2(year, str);
			str[yearLength - 1] = '-';
			*(uint16_t*)&str[yearLength] = numToStrMap[month];
			str[yearLength + 2] = '-';
			*(uint16_t*)&str[yearLength + 3] = numToStrMap[day];
			str[yearLength + 5] = '\0';
			return yearLength + 6;
		}
		inline uint8_t toTimeString(char* str)
		{
			uint8_t hourLength = 0;
			if (year < 0)
			{
				hourLength = 1;
				str[0] = '-';
			}
			hourLength += u32toa_sse2(hour, str + hourLength);
			str[hourLength - 1] = ':';
			*(uint16_t*)&str[hourLength] = numToStrMap[min];
			str[hourLength + 2] = ':';
			*(uint16_t*)&str[hourLength + 3] = numToStrMap[sec];
			if (usec != 0)
			{
				str[hourLength + 5] = '.';
				char usecBuffer[8];
				*(uint64_t*)(str + hourLength + 6) = 3458817497345568816ull;//{'0','0','0','0','0','0','\0','0'}
				uint8_t secLen = u32toa_sse2(usec, usecBuffer);
				memcpy(str + hourLength + 6 + 6 - secLen + 1, usecBuffer, secLen);
				return hourLength + 6 + 6 + 1;
			}
			else
			{
				str[hourLength + 5] = '\0';
				return hourLength + 6;
			}
		}
	};
	/*[24 bit usc][8 bit second][8 bit min][24 bit hour]*/
	struct Time
	{
		union
		{
			int64_t time;
			struct
			{
				uint32_t nsec : 30;
				uint8_t sec : 6;
				uint8_t min : 6;
				int32_t hour : 22;
			};
		};
		static inline int64_t createTime(int32_t hour, uint8_t mi, uint8_t second, uint32_t nsec)
		{
			Time t;
			t.hour = hour;
			t.min = mi;
			t.sec = second;
			t.nsec = nsec;
			return t.time;
		}
		inline void  set(int32_t hour, uint8_t mi, uint8_t second, uint32_t nsec)
		{
			this->hour = hour;
			this->min = mi;
			this->sec = second;
			this->nsec = nsec;
		}
		inline uint8_t toString(char* str)
		{
			uint8_t hourLength = u32toa_sse2(hour, str);
			str[hourLength - 1] = ':';
			*(uint16_t*)&str[hourLength] = numToStrMap[min];
			str[hourLength + 2] = ':';
			*(uint16_t*)&str[hourLength + 3] = numToStrMap[sec];
			if (nsec != 0)
			{
				str[hourLength + 5] = '.';
				char usecBuffer[8];
				*(uint64_t*)(str + hourLength + 6) = 3458817497345568816ull;//{'0','0','0','0','0','0','\0','0'}
				uint8_t secLen = u32toa_sse2(nsec / 1000, usecBuffer);
				memcpy(str + hourLength + 6 + 6 - secLen + 1, usecBuffer, secLen);
				return hourLength + 6 + 6 + 1;
			}
			else
			{
				str[hourLength + 5] = '\0';
				return hourLength + 6;
			}
		}
	};
	/*[1 byte day][1 byte month][2 byte year]*/
	struct Date {
		union
		{
			int32_t time;
			struct
			{
				uint8_t day : 5;
				uint8_t month : 4;
				int32_t year : 23;
			};
		};
		static inline int32_t createDate(int32_t _year, uint8_t _month, uint8_t _day)
		{
			Date d;
			d.day = _day;
			d.month = _month;
			d.year = _year;
			return d.time;
		}
		inline void set(int32_t _year, uint8_t _month, uint8_t _day)
		{
			day = _day;
			month = _month;
			year = _year;
		}
		uint8_t toString(char* str)
		{
			uint8_t yearLength = i32toa_sse2(year, str);
			str[yearLength - 1] = '-';
			*(uint16_t*)&str[yearLength] = numToStrMap[month];
			str[yearLength + 2] = '-';
			*(uint16_t*)&str[yearLength + 3] = numToStrMap[day];
			str[yearLength + 5] = '\0';
			return yearLength + 6;
		}

	};
#pragma pack()
}
