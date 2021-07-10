#include "function.h"
#include "meta/columnType.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
#include "field.h"
#include <time.h>
#ifdef OS_WIN
#include "util/winTime.h"
#endif
#ifdef OS_LINUX
#include <stdarg.h>
#endif
namespace SHELL {
	typedef spp::sparse_hash_map<const char*, rowFunction*, StrHash, StrCompare> ROW_FUNC_ARGV_MAP;
	typedef spp::sparse_hash_map<const char*, ROW_FUNC_ARGV_MAP*, StrHash, StrCompare> ROW_FUNC_MAP;

	typedef spp::sparse_hash_map<const char*, groupFunction*, StrHash, StrCompare> GROUP_FUNC_ARGV_MAP;
	typedef spp::sparse_hash_map<const char*, GROUP_FUNC_ARGV_MAP*, StrHash, StrCompare> GROUP_FUNC_MAP;
	class CONCAT_FUNC :public rowFunction {
	public:
		CONCAT_FUNC() :rowFunction(2, META::COLUMN_TYPE::T_STRING) {}
		virtual void* exec(Field** const valueList, const RPC::DMLRecord** const row)const
		{
			varLenValue* src = static_cast<varLenValue*>(valueList[0]->getValue(row));
			varLenValue* dest = static_cast<varLenValue*>(valueList[1]->getValue(row));
			varLenValue* newStr = (varLenValue*)shellGlobalBufferPool->allocByLevel(0);
			newStr->value = (char*)shellGlobalBufferPool->alloc(src->size + dest->size);
			memcpy((char*)newStr->value, src->value, src->size);
			memcpy((char*)newStr->value + src->size, dest->value, dest->size);
			newStr->alloced = true;
			newStr->size = src->size + dest->size;
			if (src->alloced)
				shellGlobalBufferPool->free((char*)src->value);
			if (dest->alloced)
				shellGlobalBufferPool->free((char*)dest->value);
			shellGlobalBufferPool->free(src);
			shellGlobalBufferPool->free(dest);
			return newStr;
		}
	};

	class NOW_FUNC :public rowFunction {
	public:
		NOW_FUNC() :rowFunction(0, META::COLUMN_TYPE::T_TIMESTAMP) {}
		virtual void* exec(Field** const argvs, const RPC::DMLRecord** const row) const
		{
			struct timespec tm;
			clock_gettime(CLOCK_REALTIME, &tm);
			return (void*)META::Timestamp::create(tm.tv_sec, tm.tv_nsec);
		}
	};
	/*****************************************************inner functions********************************************************/
	/*
	func MAX(a,b)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
	*/
	template<typename T>
	class MAX_FUNC :public rowFunction {
	public:
		MAX_FUNC(META::COLUMN_TYPE typeCode) :rowFunction(2, typeCode)
		{
		}
		virtual void* exec(Field** const argvs, const RPC::DMLRecord** const row) const
		{
			void* s = argvs[0]->getValue(row), * d = argvs[1]->getValue(row);
			if (*static_cast<T*>((void*)&s) > * static_cast<T*>((void*)&d))
				return s;
			else
				return d;
		}
	};
	/*
	func MAX(a,b)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
	*/
	template<typename T>
	class MIN_FUNC :public rowFunction {
	public:
		MIN_FUNC(META::COLUMN_TYPE typeCode) :rowFunction(2, typeCode)
		{
		}
		virtual void* exec(Field** const argvs, const RPC::DMLRecord** const row) const
		{
			void* s = argvs[0]->getValue(row), * d = argvs[1]->getValue(row);
			if (*static_cast<T*>((void*)&s) < *static_cast<T*>((void*)&d))
				return s;
			else
				return d;
		}
	};
	/*
	func ABS(x)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,double,float
	*/
	template<typename T>
	class ABS_FUNC :public rowFunction {
	public:
		ABS_FUNC(META::COLUMN_TYPE typeCode) :rowFunction(1, typeCode)
		{
		}
		virtual void* exec(Field** const valueList, const RPC::DMLRecord** const row)const
		{
			void* s = valueList[0]->getValue(row);
			if (*static_cast<T*>(s) < (T)0)
			{

				T v = 0 - *static_cast<T*>(s);
				return *(void**)(void*)&v;
			}
			else
				return valueList[0];
		}
	};
	/*
	func GROUP_MAX_FUNC(x)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
	*/
	template<typename T>
	class GROUP_MAX_FUNC :public groupFunction {
	public:
		GROUP_MAX_FUNC(META::COLUMN_TYPE typeCode) :groupFunction(1, typeCode)
		{
		}
		virtual void exec(Field** const valueList, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const
		{
			void* s = valueList[0]->getValue(row);
			if (count == 0 || *static_cast<T*>((void*)&historyValue) < *static_cast<T*>((void*)&s))
				historyValue = s;
			count++;
		}
		virtual void* finalValueFunc(void* historyValue, uint32_t count)const
		{
			return historyValue;
		}

	};
	/*
	func GROUP_MIN_FUNC(x)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
	*/
	template<typename T>
	class GROUP_MIN_FUNC :public groupFunction {
	public:
		GROUP_MIN_FUNC(META::COLUMN_TYPE typeCode) :groupFunction(1, typeCode)
		{
		}
		virtual void exec(Field** const argvs, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const
		{
			void* s = argvs[0]->getValue(row);
			if (count == 0 || *static_cast<T*>((void*)&historyValue) > * static_cast<T*>((void*)&s))
				historyValue = s;
			count++;
		}
		virtual void* finalValueFunc(void* historyValue, uint32_t count)const
		{
			return historyValue;
		}
	};
	/*
	func GROUP_AVG_FUNC(x)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,double,float,timestamp
	*/
	template<typename T>
	class GROUP_AVG_FUNC :public groupFunction {
	public:
		GROUP_AVG_FUNC(META::COLUMN_TYPE typeCode) :groupFunction(1, typeCode)
		{
		}
		virtual void exec(Field** const valueList, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const
		{
			void* s = valueList[0]->getValue(row);
			if (count == 0)
				historyValue = s;
			else
			{
				T v = *static_cast<T*>((void*)&historyValue) + *static_cast<T*>((void*)&s);
				historyValue = *(void**)(void*)&v;
			}
			count++;
		}
		virtual void* finalValueFunc(void* historyValue, uint32_t count)const
		{
			T v = *static_cast<T*>((void*)&historyValue) / count;
			return *(void**)(void*)&v;
		}
	};
	/*
	func GROUP_SUM_FUNC(x)
	support int32,uint32,int16,uint16,uint8,int8,int64,uint64,double,float,timestamp
	*/
	template<typename T>
	class GROUP_SUM_FUNC :public groupFunction {
	public:
		GROUP_SUM_FUNC(META::COLUMN_TYPE typeCode) :groupFunction(1, typeCode)
		{
		}
		virtual void exec(Field** const valueList, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const
		{
			void* s = valueList[0]->getValue(row);
			if (count == 0)
				historyValue = s;
			else
			{
				T v = *static_cast<T*>((void*)&historyValue) + *static_cast<T*>((void*)&s);
				historyValue = *(void**)(void*)&v;
			}
			count++;
		}
		virtual void* finalValueFunc(void* historyValue, uint32_t count)const
		{
			return historyValue;
		}
	};
	/*
	func GROUP_COUNT_FUNC(x)
	support all type
	*/
	template<typename T>
	class GROUP_COUNT_FUNC :public groupFunction {
	public:
		GROUP_COUNT_FUNC() :groupFunction(1, META::COLUMN_TYPE::T_UINT32)
		{
		}
		virtual void exec(Field** const valueList, void*& historyValue, uint32_t& count, const RPC::DMLRecord** const row)const
		{
			count++;
		}
		virtual void* finalValueFunc(void* historyValue, uint32_t count)const
		{
			return *(void**)(void*)&count;
		}
	};
	ROW_FUNC_MAP globalRowFuncMap;
	GROUP_FUNC_MAP globalGroupFuncMap;
	const char* createString(META::COLUMN_TYPE c, ...)
	{
		std::list<uint8_t> chars;
		chars.push_back(TID(c));
		uint8_t _c;
		char* realString = nullptr;
		int realStringSize = 0;
		if (TID(c) == 0)
			goto end;
		va_list vl;
		va_start(vl, c);
		do {
			_c = TID(va_arg(vl, META::COLUMN_TYPE));
			chars.push_back(_c);
			if ('\0' == chars.back())
				break;
		} while (true);
		va_end(vl);
	end:
		realString = (char*)shellGlobalBufferPool->alloc(chars.size());
		for (std::list<uint8_t>::const_iterator iter = chars.begin(); iter != chars.end(); iter++)
			realString[realStringSize++] = (char)*iter;
		return realString;
	}
#define ADD_FUNC(funcs,words,func) do{funcs->insert(std::pair<const char*, rowFunction*>(words,func));}while(0);
	void initFunction()
	{
		/*concat*/
		ROW_FUNC_ARGV_MAP* concatFuncs = new  ROW_FUNC_ARGV_MAP();
		concatFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_STRING, META::COLUMN_TYPE::T_STRING, 0), new CONCAT_FUNC()));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("concat", concatFuncs));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("CONCAT", concatFuncs));
		/*now*/
		ROW_FUNC_ARGV_MAP* nowFuncs = new  ROW_FUNC_ARGV_MAP();
		nowFuncs->insert(std::pair<const char*, rowFunction*>("\0", new NOW_FUNC()));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("now", nowFuncs));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("NOW", nowFuncs));
		/*max*/
		ROW_FUNC_ARGV_MAP* maxFuncs = new  ROW_FUNC_ARGV_MAP();
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT32, META::COLUMN_TYPE::T_INT32, 0), new MAX_FUNC<int32_t>(META::COLUMN_TYPE::T_INT32)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT64, META::COLUMN_TYPE::T_INT64, 0), new MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_INT64)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT32, META::COLUMN_TYPE::T_UINT32, 0), new MAX_FUNC<uint32_t>(META::COLUMN_TYPE::T_UINT32)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT64, META::COLUMN_TYPE::T_UINT64, 0), new MAX_FUNC<uint64_t>(META::COLUMN_TYPE::T_UINT64)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT8, META::COLUMN_TYPE::T_INT8, 0), new MAX_FUNC<int8_t>(META::COLUMN_TYPE::T_INT8)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT8, META::COLUMN_TYPE::T_UINT8, 0), new MAX_FUNC<uint8_t>(META::COLUMN_TYPE::T_UINT8)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT16, META::COLUMN_TYPE::T_INT16, 0), new MAX_FUNC<uint16_t>(META::COLUMN_TYPE::T_INT16)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT16, META::COLUMN_TYPE::T_UINT16, 0), new MAX_FUNC<uint16_t>(META::COLUMN_TYPE::T_UINT16)));

		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_FLOAT, META::COLUMN_TYPE::T_FLOAT, 0), new MAX_FUNC<float>(META::COLUMN_TYPE::T_FLOAT)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DOUBLE, META::COLUMN_TYPE::T_DOUBLE, 0), new MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_DOUBLE)));

		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_TIMESTAMP, META::COLUMN_TYPE::T_TIMESTAMP, 0), new MAX_FUNC<uint64_t>(META::COLUMN_TYPE::T_TIMESTAMP)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DATETIME, META::COLUMN_TYPE::T_DATETIME, 0), new MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_DATETIME)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_TIME, META::COLUMN_TYPE::T_TIME, 0), new MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_TIME)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DATE, META::COLUMN_TYPE::T_DATE, 0), new MAX_FUNC<int32_t>(META::COLUMN_TYPE::T_DATE)));
		maxFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_YEAR, META::COLUMN_TYPE::T_YEAR, 0), new MAX_FUNC<int16_t>(META::COLUMN_TYPE::T_YEAR)));

		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("max", maxFuncs));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("MAX", maxFuncs));
		/*min*/
		ROW_FUNC_ARGV_MAP* minFuncs = new  ROW_FUNC_ARGV_MAP();
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT32, META::COLUMN_TYPE::T_INT32, 0), new MIN_FUNC<int32_t>(META::COLUMN_TYPE::T_INT32)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT64, META::COLUMN_TYPE::T_INT64, 0), new MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_INT64)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT32, META::COLUMN_TYPE::T_UINT32, 0), new MIN_FUNC<uint32_t>(META::COLUMN_TYPE::T_UINT32)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT64, META::COLUMN_TYPE::T_UINT64, 0), new MIN_FUNC<uint64_t>(META::COLUMN_TYPE::T_UINT64)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT8, META::COLUMN_TYPE::T_INT8, 0), new MIN_FUNC<int8_t>(META::COLUMN_TYPE::T_INT8)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT8, META::COLUMN_TYPE::T_UINT8, 0), new MIN_FUNC<uint8_t>(META::COLUMN_TYPE::T_UINT8)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT16, META::COLUMN_TYPE::T_INT16, 0), new MIN_FUNC<uint16_t>(META::COLUMN_TYPE::T_INT16)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT16, META::COLUMN_TYPE::T_UINT16, 0), new MIN_FUNC<uint16_t>(META::COLUMN_TYPE::T_UINT16)));

		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_FLOAT, META::COLUMN_TYPE::T_FLOAT, 0), new MIN_FUNC<float>(META::COLUMN_TYPE::T_FLOAT)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DOUBLE, META::COLUMN_TYPE::T_DOUBLE, 0), new MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_DOUBLE)));

		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_TIMESTAMP, META::COLUMN_TYPE::T_TIMESTAMP, 0), new MIN_FUNC<uint64_t>(META::COLUMN_TYPE::T_TIMESTAMP)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DATETIME, META::COLUMN_TYPE::T_DATETIME, 0), new MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_DATETIME)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_TIME, META::COLUMN_TYPE::T_TIME, 0), new MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_TIME)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DATE, META::COLUMN_TYPE::T_DATE, 0), new MIN_FUNC<int32_t>(META::COLUMN_TYPE::T_DATE)));
		minFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_YEAR, META::COLUMN_TYPE::T_YEAR, 0), new MIN_FUNC<int16_t>(META::COLUMN_TYPE::T_YEAR)));

		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("min", minFuncs));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("MIN", minFuncs));

		/*abs*/
		ROW_FUNC_ARGV_MAP* absFuncs = new  ROW_FUNC_ARGV_MAP();
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT32, 0), new ABS_FUNC<int32_t>(META::COLUMN_TYPE::T_INT32)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT64, 0), new ABS_FUNC<int64_t>(META::COLUMN_TYPE::T_INT64)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT32, 0), new ABS_FUNC<uint32_t>(META::COLUMN_TYPE::T_UINT32)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT64, 0), new ABS_FUNC<uint64_t>(META::COLUMN_TYPE::T_UINT64)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT8, 0), new ABS_FUNC<int8_t>(META::COLUMN_TYPE::T_INT8)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT8, 0), new ABS_FUNC<uint8_t>(META::COLUMN_TYPE::T_UINT8)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_INT16, 0), new ABS_FUNC<uint16_t>(META::COLUMN_TYPE::T_INT16)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_UINT16, 0), new ABS_FUNC<uint16_t>(META::COLUMN_TYPE::T_UINT16)));

		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_FLOAT, 0), new ABS_FUNC<float>(META::COLUMN_TYPE::T_FLOAT)));
		absFuncs->insert(std::pair<const char*, rowFunction*>(createString(META::COLUMN_TYPE::T_DOUBLE, 0), new ABS_FUNC<int64_t>(META::COLUMN_TYPE::T_DOUBLE)));

		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("abs", absFuncs));
		globalRowFuncMap.insert(std::pair<const char*, ROW_FUNC_ARGV_MAP*>("ABS", absFuncs));


		/*-----------------------------------------------group-------------------------------------------------*/
		GROUP_FUNC_ARGV_MAP* groupMaxFunc = new GROUP_FUNC_ARGV_MAP();
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT32, 0), new GROUP_MAX_FUNC<int32_t>(META::COLUMN_TYPE::T_INT32)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT64, 0), new GROUP_MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_INT64)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT32, 0), new GROUP_MAX_FUNC<uint32_t>(META::COLUMN_TYPE::T_UINT32)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT64, 0), new GROUP_MAX_FUNC<uint64_t>(META::COLUMN_TYPE::T_UINT64)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT8, 0), new GROUP_MAX_FUNC<int8_t>(META::COLUMN_TYPE::T_INT8)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT8, 0), new GROUP_MAX_FUNC<uint8_t>(META::COLUMN_TYPE::T_UINT8)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT16, 0), new GROUP_MAX_FUNC<uint16_t>(META::COLUMN_TYPE::T_INT16)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT16, 0), new GROUP_MAX_FUNC<uint16_t>(META::COLUMN_TYPE::T_UINT16)));

		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_FLOAT, 0), new GROUP_MAX_FUNC<float>(META::COLUMN_TYPE::T_FLOAT)));
		groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_DOUBLE, 0), new GROUP_MAX_FUNC<int64_t>(META::COLUMN_TYPE::T_DOUBLE)));

		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("max", groupMaxFunc));
		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("MAX", groupMaxFunc));

		GROUP_FUNC_ARGV_MAP* groupMinFunc = new GROUP_FUNC_ARGV_MAP();
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT32, 0), new GROUP_MIN_FUNC<int32_t>(META::COLUMN_TYPE::T_INT32)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT64, 0), new GROUP_MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_INT64)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT32, 0), new GROUP_MIN_FUNC<uint32_t>(META::COLUMN_TYPE::T_UINT32)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT64, 0), new GROUP_MIN_FUNC<uint64_t>(META::COLUMN_TYPE::T_UINT64)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT8, 0), new GROUP_MIN_FUNC<int8_t>(META::COLUMN_TYPE::T_INT8)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT8, 0), new GROUP_MIN_FUNC<uint8_t>(META::COLUMN_TYPE::T_UINT8)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_INT16, 0), new GROUP_MIN_FUNC<uint16_t>(META::COLUMN_TYPE::T_INT16)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_UINT16, 0), new GROUP_MIN_FUNC<uint16_t>(META::COLUMN_TYPE::T_UINT16)));

		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_FLOAT, 0), new GROUP_MIN_FUNC<float>(META::COLUMN_TYPE::T_FLOAT)));
		groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_DOUBLE, 0), new GROUP_MIN_FUNC<int64_t>(META::COLUMN_TYPE::T_DOUBLE)));

		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("min", groupMinFunc));
		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("MIN", groupMinFunc));

		GROUP_FUNC_ARGV_MAP* groupCountFunc = new GROUP_FUNC_ARGV_MAP();
		groupCountFunc->insert(std::pair<const char*, groupFunction*>(createString(META::COLUMN_TYPE::T_MAX_TYPE, 0), new GROUP_COUNT_FUNC<int32_t>()));
		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("count", groupCountFunc));
		globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("COUNT", groupCountFunc));
	}
	const rowFunction* getRowFunction(const char* name, const char* argvTypes)
	{
		ROW_FUNC_MAP::iterator iter = globalRowFuncMap.find(name);
		if (iter == globalRowFuncMap.end())
			return nullptr;
		ROW_FUNC_ARGV_MAP::iterator fiter = iter->second->find(argvTypes);
		if (fiter == iter->second->end())
			return nullptr;
		else
			return fiter->second;
	}
	const groupFunction* getGroupFunction(const char* name, const char* argvTypes)
	{
		GROUP_FUNC_MAP::iterator iter = globalGroupFuncMap.find(name);
		if (iter == globalGroupFuncMap.end())
			return nullptr;
		GROUP_FUNC_ARGV_MAP::iterator fiter = iter->second->find(argvTypes);
		if (fiter == iter->second->end())
			return nullptr;
		else
			return fiter->second;
	}
}
