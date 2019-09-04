#include "function.h"
#include "meta/columnType.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
#include "field.h"
#include <time.h>
#ifdef OS_WIN
#include "util/winTime.h"
#endif
namespace STORE {
	namespace SHELL {
		typedef spp::sparse_hash_map<const char*, function*, StrHash, StrCompare> FUNC_ARGV_MAP;
		typedef spp::sparse_hash_map<const char*, FUNC_ARGV_MAP*, StrHash, StrCompare> FUNC_MAP;

		typedef spp::sparse_hash_map<const char*, groupFunction*, StrHash, StrCompare> GROUP_FUNC_ARGV_MAP;
		typedef spp::sparse_hash_map<const char*, GROUP_FUNC_ARGV_MAP*, StrHash, StrCompare> GROUP_FUNC_MAP;
		class CONCAT_FUNC :public function {
		public:
			CONCAT_FUNC() :function(2, META::T_STRING) {}
			virtual void* exec(const field** valueList, const DATABASE_INCREASE::DMLRecord* row)const
			{

			}
		};

		class NOW_FUNC :public function {
		public:
			NOW_FUNC() :function(0, META::T_TIMESTAMP) {}
			virtual void* exec(field** const argvs, const DATABASE_INCREASE::DMLRecord* row) const
			{
				struct timespec tm;
				clock_gettime(CLOCK_REALTIME, &tm);
				return (void*)META::timestamp::create(tm.tv_sec, tm.tv_nsec);
			}
		};
		/*****************************************************inner functions********************************************************/
		/*
		func MAX(a,b)
		support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
		*/
		template<typename T>
		class MAX_FUNC :public function {
		public:
			MAX_FUNC(uint8_t typeCode) :function(2, typeCode)
			{
			}
			virtual void* exec(field** const argvs, const DATABASE_INCREASE::DMLRecord* row) const
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
		class MIN_FUNC :public function {
		public:
			MIN_FUNC(uint8_t typeCode) :function(2, typeCode)
			{
			}
			virtual void* exec(field** const argvs, const DATABASE_INCREASE::DMLRecord* row) const
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
		class ABS_FUNC :public function {
		public:
			ABS_FUNC(uint8_t typeCode) :function(1, typeCode)
			{
			}
			virtual void* exec( field** const valueList, const DATABASE_INCREASE::DMLRecord* row)const
			{
				void* s = valueList[0]->getValue(row);
				if (*static_cast<T*>((void*)&s) < (T)0)
				{

					T v = 0 - *static_cast<T*>((void*)&s);
					return *(void**)(void*)& v;
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
			GROUP_MAX_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const
			{
				void* s = currentValue->getValue(row);
				if (count == 0 || *static_cast<T*>((void*)& historyValue) < *static_cast<T*>((void*)&s))
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
			GROUP_MIN_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const
			{
				void* s = currentValue->getValue(row);
				if (count == 0 || *static_cast<T*>((void*)& historyValue) > * static_cast<T*>((void*)&s))
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
			GROUP_AVG_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const
			{
				void* s = currentValue->getValue(row);
				if (count == 0)
					historyValue = s;
				else
				{
					T v = *static_cast<T*>((void*)& historyValue) + *static_cast<T*>((void*)& s);
					historyValue = *(void**)(void*)& v;
				}
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)const
			{
				T v = *static_cast<T*>((void*)& historyValue) / count;
				return *(void**)(void*)& v;
			}
		};
		/*
		func GROUP_SUM_FUNC(x)
		support int32,uint32,int16,uint16,uint8,int8,int64,uint64,double,float,timestamp
		*/
		template<typename T>
		class GROUP_SUM_FUNC :public groupFunction {
		public:
			GROUP_SUM_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const
			{
				void* s = currentValue->getValue(row);
				if (count == 0)
					historyValue = s;
				else
				{
					T v = *static_cast<T*>((void*)& historyValue) + *static_cast<T*>((void*)& s);
					historyValue = *(void**)(void*)& v;
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
			GROUP_COUNT_FUNC(uint8_t typeCode) :groupFunction(META::T_UINT32, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)const
			{
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)const
			{
				return *(void**)(void*)& count;
			}
		};
		FUNC_MAP globalFuncMap;
		GROUP_FUNC_MAP globalGroupFuncMap;
		const char* createString(char c,...)
		{
			std::list<char> chars;
			chars.push_back(c);
			char* realString = nullptr;
			int realStringSize = 0;
			if (c == '\0')
				goto end;
			va_list vl;
			va_start(vl, c);
			do {
				chars.push_back(va_arg(vl, char));
				if ('\0' == chars.back())
					break;
			} while (true);
			va_end(vl);
		end:
			realString = (char*)shellGlobalBufferPool.alloc(chars.size());
			for (std::list<char>::const_iterator iter = chars.begin(); iter != chars.end(); iter++)
				realString[realStringSize++] = *iter;
			return realString;
		}
#define ADD_FUNC(funcs,words,func) do{funcs->insert(std::pair<const char*, function*>(words,func));}while(0);
		void initFunction()
		{
			/*now*/
			FUNC_ARGV_MAP* nowFuncs = new  FUNC_ARGV_MAP();
			nowFuncs->insert(std::pair<const char*, function*>("\0" , new NOW_FUNC()));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("now", nowFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("NOW", nowFuncs));
			/*max*/
			FUNC_ARGV_MAP* maxFuncs = new  FUNC_ARGV_MAP();
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_INT32 ,META::T_INT32 ,0 ), new MAX_FUNC<int32_t>(META::T_INT32)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_INT64 ,META::T_INT64 ,0 ), new MAX_FUNC<int64_t>(META::T_INT64)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_UINT32 ,META::T_UINT32 ,0 ), new MAX_FUNC<uint32_t>(META::T_UINT32)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_UINT64 ,META::T_UINT64 ,0 ), new MAX_FUNC<uint64_t>(META::T_UINT64)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_INT8 ,META::T_INT8 ,0 ), new MAX_FUNC<int8_t>(META::T_INT8)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_UINT8 ,META::T_UINT8 ,0 ), new MAX_FUNC<uint8_t>(META::T_UINT8)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_INT16 ,META::T_INT16 ,0 ), new MAX_FUNC<uint16_t>(META::T_INT16)));
			maxFuncs->insert(std::pair<const char*, function*>(createString(META::T_UINT16 ,META::T_UINT16 ,0 ), new MAX_FUNC<uint16_t>(META::T_UINT16)));

			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_FLOAT ,META::T_FLOAT ,0 ), new MAX_FUNC<float>(META::T_FLOAT)));
			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_DOUBLE ,META::T_DOUBLE ,0 ), new MAX_FUNC<int64_t>(META::T_DOUBLE)));

			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_TIMESTAMP ,META::T_TIMESTAMP ,0 ), new MAX_FUNC<uint64_t>(META::T_TIMESTAMP)));
			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_DATETIME ,META::T_DATETIME ,0 ), new MAX_FUNC<int64_t>(META::T_DATETIME)));
			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_TIME ,META::T_TIME ,0 ), new MAX_FUNC<int64_t>(META::T_TIME)));
			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_DATE ,META::T_DATE ,0 ), new MAX_FUNC<int32_t>(META::T_DATE)));
			maxFuncs->insert(std::pair<const char*, function*>(createString( META::T_YEAR ,META::T_YEAR ,0 ), new MAX_FUNC<int16_t>(META::T_YEAR)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("max", maxFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MAX", maxFuncs));
			/*min*/
			FUNC_ARGV_MAP* minFuncs = new  FUNC_ARGV_MAP();
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT32 ,META::T_INT32 ,0 ), new MIN_FUNC<int32_t>(META::T_INT32)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT64 ,META::T_INT64 ,0 ), new MIN_FUNC<int64_t>(META::T_INT64)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT32 ,META::T_UINT32 ,0 ), new MIN_FUNC<uint32_t>(META::T_UINT32)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT64 ,META::T_UINT64 ,0 ), new MIN_FUNC<uint64_t>(META::T_UINT64)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT8 ,META::T_INT8 ,0 ), new MIN_FUNC<int8_t>(META::T_INT8)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT8 ,META::T_UINT8 ,0 ), new MIN_FUNC<uint8_t>(META::T_UINT8)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT16 ,META::T_INT16 ,0 ), new MIN_FUNC<uint16_t>(META::T_INT16)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT16 ,META::T_UINT16 ,0 ), new MIN_FUNC<uint16_t>(META::T_UINT16)));

			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_FLOAT ,META::T_FLOAT ,0 ), new MIN_FUNC<float>(META::T_FLOAT)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_DOUBLE ,META::T_DOUBLE ,0 ), new MIN_FUNC<int64_t>(META::T_DOUBLE)));

			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_TIMESTAMP ,META::T_TIMESTAMP ,0 ), new MIN_FUNC<uint64_t>(META::T_TIMESTAMP)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_DATETIME ,META::T_DATETIME ,0 ), new MIN_FUNC<int64_t>(META::T_DATETIME)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_TIME ,META::T_TIME ,0 ), new MIN_FUNC<int64_t>(META::T_TIME)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_DATE ,META::T_DATE ,0 ), new MIN_FUNC<int32_t>(META::T_DATE)));
			minFuncs->insert(std::pair<const char*, function*>(createString( META::T_YEAR ,META::T_YEAR ,0 ), new MIN_FUNC<int16_t>(META::T_YEAR)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("min", minFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MIN", minFuncs));

			/*abs*/
			FUNC_ARGV_MAP* absFuncs = new  FUNC_ARGV_MAP();
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT32  ,0 ), new ABS_FUNC<int32_t>(META::T_INT32)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT64  ,0 ), new ABS_FUNC<int64_t>(META::T_INT64)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT32  ,0 ), new ABS_FUNC<uint32_t>(META::T_UINT32)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT64  ,0 ), new ABS_FUNC<uint64_t>(META::T_UINT64)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT8  ,0 ), new ABS_FUNC<int8_t>(META::T_INT8)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT8  ,0 ), new ABS_FUNC<uint8_t>(META::T_UINT8)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_INT16  ,0 ), new ABS_FUNC<uint16_t>(META::T_INT16)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_UINT16  ,0 ), new ABS_FUNC<uint16_t>(META::T_UINT16)));

			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_FLOAT  ,0 ), new ABS_FUNC<float>(META::T_FLOAT)));
			absFuncs->insert(std::pair<const char*, function*>(createString( META::T_DOUBLE ,0 ), new ABS_FUNC<int64_t>(META::T_DOUBLE)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("abs", absFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("ABS", absFuncs));


			/*-----------------------------------------------group-------------------------------------------------*/
			GROUP_FUNC_ARGV_MAP* groupMaxFunc = new GROUP_FUNC_ARGV_MAP();
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT32  ,0 ), new GROUP_MAX_FUNC<int32_t>(META::T_INT32)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT64  ,0 ), new GROUP_MAX_FUNC<int64_t>(META::T_INT64)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT32  ,0 ), new GROUP_MAX_FUNC<uint32_t>(META::T_UINT32)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT64  ,0 ), new GROUP_MAX_FUNC<uint64_t>(META::T_UINT64)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT8  ,0 ), new GROUP_MAX_FUNC<int8_t>(META::T_INT8)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT8  ,0 ), new GROUP_MAX_FUNC<uint8_t>(META::T_UINT8)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT16  ,0 ), new GROUP_MAX_FUNC<uint16_t>(META::T_INT16)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT16  ,0 ), new GROUP_MAX_FUNC<uint16_t>(META::T_UINT16)));

			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_FLOAT  ,0 ), new GROUP_MAX_FUNC<float>(META::T_FLOAT)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_DOUBLE ,0 ), new GROUP_MAX_FUNC<int64_t>(META::T_DOUBLE)));

			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("max", groupMaxFunc));
			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("MAX", groupMaxFunc));

			GROUP_FUNC_ARGV_MAP* groupMinFunc = new GROUP_FUNC_ARGV_MAP();
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT32  ,0 ), new GROUP_MIN_FUNC<int32_t>(META::T_INT32)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT64  ,0 ), new GROUP_MIN_FUNC<int64_t>(META::T_INT64)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT32  ,0 ), new GROUP_MIN_FUNC<uint32_t>(META::T_UINT32)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT64  ,0 ), new GROUP_MIN_FUNC<uint64_t>(META::T_UINT64)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT8  ,0 ), new GROUP_MIN_FUNC<int8_t>(META::T_INT8)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT8  ,0 ), new GROUP_MIN_FUNC<uint8_t>(META::T_UINT8)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_INT16  ,0 ), new GROUP_MIN_FUNC<uint16_t>(META::T_INT16)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_UINT16  ,0 ), new GROUP_MIN_FUNC<uint16_t>(META::T_UINT16)));

			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_FLOAT  ,0 ), new GROUP_MIN_FUNC<float>(META::T_FLOAT)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_DOUBLE ,0 ), new GROUP_MIN_FUNC<int64_t>(META::T_DOUBLE)));

			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("min", groupMinFunc));
			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("MIN", groupMinFunc));

			GROUP_FUNC_ARGV_MAP* groupCountFunc = new GROUP_FUNC_ARGV_MAP();
			groupCountFunc->insert(std::pair<const char*, groupFunction*>(createString( META::T_CURRENT_VERSION_MAX_TYPE  ,0 ), new GROUP_COUNT_FUNC<int32_t>(META::T_INT32)));
			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("count", groupCountFunc));
			globalGroupFuncMap.insert(std::pair<const char*, GROUP_FUNC_ARGV_MAP*>("COUNT", groupCountFunc));
		}
		const function* getFunction(const char* name, const char* argvTypes)
		{
			FUNC_MAP::iterator iter = globalFuncMap.find(name);
			if (iter == globalFuncMap.end())
				return nullptr;
			FUNC_ARGV_MAP::iterator fiter = iter->second->find(argvTypes);
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

}
