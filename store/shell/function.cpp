#include "function.h"
#include "meta/columnType.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
#include "field.h"
namespace STORE {
	namespace SHELL {
		typedef spp::sparse_hash_map<const char*, function*, StrHash, StrCompare> FUNC_ARGV_MAP;
		typedef spp::sparse_hash_map<const char*, FUNC_ARGV_MAP*, StrHash, StrCompare> FUNC_MAP;

		typedef spp::sparse_hash_map<const char*, groupFunction*, StrHash, StrCompare> GROUP_FUNC_ARGV_MAP;
		typedef spp::sparse_hash_map<const char*, GROUP_FUNC_ARGV_MAP*, StrHash, StrCompare> GROUP_FUNC_MAP;

		/*****************************************************inner functions********************************************************/
		/*
		func MAX(a,b)
		support int32,uint32,int16,uint16,uint8,int8,int64,uint64,timestamp,datetime,date,time,year,double,float
		*/
		template<typename T>
		class MAX_FUNC :public function {
			MAX_FUNC(uint8_t typeCode) :function(2, typeCode)
			{
			}
			virtual void* exec(const field** valueList, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = valueList[0]->getValue(row), * d = valueList[1]->getValue(row);
				if (*static_cast<T*>(&s) > *static_cast<T*>(&d))
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
			MIN_FUNC(uint8_t typeCode) :function(2, typeCode)
			{
			}
			virtual void* exec(const field** valueList, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = valueList[0]->getValue(row), * d = valueList[1]->getValue(row);
				if (*static_cast<T*>(&s) < *static_cast<T*>(&d))
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
			ABS_FUNC(uint8_t typeCode) :function(1, typeCode)
			{
			}
			virtual void* exec(const field** valueList, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = valueList[0]->getValue(row);
				if (*static_cast<T*>(&s) < (T)0)
				{
					
					T v = 0 - *static_cast<T*>(&s);
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
			GROUP_MAX_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = currentValue->getValue(row);
				if (count == 0 || *static_cast<T*>((void*)&historyValue) < *static_cast<T*>(&s))
					historyValue = s;
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)
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
			GROUP_MIN_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = currentValue->getValue(row);
				if (count == 0 || *static_cast<T*>((void*)&historyValue) > *static_cast<T*>(&s))
					historyValue = s;
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)
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
			GROUP_AVG_FUNC(uint8_t typeCode) :groupFunction(typeCode, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = currentValue->getValue(row);
				if (count == 0)
					historyValue = s;
				else
				{
					T v = *static_cast<T*>((void*)&historyValue) + *static_cast<T*>((void*)&s);
					historyValue = *(void**)(void*)&v;
				}
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)
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
			GROUP_SUM_FUNC(uint8_t typeCode) :groupFunction(typeCode,typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)
			{
				void* s = currentValue->getValue(row);
				if (count == 0)
					historyValue = s;
				else
				{
					T v = *static_cast<T*>((void*)&historyValue) + *static_cast<T*>((void*)&s);
					historyValue = *(void**)(void*)&v;
				}
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)
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
			GROUP_COUNT_FUNC(uint8_t typeCode) :groupFunction(T_UINT32, typeCode)
			{
			}
			virtual void exec(const field* currentValue, void*& historyValue, uint32_t& count, const DATABASE_INCREASE::DMLRecord* row)
			{
				count++;
			}
			virtual void* finalValueFunc(void* historyValue, uint32_t count)
			{
				return *(void**)(void*)&count;
			}
		};
		FUNC_MAP globalFuncMap;
		GROUP_FUNC_MAP globalGroupFuncMap;
		void initFunction()
		{
			/*max*/
			FUNC_ARGV_MAP* maxFuncs = new  FUNC_ARGV_MAP();
			maxFuncs->insert(std::pair<const char*, function*>({ T_INT32 ,T_INT32 ,0 }, new MAX_FUNC<int32_t>(T_INT32)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_INT64 ,T_INT64 ,0 }, new MAX_FUNC<int64_t>(T_INT64)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_UINT32 ,T_UINT32 ,0 }, new MAX_FUNC<uint32_t>(T_UINT32)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_UINT64 ,T_UINT64 ,0 }, new MAX_FUNC<uint64_t>(T_UINT64)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_INT8 ,T_INT8 ,0 }, new MAX_FUNC<int8_t>(T_INT8)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_UINT8 ,T_UINT8 ,0 }, new MAX_FUNC<uint8_t>(T_UINT8)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_INT16 ,T_INT16 ,0 }, new MAX_FUNC<uint16_t>(T_INT16)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_UINT16 ,T_UINT16 ,0 }, new MAX_FUNC<uint16_t>(T_UINT16)));

			maxFuncs->insert(std::pair<const char*, function*>({ T_FLOAT ,T_FLOAT ,0 }, new MAX_FUNC<float>(T_FLOAT)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_DOUBLE ,T_DOUBLE ,0 }, new MAX_FUNC<int64_t>(T_DOUBLE)));

			maxFuncs->insert(std::pair<const char*, function*>({ T_TIMESTAMP ,T_TIMESTAMP ,0 }, new MAX_FUNC<uint64_t>(T_TIMESTAMP)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_DATETIME ,T_DATETIME ,0 }, new MAX_FUNC<int64_t>(T_DATETIME)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_TIME ,T_TIME ,0 }, new MAX_FUNC<int64_t>(T_TIME)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_DATE ,T_DATE ,0 }, new MAX_FUNC<int32_t>(T_DATE)));
			maxFuncs->insert(std::pair<const char*, function*>({ T_YEAR ,T_YEAR ,0 }, new MAX_FUNC<int16_t>(T_YEAR)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("max", maxFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MAX", maxFuncs));
			/*min*/
			FUNC_ARGV_MAP* minFuncs = new  FUNC_ARGV_MAP();
			minFuncs->insert(std::pair<const char*, function*>({ T_INT32 ,T_INT32 ,0 }, new MIN_FUNC<int32_t>(T_INT32)));
			minFuncs->insert(std::pair<const char*, function*>({ T_INT64 ,T_INT64 ,0 }, new MIN_FUNC<int64_t>(T_INT64)));
			minFuncs->insert(std::pair<const char*, function*>({ T_UINT32 ,T_UINT32 ,0 }, new MIN_FUNC<uint32_t>(T_UINT32)));
			minFuncs->insert(std::pair<const char*, function*>({ T_UINT64 ,T_UINT64 ,0 }, new MIN_FUNC<uint64_t>(T_UINT64)));
			minFuncs->insert(std::pair<const char*, function*>({ T_INT8 ,T_INT8 ,0 }, new MIN_FUNC<int8_t>(T_INT8)));
			minFuncs->insert(std::pair<const char*, function*>({ T_UINT8 ,T_UINT8 ,0 }, new MIN_FUNC<uint8_t>(T_UINT8)));
			minFuncs->insert(std::pair<const char*, function*>({ T_INT16 ,T_INT16 ,0 }, new MIN_FUNC<uint16_t>(T_INT16)));
			minFuncs->insert(std::pair<const char*, function*>({ T_UINT16 ,T_UINT16 ,0 }, new MIN_FUNC<uint16_t>(T_UINT16)));

			minFuncs->insert(std::pair<const char*, function*>({ T_FLOAT ,T_FLOAT ,0 }, new MIN_FUNC<float>(T_FLOAT)));
			minFuncs->insert(std::pair<const char*, function*>({ T_DOUBLE ,T_DOUBLE ,0 }, new MIN_FUNC<int64_t>(T_DOUBLE)));

			minFuncs->insert(std::pair<const char*, function*>({ T_TIMESTAMP ,T_TIMESTAMP ,0 }, new MIN_FUNC<uint64_t>(T_TIMESTAMP)));
			minFuncs->insert(std::pair<const char*, function*>({ T_DATETIME ,T_DATETIME ,0 }, new MIN_FUNC<int64_t>(T_DATETIME)));
			minFuncs->insert(std::pair<const char*, function*>({ T_TIME ,T_TIME ,0 }, new MIN_FUNC<int64_t>(T_TIME)));
			minFuncs->insert(std::pair<const char*, function*>({ T_DATE ,T_DATE ,0 }, new MIN_FUNC<int32_t>(T_DATE)));
			minFuncs->insert(std::pair<const char*, function*>({ T_YEAR ,T_YEAR ,0 }, new MIN_FUNC<int16_t>(T_YEAR)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("min", minFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MIN", minFuncs));

			/*abs*/
			FUNC_ARGV_MAP* absFuncs = new  FUNC_ARGV_MAP();
			absFuncs->insert(std::pair<const char*, function*>({ T_INT32  ,0 }, new ABS_FUNC<int32_t>(T_INT32)));
			absFuncs->insert(std::pair<const char*, function*>({ T_INT64  ,0 }, new ABS_FUNC<int64_t>(T_INT64)));
			absFuncs->insert(std::pair<const char*, function*>({ T_UINT32  ,0 }, new ABS_FUNC<uint32_t>(T_UINT32)));
			absFuncs->insert(std::pair<const char*, function*>({ T_UINT64  ,0 }, new ABS_FUNC<uint64_t>(T_UINT64)));
			absFuncs->insert(std::pair<const char*, function*>({ T_INT8  ,0 }, new ABS_FUNC<int8_t>(T_INT8)));
			absFuncs->insert(std::pair<const char*, function*>({ T_UINT8  ,0 }, new ABS_FUNC<uint8_t>(T_UINT8)));
			absFuncs->insert(std::pair<const char*, function*>({ T_INT16  ,0 }, new ABS_FUNC<uint16_t>(T_INT16)));
			absFuncs->insert(std::pair<const char*, function*>({ T_UINT16  ,0 }, new ABS_FUNC<uint16_t>(T_UINT16)));

			absFuncs->insert(std::pair<const char*, function*>({ T_FLOAT  ,0 }, new ABS_FUNC<float>(T_FLOAT)));
			absFuncs->insert(std::pair<const char*, function*>({ T_DOUBLE ,0 }, new ABS_FUNC<int64_t>(T_DOUBLE)));

			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("abs", absFuncs));
			globalFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("ABS", absFuncs));


			/*-----------------------------------------------group-------------------------------------------------*/
			GROUP_FUNC_ARGV_MAP* groupMaxFunc = new GROUP_FUNC_ARGV_MAP();
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_INT32  ,0 }, new GROUP_MAX_FUNC<int32_t>(T_INT32)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_INT64  ,0 }, new GROUP_MAX_FUNC<int64_t>(T_INT64)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT32  ,0 }, new GROUP_MAX_FUNC<uint32_t>(T_UINT32)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT64  ,0 }, new GROUP_MAX_FUNC<uint64_t>(T_UINT64)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_INT8  ,0 }, new GROUP_MAX_FUNC<int8_t>(T_INT8)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT8  ,0 }, new GROUP_MAX_FUNC<uint8_t>(T_UINT8)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_INT16  ,0 }, new GROUP_MAX_FUNC<uint16_t>(T_INT16)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT16  ,0 }, new GROUP_MAX_FUNC<uint16_t>(T_UINT16)));

			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_FLOAT  ,0 }, new GROUP_MAX_FUNC<float>(T_FLOAT)));
			groupMaxFunc->insert(std::pair<const char*, groupFunction*>({ T_DOUBLE ,0 }, new GROUP_MAX_FUNC<int64_t>(T_DOUBLE)));

			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("max", groupMaxFunc));
			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MAX", groupMaxFunc));

			GROUP_FUNC_ARGV_MAP* groupMinFunc = new GROUP_FUNC_ARGV_MAP();
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_INT32  ,0 }, new GROUP_MIN_FUNC<int32_t>(T_INT32)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_INT64  ,0 }, new GROUP_MIN_FUNC<int64_t>(T_INT64)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT32  ,0 }, new GROUP_MIN_FUNC<uint32_t>(T_UINT32)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT64  ,0 }, new GROUP_MIN_FUNC<uint64_t>(T_UINT64)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_INT8  ,0 }, new GROUP_MIN_FUNC<int8_t>(T_INT8)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT8  ,0 }, new GROUP_MIN_FUNC<uint8_t>(T_UINT8)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_INT16  ,0 }, new GROUP_MIN_FUNC<uint16_t>(T_INT16)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_UINT16  ,0 }, new GROUP_MIN_FUNC<uint16_t>(T_UINT16)));

			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_FLOAT  ,0 }, new GROUP_MIN_FUNC<float>(T_FLOAT)));
			groupMinFunc->insert(std::pair<const char*, groupFunction*>({ T_DOUBLE ,0 }, new GROUP_MIN_FUNC<int64_t>(T_DOUBLE)));

			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("min", groupMinFunc));
			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("MIN", groupMinFunc));

			GROUP_FUNC_ARGV_MAP* groupCountFunc = new GROUP_FUNC_ARGV_MAP();
			groupCountFunc->insert(std::pair<const char*, groupFunction*>({ T_CURRENT_VERSION_MAX_TYPE  ,0 }, new GROUP_COUNT_FUNC<int32_t>(T_INT32)));
			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("count", groupCountFunc));
			globalGroupFuncMap.insert(std::pair<const char*, FUNC_ARGV_MAP*>("COUNT", groupCountFunc));
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
