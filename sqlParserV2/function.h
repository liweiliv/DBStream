#pragma once
#include "util/sparsepp/spp.h"
#include "str.h"
#include "literalTranslate.h"
#include "sqlHandle.h"
namespace SQL_PARSER {
	typedef DS (*sqlFunc) (const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle);
	struct func {
		inline bool operator()(const func& s, const func& d) const
		{
			if (s.l != d.l)
				return false;
			return memcmp(s.data, d.data, s.length - 1) == 0;
		}
		inline uint32_t operator()(const func& s) const
		{
			uint32_t hash = 1315423911;
			const char* p = s.name(), * e = &s.data[length - 1];
			while (p != e)
			{
				hash ^= ((hash << 5) + (*p++) + (hash >> 2));
			}
			return (hash & 0x7FFFFFFF);
		}
		sqlFunc funcPoint;
		union 
		{
			struct {
				uint16_t length;
				uint8_t nameLength;
				uint8_t argvCount;
			};
			uint32_t l;
		};
		char data[512];
		inline const char* name() const
		{
			return data;
		}
		inline const char* argvTypeList() const
		{
			return &data[nameLength];
		}
		inline literalType returnType() const 
		{
			return &data[length];
		}
	};
	class funcNameCompare()
	{
		inline bool operator()(const func & s, const func & d) const
		{
			if (s.l != d.l)
				return false;
			return memcmp(s.data, d.data, s.nameLength) == 0;
		}
		inline uint32_t operator()(const func & s) const
		{
			uint32_t hash = 1315423911;
			const char* p = s.name();
			while (*p != 0)
			{
				hash ^= ((hash << 5) + (*p++) + (hash >> 2));
			}
			return (hash & 0x7FFFFFFF);
		}
	};
	typedef spp::sparse_hash_set<const func, func, func> FUNC_SET;
	typedef spp::sparse_hash_set<const func, funcNameCompare, funcNameCompare> FUNC_FUZZY_SET;
	class  function {
	private:
		FUNC_SET m_func;
		FUNC_FUZZY_SET m_funcFuzzy;
	public:
		virtual DS init() = 0;
		const func* getFunc(sqlHandle* handle, const func& f)
		{
			FUNC_SET::const_iterator iter = m_func.find(f);
			if (iter != m_func.end())
			{
				return &(*iter);
			}
			else
			{
				FUNC_FUZZY_SET::const_iterator fiter = m_funcFuzzy.find(f);
				if (fiter == m_funcFuzzy.end())
					return nullptr;
				const func& matchedFunc = *fiter;
				for (uint8_t i = 0; i < f.argvCount; i++)
				{
					if (f.argvTypeList()[i] != matchedFunc.argvTypeList()[i])
					{
						if (!handle->literalTrans->canTrans(f.argvTypeList()[i], matchedFunc.argvTypeList()[i]))
							return nullptr;
					}
				}
				return &matchedFunc;
			}
		}
	};
}