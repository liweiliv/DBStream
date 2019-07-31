/*
 * sqlParser.h
 *
 *  Created on: 2018年11月21日
 *      Author: liwei
 */

#ifndef SQLPARSER_H_
#define SQLPARSER_H_
#include <string>
#include <map>
#include "sqlParserHandle.h"
#include "../util/unorderMapUtil.h"
#ifdef  OS_WIN
#include <wtypes.h>
#endif
#include "..//util/winDll.h"
class jsonValue;
namespace SQL_PARSER
{
	typedef std::unordered_map<const char*, SQL_TYPE, StrHash, StrCompare> SQL_TYPE_TREE;
	class SQLWord;
	class sqlParser
	{
	private:
		std::map<uint32_t, SQLWord*> m_parseTree;
		std::map<uint32_t, SQLWord*> m_parseTreeHead;
		SQL_TYPE_TREE m_sqlTypes;
#ifdef OS_WIN
		HINSTANCE m_funcsHandle;
#else
		void* m_funcsHandle;
#endif
		SQLWord* loadSQlWordFromJson(jsonValue* json, uint32_t id, SQLWord* top = nullptr);
		void (*m_initUserDataFunc)(handle* h);
		void (*m_destroyUserDataFunc)(handle* h);
	public:
		DLL_EXPORT sqlParser();
		DLL_EXPORT ~sqlParser();
		DLL_EXPORT int LoadFuncs(const char* fileName);
		DLL_EXPORT int LoadParseTree(const char* config);
		DLL_EXPORT int LoadParseTreeFromFile(const char* file);
	public:
		DLL_EXPORT parseValue parse(handle*& h, const char * database,const char* sql);
		DLL_EXPORT parseValue parseSqlType(handle*& h, const char* sql);
	};
};
#endif /* SQLPARSER_H_ */
