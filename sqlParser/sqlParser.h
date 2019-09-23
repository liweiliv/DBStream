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
#include "util/unorderMapUtil.h"
#ifdef  OS_WIN
#include <wtypes.h>
#endif
#include "util/winDll.h"
class jsonValue;
class jsonString;
class jsonArray;
class jsonObject;
class SQLWord;
class SQLWordArray;
class SQLSingleWord;
namespace SQL_PARSER
{
	typedef std::unordered_map<const char*, SQL_TYPE, StrHash, StrCompare> SQL_TYPE_TREE;
	class SQLWord;
	class sqlParser
	{
	private:
		std::map<std::string, SQLWordArray*> m_parseTree;
		std::map<std::string, SQLWordArray*> m_parseTreeHead;
		SQL_TYPE_TREE m_sqlTypes;
#ifdef OS_WIN
		HINSTANCE m_funcsHandle;
#else
		void* m_funcsHandle;
#endif
		bool getLoopCondition(const jsonValue* loop, SQLWord*& condition);
		bool checkWords();
		parserFuncType getFunc(const jsonString* json);
		bool forwardDeclare(const jsonArray* value);
		SQLWord* getInclude(const jsonString* value, const std::string& topName, SQLWord* top);
		SQLWordArray* loadWordArrayFromJson (const jsonObject* json, const char* name, SQLWordArray* top);
		SQLSingleWord* loadSingleWordFromJson(const jsonObject* json);

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
	};
};
#endif /* SQLPARSER_H_ */
