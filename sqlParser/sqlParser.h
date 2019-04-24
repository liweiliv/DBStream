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
#ifdef  OS_WIN
#include <wtypes.h>
#endif //  OS_WIN

class jsonValue;
namespace SQL_PARSER
{
class SQLWord;
class sqlParser
{
private:
    std::map<uint32_t, SQLWord *> m_parseTree;
    std::map<uint32_t, SQLWord *> m_parseTreeHead;
#ifdef OS_WIN
	HINSTANCE m_funcsHandle;
#else
    void * m_funcsHandle;
#endif
    SQLWord* loadSQlWordFromJson(jsonValue *json);
	void (*m_initUserDataFunc)(handle *h);
	void (*m_destroyUserDataFunc)(handle *h);
public:
    sqlParser();
    ~sqlParser();
    int LoadFuncs(const char * fileName);
    int LoadParseTree(const char *config);
    int LoadParseTreeFromFile(const char * file);
public:
    parseValue parse(handle *&h, const char * sql);
};
};
#endif /* SQLPARSER_H_ */
