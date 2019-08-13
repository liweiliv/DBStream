/*
 * sqlParserEngine.cpp
 *
 *  Created on: 2018年11月16日
 *      Author: liwei
 */

#include <stdio.h>
#include <fcntl.h>
#include "util/file.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#ifdef OS_LINUX
#include <dlfcn.h>
#endif
#include <map>
#include <set>
#include <list>
#include "util/stackLog.h"
#include "sqlWord.h"
#include "sqlParserUtil.h"
#include "sqlParser.h"
#include "glog/logging.h"
#include "util/json.h"
#ifdef OS_WIN
#include "util/winString.h"
#endif
using namespace std;
//#define DEBUG
namespace SQL_PARSER
{

	SQLWord* sqlParser::loadSQlWordFromJson(jsonValue* json, const std::string & name, SQLWord* top)
	{
		SQLWord* s = nullptr;
		jsonObject* obj = static_cast<jsonObject*>(json);
		jsonValue* value = obj->get("OPT");
		bool optional = false; //default  false
		if (value != nullptr)
		{
			if (value->t != jsonObject::J_BOOL)
			{
				LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			optional = static_cast<jsonBool*>(value)->m_value;
		}
		SQL_TYPE sqlType = UNSUPPORT;
		if ((value = static_cast<jsonObject*>(obj)->get("TYPE")) != NULL || (value = static_cast<jsonObject*>(obj)->get("type")) != NULL)
		{
			if (value->t != jsonObject::J_STRING)
			{
				LOG(ERROR) << "expect string type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			SQL_TYPE_TREE::const_iterator iter = m_sqlTypes.find(static_cast<jsonString*>(value)->m_value.c_str());
			if (iter != m_sqlTypes.end())
				sqlType = iter->second;
		}
		bool OR = false;
		value = obj->get("OR");
		if (value != nullptr)
		{
			if (value->t != jsonObject::J_BOOL)
			{
				LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			OR = static_cast<jsonBool*>(value)->m_value;
		}
		std::string loop;
		value = obj->get("LOOP");
		if (value != nullptr)
		{
			if (value->t != jsonObject::J_STRING)
			{
				LOG(ERROR) << "expect string type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			loop = static_cast<jsonString*>(value)->m_value;
		}
		string comment;
		if (nullptr != (value = obj->get("COMMENT")))
		{
			if (value->t != jsonObject::J_STRING)
			{
				LOG(ERROR) << "expect string type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			comment = static_cast<jsonString*>(value)->m_value;
		}
		if (nullptr != (value = obj->get("DECLARE")))
		{
			if (value->t != jsonObject::J_ARRAY)
			{
				LOG(ERROR) << "expect array type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			for (std::list<jsonValue*>::const_iterator iter = static_cast<jsonArray*>(value)->m_values.begin(); iter != static_cast<jsonArray*>(value)->m_values.end(); iter++)
			{
				if ((*iter)->t != jsonObject::J_STRING)
				{
					LOG(ERROR) << "expect string type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
				map<std::string, SQLWord*>::iterator wordIter = m_parseTree.find(static_cast<jsonString*>(*iter)->m_value);
				if (wordIter == m_parseTree.end())
				{
					SQLWordArray* w = new SQLWordArray(false, false, false, nullptr);
					w->m_forwardDeclare = true;
					m_parseTree.insert(std::pair<std::string, SQLWord*>(static_cast<jsonString*>(*iter)->m_value, w));
				}
			}
		}
		if (nullptr != (value = obj->get("INCLUDE")))
		{
			if (value->t != jsonObject::J_STRING)
			{
				LOG(ERROR) << "expect number type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if (static_cast<jsonString*>(value)->m_value == name)//recursive 
			{
				if (top == nullptr)
				{
					LOG(ERROR) << "recursive must after child declare in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
				return top;
			}
			map<std::string, SQLWord*>::iterator iter = m_parseTree.find(
				static_cast<jsonString*>(value)->m_value);
			if (iter == m_parseTree.end())
			{
				LOG(ERROR) << "can not find INCLUDE WORD: [" << static_cast<jsonString*>(value)->m_value << "] ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			iter->second->include(); //it will not be free by parent when destroy
			return iter->second;
		}
		
		if ((value = obj->get("K")) != nullptr)
		{
			if (value->t != jsonObject::J_STRING)
			{
				LOG(ERROR) << "expect number type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if ((s = SQLSingleWord::create(optional, static_cast<jsonString*>(value)->m_value)) == nullptr)
			{
				LOG(ERROR) << "create sql word by  [" << value->toString() << "] failed ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if ((value = obj->get("F")) != nullptr)
			{
				if (value->t != jsonObject::J_STRING)
				{
					delete s;
					LOG(ERROR) << "expect number type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
#ifdef OS_LINUX
				if (nullptr
					== (static_cast<SQLSingleWord*>(s)->m_parser =
					(parseValue(*)(handle*, const string&)) dlsym(
						m_funcsHandle,
						static_cast<jsonString*>(value)->m_value.c_str())))
#endif
#ifdef OS_WIN
				if (nullptr
					== (static_cast<SQLSingleWord*>(s)->m_parser =
					(parseValue(*)(handle*, const string&)) GetProcAddress(
						m_funcsHandle,
						static_cast<jsonString*>(value)->m_value.c_str())))
#endif
				{
					LOG(ERROR)<<"can not get fun:"<< static_cast<jsonString*>(value)->m_value<<" in funcs,occurred in [" << json->toString() << "]";
					delete s;
					return nullptr;
				}
			}
		}
		else if ((value = obj->get("C")) != nullptr)
		{
			if (value->t != jsonObject::J_ARRAY)
			{
				LOG(ERROR) << "expect array type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if (top == nullptr)
			{
				map<std::string, SQLWord*>::iterator miter = m_parseTree.find(name);
				if (miter != m_parseTree.end())
					s = miter->second;
			}
			if (s != nullptr)
			{
				s->m_optional = optional;
				static_cast<SQLWordArray*>(s)->m_or = optional;
			}
			if (!loop.empty())
			{
				if (loop == "always")
				{
					if (s != nullptr)
						static_cast<SQLWordArray*>(s)->m_loop = true;
					else
						s = new SQLWordArray(optional, OR, true, nullptr);
				}
				else
				{
					SQLSingleWord* loopCondition = SQLSingleWord::create(false, loop);
					if (loopCondition == nullptr)
					{
						if (s != nullptr)
							delete s;
						LOG(ERROR)<<"invalid loop condition:"<<loop<<",occurred in [" << json->toString() << "]";
						return nullptr;
					}
					if (s != nullptr)
					{
						static_cast<SQLWordArray*>(s)->m_loop = true;
						static_cast<SQLWordArray*>(s)->m_loopCondition = loopCondition;
					}
					else
						s = new SQLWordArray(optional, OR, true, loopCondition);
				}
			}
			else
			{
				if(s == nullptr)
					s = new SQLWordArray(optional, OR, false,nullptr);
			}
			if (top == nullptr)
				top = s;

			for (list<jsonValue*>::iterator iter =
				static_cast<jsonArray*>(value)->m_values.begin();
				iter != static_cast<jsonArray*>(value)->m_values.end(); iter++)
			{
				if ((*iter)->t != jsonObject::J_OBJECT)
				{
					delete s;
					LOG(ERROR) << "expect object type in [" << (*iter)->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
				SQLWord* child = loadSQlWordFromJson(*iter,name,top);
				if (child == nullptr)
				{
					delete s;
					LOG(ERROR) << "parse child parse tree failed in [" << (*iter)->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
				static_cast<SQLWordArray*>(s)->append(child);
			}
		}
		if (s)
		{
			s->m_comment = json->toString();
			s->m_sqlType = sqlType;
		}
		return s;
	}
	DLL_EXPORT sqlParser::sqlParser() :
		m_funcsHandle(nullptr), m_initUserDataFunc(nullptr), m_destroyUserDataFunc(
			nullptr)
	{
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("BEGIN", BEGIN));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("COMMIT", COMMIT));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ROLLBACK", ROLLBACK));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("INSERT_INTO", INSERT_INTO));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("DELETE_FROM", DELETE_FROM));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("UPDATE_SET", UPDATE_SET));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("REPLACE", REPLACE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("SELECT", SELECT));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("USE_DATABASE", USE_DATABASE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("CREATE_DATABASE", CREATE_DATABASE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("DROP_DATABASE", DROP_DATABASE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_DATABASE", ALTER_DATABASE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("CREATE_TABLE", CREATE_TABLE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("CREATE_TABLE_LIKE", CREATE_TABLE_LIKE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("RENAME_TABLE", RENAME_TABLE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("DROP_TABLE", DROP_TABLE));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("CREATE_INDEX", CREATE_INDEX));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("DROP_INDEX", DROP_INDEX));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_DROP_FOREIGN_KEY", ALTER_TABLE_DROP_FOREIGN_KEY));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_DROP_PRIMARY_KEY", ALTER_TABLE_DROP_PRIMARY_KEY));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_DROP_INDEX", ALTER_TABLE_DROP_INDEX));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_DROP_COLUMN", ALTER_TABLE_DROP_COLUMN));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_ADD_KEY", ALTER_TABLE_ADD_KEY));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_ADD_CONSTRAINT", ALTER_TABLE_ADD_CONSTRAINT));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_ADD_COLUMN", ALTER_TABLE_ADD_COLUMN));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_ADD_COLUMNS", ALTER_TABLE_ADD_COLUMNS));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_CHANGE_COLUMN", ALTER_TABLE_CHANGE_COLUMN));
		m_sqlTypes.insert(std::pair<const char*, SQL_TYPE>("ALTER_TABLE_MODIFY_COLUMN", ALTER_TABLE_MODIFY_COLUMN));
	}
	DLL_EXPORT sqlParser::~sqlParser()
	{
#ifdef OS_LINUX
		if (m_funcsHandle)
			dlclose(m_funcsHandle);
#endif
#ifdef OS_WIN
		if (m_funcsHandle)
			FreeLibrary(m_funcsHandle);
#endif
		for (map<std::string, SQLWord*>::iterator iter = m_parseTree.begin();
			iter != m_parseTree.end(); iter++)
		{
			if (iter->second->deInclude())
				delete iter->second;
		}
	}
	bool fileExist(const char* file)
	{
		fileHandle fd = openFile(file, true, false, false);
		if (fd > 0)
		{
			closeFile(fd);
			return true;
		}
		return false;
	}
#ifdef OS_LINUX
	int sqlParser::LoadFuncs(const char* fileName)
	{
		if (!fileExist(fileName))
			return -1;
		string soFile;
		string compileCmd;
		const char* end = fileName + strlen(fileName);
		while (*end != '.' && end != fileName)
			end--;
		if (end == fileName)
			end = fileName + strlen(fileName);
		if (strcmp(end, ".so") == 0)
		{
			soFile = fileName;
			goto LOAD;
		}
		else 	if (strcmp(end, ".cpp") == 0 || strcmp(end, ".cc") == 0)
		{
			const char* baseName = basename(fileName);
			soFile = string(fileName, baseName - fileName).append("lib").append(baseName, end - baseName).append(".so");
			if (!fileExist(soFile.c_str()))
				goto COMPILE;
			if (getFileTime(fileName) >= getFileTime(soFile.c_str()))
				remove(soFile.c_str());
			else
				goto LOAD;
		}
		else
			return -1;
	COMPILE:
		compileCmd.assign("g++ -shared -g -fPIC -Wall -o ");
		compileCmd.append(soFile).append(" ").append(fileName);
		if (system(compileCmd.c_str()) != 0)
			return -2;
	LOAD:
		if (m_funcsHandle != NULL)
			dlclose(m_funcsHandle);
		m_funcsHandle = dlopen(soFile.c_str(), RTLD_NOW);
		if (m_funcsHandle == NULL)
		{
			fprintf(stderr, "load %s failed for %s\n", soFile.c_str(), dlerror());
			return -3;
		}
		m_initUserDataFunc = (void(*)(handle*)) dlsym(m_funcsHandle, "createUserData");
		m_destroyUserDataFunc = (void(*)(handle*)) dlsym(m_funcsHandle, "destroyUserData");
		return 0;
	}
#endif
#ifdef OS_WIN
	DLL_EXPORT int sqlParser::LoadFuncs(const char* fileName)
	{
		if (m_funcsHandle != NULL)
			FreeLibrary(m_funcsHandle);
		m_funcsHandle = LoadLibraryEx(fileName,0, LOAD_WITH_ALTERED_SEARCH_PATH);
		if (m_funcsHandle == NULL)
		{
			LOG(ERROR)<<"load "<< fileName <<" failed for "<< GetLastError()<<","<<strerror(GetLastError());
			return -3;
		}
		m_initUserDataFunc = (void(*)(handle*)) GetProcAddress(m_funcsHandle, "createUserData");
		if (nullptr == m_initUserDataFunc)
		{
			LOG(ERROR) << "load func: createUserData from " << fileName << " failed for " << GetLastError() << "," << strerror(GetLastError());
			FreeLibrary(m_funcsHandle);
			m_funcsHandle = nullptr;
			return -3;
		}
		m_destroyUserDataFunc = (void(*)(handle*)) GetProcAddress(m_funcsHandle, "destroyUserData");
		if (nullptr == m_destroyUserDataFunc)
		{
			LOG(ERROR) << "load func: destroyUserData from " << fileName << " failed for " << GetLastError() << "," << strerror(GetLastError());
			FreeLibrary(m_funcsHandle);
			m_funcsHandle = nullptr;
			return -3;
		}
		return 0;
	}
#endif
	bool sqlParser::checkWords()
	{
		bool ok = true;
		for (std::map<std::string, SQLWord*>::const_iterator iter = m_parseTree.begin(); iter != m_parseTree.end(); iter++)
		{
			if (iter->second->m_forwardDeclare)
			{
				if (iter->second->m_refs > 0 )
				{
					LOG(ERROR) << "word :" << iter->first << " has been forward declare ,and used by other words,but can not find implementation code";
					ok = false;
				}
				else if (m_parseTreeHead.find(iter->first) != m_parseTreeHead.end())
				{
					LOG(ERROR) << "word :" << iter->first << " has been forward declare ,and used as head,but can not find implementation code";
					ok = false;
				}
				else
				{
					LOG(WARNING) << "word :" << iter->first << " has been forward declare ,but can not find implementation code";
				}
			}
		}
		return ok;
	}
	DLL_EXPORT int sqlParser::LoadParseTree(const char* config)
	{
		int32_t size = 0;
		jsonValue* segment = NULL;
		const char* p = nextWord(config);
		while (true)
		{
			if (NULL == (segment = jsonValue::Parse(p, size)))
			{
				LOG(ERROR) << "load parse tree from " << p << " failed";
				return -1;
			}
			if (segment->t != jsonValue::J_OBJECT)
			{
				LOG(ERROR) << "expect object type in [" << segment->toString() << "]";
				delete segment;
				return -1;
			}
			for (std::list<jsonObject::objectKeyValuePair>::const_iterator iter = static_cast<jsonObject*>(segment)->m_valueList.begin(); iter != static_cast<jsonObject*>(segment)->m_valueList.end(); iter++)
			{
				jsonValue* sentence = (*iter).value;
				jsonValue* value;
				bool head = false;
				if ((value = static_cast<jsonObject*>(sentence)->get("HEAD")) != nullptr)
				{
					if (value->t != jsonObject::J_BOOL)
					{
						delete segment;
						LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << sentence->toString() << "]";
						return -1;
					}
					head = static_cast<jsonBool*>(value)->m_value;
				}

				SQLWord* s = loadSQlWordFromJson(sentence, (*iter).key);
				if (s == NULL)
				{
					delete segment;
					LOG(ERROR) << "load parse tree failed";
				}
				s->include();
				if (s->m_forwardDeclare)
					s->m_forwardDeclare = false;
				else
				{
					pair<map<std::string, SQLWord*>::iterator, bool> i = m_parseTree.insert(
						pair<std::string, SQLWord*>(iter->key, s));
					if (!i.second)
					{
						LOG(ERROR) << sentence->toString()<<" has the same name "<< (*iter).key<<" with "<< i.first->second->m_comment;
						delete segment;
						return -1;
					}
				}
				if (head)
					m_parseTreeHead.insert(pair<std::string, SQLWord*>(iter->key, s));
			}
			delete segment;
			p = nextWord(p + size);
			if (p[0] == '\0')
				break;
		}
		if (!checkWords())
			return -1;
		return 0;
	}
	DLL_EXPORT int sqlParser::LoadParseTreeFromFile(const char* file)
	{
		fileHandle fd = openFile(file, true, false, false);
		if (!fileHandleValid(fd))
		{
			LOG(ERROR)<<"open parse tree file:"<<file<<" failed for "<< errno<<","<<strerror(errno);
			return -1;
		}
		long size = seekFile(fd, 0, SEEK_END);
		seekFile(fd, 0, SEEK_SET);
		char* buf = (char*)malloc(size + 1);
		if (size != readFile(fd, buf, size))
		{
			closeFile(fd);
			free(buf);
			return -1;
		}
		buf[size] = '\0';
		int ret = LoadParseTree(buf);
		closeFile(fd);
		free(buf);
		return ret;
	}
	char* preProcessSql(const char* sql, uint32_t version)
	{
		char* newSql = (char*)malloc(strlen(sql) + 1);
		const char* src = jumpOverSpace(sql);
		char* dest = newSql;
		while (*src != '\0')
		{
			src = jumpOverSpace(src);
			const char* end;
			if (strncmp(src, "/*", 2) == 0) //[/*!410000 DEFAULT CHARSET UTF8*/]
			{
				const char* endOfComment = src;
				if (false == jumpOverComment(endOfComment))
				{
					free(newSql);
					return NULL;
				}
				if (*(src + 2) == '!')
				{
					end = jumpOverSpace(src + 3);
					uint32_t sqlVersion = atol(end);
					if (sqlVersion <= version)
					{
						src = jumpOverSpace(realEndOfWord(end));
						memcpy(dest, src, endOfComment - 2 - src);
						dest += endOfComment - 2 - src;
						*dest = ' ';
						dest++;
					}
				}
				src = endOfComment;
			}
			else
			{
				const char* end = realEndOfWord(src);
				memcpy(dest, src, end - src);
				dest += end - src;
				*dest = ' ';
				dest++;
				src = end;
			}
		}
		*(dest - 1) = '\0';
		return newSql;
	}
	DLL_EXPORT parseValue sqlParser::parseSqlType(handle*& h, const char* sql)
	{
		h = new handle;
		handle* currentHandle = h;
		while (true)
		{
			for (map<std::string, SQLWord*>::iterator iter = m_parseTreeHead.begin();
				iter != m_parseTreeHead.end(); iter++)
			{
				const char* tmp = sql;
				SQLWord* s = static_cast<SQLWord*>(iter->second);
				if (s->match(currentHandle, tmp) != OK)
					continue;
				sql = nextWord(tmp);
				goto PARSE_SUCCESS;
			}
			/*not match after compare to all SQLWords in m_parseTreeHead,return*/
			delete h;
			h = nullptr;
			return NOT_MATCH;
PARSE_SUCCESS:
			while (sql[0] == ';')
				sql = nextWord(sql + 1);
			if (sql[0] == '\0') //sql has finished ,return
				return OK;
			/*sql do not finish,go on prase */
			handle* _h = new handle;
			currentHandle->next = _h;
			currentHandle = _h;
		}
	}
	DLL_EXPORT parseValue sqlParser::parse(handle*& h,const char * database, const char* sql)
	{
		h = new handle;
		if (database != nullptr)
			h->dbName = database;
		handle* currentHandle = h;
		while (true)
		{
			for (map<std::string, SQLWord*>::iterator iter = m_parseTreeHead.begin();
				iter != m_parseTreeHead.end(); iter++)
			{
				const char* tmp = sql;
				SQLWord* s = static_cast<SQLWord*>(iter->second);
				if (s->match(currentHandle, tmp) != OK)
				{
					continue;
				}
				else
				{
					sql = nextWord(tmp);
					goto PARSE_SUCCESS;
				}
			}
			/*not match after compare to all SQLWords in m_parseTreeHead,return*/
			delete h;
			h = nullptr;
			return NOT_MATCH;
PARSE_SUCCESS:
			if (m_initUserDataFunc)
				m_initUserDataFunc(h);
			if (m_destroyUserDataFunc)
				h->destroyUserDataFunc = m_initUserDataFunc;
			statusInfo* s = currentHandle->head;
			while (s)
			{
				if (s->parserFunc&& OK != s->parserFunc(currentHandle, s->sql))
				{
					delete h;
					return NOT_MATCH;
				}
				s = s->next;
			}
			while (sql[0] == ';')
				sql = nextWord(sql + 1);
			if (sql[0] == '\0') //sql has finished ,return
				return OK;

			/*sql do not finish,go on prase */
			handle* _h = new handle;
			if (!currentHandle->dbName.empty())
				h->dbName = currentHandle->dbName;
			currentHandle->next = _h;
			currentHandle = _h;
		}
	}
}
;
#ifdef TEST
int main(int argc, char* argv[])
{
	SQL_PARSER::sqlParser p;
	initStackLog();
	initKeyWords();
	if (0 != p.LoadFuncs("sqlParserFuncs.cpp"))
	{
		printf("load funcs failed\n");
	}
	if (0 != p.LoadParseTreeFromFile("ParseTree"))
	{
		printf("load parse tree failed\n");
	}
	char* sql = NULL;
	if (argc == 1)
		sql = (char*) "rename table `mp_time_limit20` to `mp_time_limit_20`";
	else
	{
		fileHandle fd = openFile(argv[1], true, false, false);
		if (fd < 0)
			return -1;
		uint32_t size = seekFile(fd, 0, SEEK_END);
		seekFile(fd, 0, SEEK_SET);
		sql = (char*)malloc(size + 1);
		if (size != readFile(fd, sql, size))
		{
			closeFile(fd);
			free(sql);
			return -1;
		}
		sql[size] = '\0';
	}
#if 0
	sqlParser::handle * h;
	p.parse(h, sql);
	string s;
	getFullStackLog(s);
	printf("%s\n", s.c_str());
	if (argc > 1)
		free(sql);
	destroyStackLog();
	destroyKeyWords();
#endif
	char* newSql = SQL_PARSER::preProcessSql(sql, 0xfffffffful);
	printf("%s\n", newSql);

}
#endif

