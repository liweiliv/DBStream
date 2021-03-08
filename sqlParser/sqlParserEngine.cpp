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
	bool sqlParser::getLoopCondition(const jsonValue* loop, SQLWord*& condition)
	{
		if (loop->t == JSON_TYPE::J_STRING)
		{
			if (strncasecmp(static_cast<const jsonString*>(loop)->m_value.c_str(), "always", 7) == 0)
			{
				condition = nullptr;
				return true;
			}
			else
			{
				condition = SQLSingleWord::create(false, static_cast<const jsonString*>(loop)->m_value, m_quote);
				if (condition != nullptr)
					return true;
				else
					return false;
			}
		}
		else if (loop->t == JSON_TYPE::J_OBJECT)
		{
			condition = loadWordArrayFromJson(static_cast<const jsonObject*>(loop), nullptr, nullptr);
			if (condition != nullptr)
				return true;
			else
				return false;
		}
		else
			return false;
	}
	parserFuncType sqlParser::getFunc(const jsonString* json)
	{
		parserFuncType func = nullptr;
#ifdef OS_LINUX
		if (nullptr
			== (func =
			(parserFuncType)dlsym(
				m_funcsHandle,
				json->m_value.c_str())))
#endif
#ifdef OS_WIN
			if (nullptr
				== (func = (parserFuncType)GetProcAddress(
					m_funcsHandle, json->m_value.c_str())))
#endif
			{
				LOG(ERROR) << "can not get func:" << json->m_value << " in funcs,occurred in [" << json->toString() << "]";
				return nullptr;
			}
		return func;
	}
	bool sqlParser::forwardDeclare(const jsonArray* value)
	{
		for (std::list<jsonValue*>::const_iterator iter = value->m_values.begin(); iter != value->m_values.end(); iter++)
		{
			if ((*iter)->t != JSON_TYPE::J_STRING)
			{
				LOG(ERROR) << "expect string type in [" << value->toString() << "]";
				return false;
			}
			std::map<std::string, SQLWordArray*>::iterator wordIter = m_parseTree.find(static_cast<jsonString*>(*iter)->m_value);
			if (wordIter == m_parseTree.end())
			{
				SQLWordArray* w = new SQLWordArray(false, false, false, nullptr);
				w->m_forwardDeclare = true;
				m_parseTree.insert(std::pair<std::string, SQLWordArray*>(static_cast<jsonString*>(*iter)->m_value, w));
			}
		}
		return true;
	}
	SQLWord* sqlParser::getInclude(const jsonString* value, const std::string& topName, SQLWord* top)
	{
		if (value->m_value == topName)//recursive 
		{
			if (top == nullptr)
			{
				LOG(ERROR) << "recursive must after child declare in [" << value->toString() << "]";
				return nullptr;
			}
			return top;
		}
		std::map<std::string, SQLWordArray*>::iterator iter = m_parseTree.find(
			static_cast<const jsonString*>(value)->m_value);
		if (iter == m_parseTree.end())
		{
			LOG(ERROR) << "can not find INCLUDE WORD: [" << static_cast<const jsonString*>(value)->m_value << "]";
			return nullptr;
		}
		iter->second->include(); //it will not be free by parent when destroy
		return iter->second;
	}
	SQLSingleWord* sqlParser::loadSingleWordFromJson(const jsonObject* json)
	{
		const jsonValue* value = json->get("OPT");
		bool optional = false; //default  false
		parserFuncType func = nullptr;
		if (value != nullptr)
		{
			if (value->t != JSON_TYPE::J_BOOL)
			{
				LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			optional = static_cast<const jsonBool*>(value)->m_value;
		}
		if (nullptr != (value = json->get("F")))
		{
			if (value->t != JSON_TYPE::J_STRING)
			{
				LOG(ERROR) << "function type expect string type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if (nullptr == (func = static_cast<parserFuncType>(getFunc(static_cast<const jsonString*>(value)))))
			{
				LOG(ERROR) << "can not fund func: [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
		}
		value = json->get("K");
		if (value == nullptr || value->t != JSON_TYPE::J_STRING)
		{
			LOG(ERROR) << "expect key value pair :\"K\":\"type info\" by string type in [" << json->toString() << "]";
			return nullptr;
		}
		SQLSingleWord* v = SQLSingleWord::create(optional, static_cast<const jsonString*>(value)->m_value, m_quote);
		if (v == nullptr)
		{
			LOG(ERROR) << "create SQLSingleWord failed in [" << json->toString() << "]";
			return nullptr;
		}
		v->m_parser = func;
		v->m_comment = json->toString();
		return v;
	}
	SQLWordArray* sqlParser::loadWordArrayFromJson(const jsonObject* json, const char* name, SQLWordArray* top)
	{
		const jsonValue* value;
		bool optional = false; //default  false
		bool OR = false;
		bool loop = false;
		SQLWord* loopCondition = nullptr;
		SQLWordArray* array = nullptr;
		if ((value = json->get("OPT")) != nullptr)
		{
			if (value->t != JSON_TYPE::J_BOOL)
			{
				LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			optional = static_cast<const jsonBool*>(value)->m_value;
		}
		if ((value = json->get("OR")) != nullptr)
		{
			if (value->t != JSON_TYPE::J_BOOL)
			{
				LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			OR = static_cast<const jsonBool*>(value)->m_value;
		}
		if (nullptr != (value = json->get("DECLARE")))
		{
			if (value->t != JSON_TYPE::J_ARRAY)
			{
				LOG(ERROR) << "expect array type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			if (!forwardDeclare(static_cast<const jsonArray*>(value)))
			{
				LOG(ERROR) << "forwardDeclare failed in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
		}
		if ((value = json->get("LOOP")) != nullptr)
		{
			if (!getLoopCondition(value, loopCondition))
			{
				LOG(ERROR) << "load loop condition in [" << value->toString() << "] failed" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			loop = true;
		}
		if ((value = json->get("C")) == nullptr)
		{
			LOG(ERROR) << "expect \"C\":[child info] in [" << json->toString() << "]" << " ,occurred in [" << json->toString() << "]";
			if (loopCondition)
				delete loopCondition;
			return nullptr;
		}

		if (value->t != JSON_TYPE::J_ARRAY)
		{
			LOG(ERROR) << "expect array type in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
			return nullptr;
		}

		if (top == nullptr)
		{
			map<std::string, SQLWordArray*>::iterator miter = m_parseTree.find(name);
			if (miter != m_parseTree.end())//has been forward declare,fill value
			{
				array = static_cast<SQLWordArray*>(miter->second);
				if (!array->m_forwardDeclare)
				{
					LOG(ERROR) << "redefine " << name << " in [" << value->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					return nullptr;
				}
				array->m_optional = optional;
				array->m_or = OR;
				array->m_loop = loop;
				array->m_loopCondition = loopCondition;
				array->m_forwardDeclare = false;
			}
		}

		if (array == nullptr)
		{
			array = new SQLWordArray(optional, OR, loop, loopCondition);
		}
		if (top == nullptr)
			top = array;
		for (list<jsonValue*>::const_iterator iter =
			static_cast<const jsonArray*>(value)->m_values.begin();
			iter != static_cast<const jsonArray*>(value)->m_values.end(); iter++)
		{
			if ((*iter)->t != JSON_TYPE::J_OBJECT)
			{
				delete array;
				LOG(ERROR) << "expect object type in [" << (*iter)->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			SQLWord* child = nullptr;
			const jsonValue* inc;
			if (static_cast<jsonObject*>(*iter)->get("K") != nullptr)
			{
				child = loadSingleWordFromJson(static_cast<jsonObject*>(*iter));
			}
			else if (static_cast<jsonObject*>(*iter)->get("C") != nullptr)
			{
				child = loadWordArrayFromJson(static_cast<jsonObject*>(*iter), name, top);
			}
			else if ((inc = static_cast<jsonObject*>(*iter)->get("INCLUDE")) != nullptr)
			{
				if (inc->t != JSON_TYPE::J_STRING)
				{
					LOG(ERROR) << "expect string type in INCLUDE : [" << (*iter)->toString() << "]" << " ,occurred in [" << json->toString() << "]";
					delete array;
					return nullptr;
				}
				child = getInclude(static_cast<const jsonString*>(inc), name, top);
			}
			if (child == nullptr)
			{
				delete array;
				LOG(ERROR) << "parse child parse tree failed in [" << (*iter)->toString() << "]" << " ,occurred in [" << json->toString() << "]";
				return nullptr;
			}
			array->append(child);
		}
		array->m_comment = json->toString();
		return array;
	}
	DLL_EXPORT sqlParser::sqlParser() :
		m_funcsHandle(nullptr), m_quote(0), m_initUserDataFunc(nullptr), m_destroyUserDataFunc(
			nullptr)
	{
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
		for (map<std::string, SQLWordArray*>::iterator iter = m_parseTree.begin();
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
		LOG(INFO) << "sqlParser load func success";
		return 0;
	}
#endif
#ifdef OS_WIN
	DLL_EXPORT int sqlParser::LoadFuncs(const char* fileName)
	{
		if (m_funcsHandle != NULL)
			FreeLibrary(m_funcsHandle);
		m_funcsHandle = LoadLibraryEx(fileName, 0, LOAD_WITH_ALTERED_SEARCH_PATH);
		if (m_funcsHandle == NULL)
		{
			LOG(ERROR) << "load " << fileName << " failed for " << GetLastError() << "," << strerror(GetLastError());
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
		LOG(INFO) << "sqlParser load func success";
		return 0;
	}
#endif
	bool sqlParser::checkWords()
	{
		bool ok = true;
		for (std::map<std::string, SQLWordArray*>::const_iterator iter = m_parseTree.begin(); iter != m_parseTree.end(); iter++)
		{
			if (iter->second->m_forwardDeclare)
			{
				if (iter->second->m_refs > 0)
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
			if (!dsCheck(jsonValue::parse(segment, p, size)))
			{
				LOG(ERROR) << "load parse tree from " << p << " failed for json string is parse failed";
				return -1;
			}
			if (segment->t != JSON_TYPE::J_OBJECT)
			{
				LOG(ERROR) << "expect object type in [" << segment->toString() << "]";
				delete segment;
				return -1;
			}
			for (std::list<std::pair<std::string, jsonValue*> >::const_iterator iter = static_cast<jsonObject*>(segment)->m_valueList.begin(); iter != static_cast<jsonObject*>(segment)->m_valueList.end(); iter++)
			{
				if ((*iter).second->t != JSON_TYPE::J_OBJECT)
				{
					LOG(ERROR) << "expect object type in [" << (*iter).second->toString() << "]";
					delete segment;
					return -1;
				}
				jsonObject* sentence = static_cast<jsonObject*>((*iter).second);
				const jsonValue* value;
				bool head = false;

				if ((value = static_cast<jsonObject*>(sentence)->get("C")) == nullptr || value->t != JSON_TYPE::J_ARRAY)
				{
					delete segment;
					LOG(ERROR) << "expect \"C\":[child info ] as array type in [" << static_cast<jsonObject*>(sentence)->toString() << "]" << " ,occurred in [" << sentence->toString() << "]";
					return -1;
				}
				if ((value = static_cast<jsonObject*>(sentence)->get("HEAD")) != nullptr)
				{
					if (value->t != JSON_TYPE::J_BOOL)
					{
						delete segment;
						LOG(ERROR) << "expect bool type in [" << value->toString() << "]" << " ,occurred in [" << sentence->toString() << "]";
						return -1;
					}
					head = static_cast<const jsonBool*>(value)->m_value;
				}

				SQLWordArray* s = loadWordArrayFromJson(sentence, (*iter).first.c_str(), nullptr);
				if (s == nullptr)
				{
					delete segment;
					LOG(ERROR) << "load parse tree failed";
					return -1;
				}
				s->include();
				m_parseTree.insert(pair<std::string, SQLWordArray*>((*iter).first, s));
				if (head)
					m_parseTreeHead.insert(pair<std::string, SQLWordArray*>((*iter).first, s));
			}
			delete segment;
			p = nextWord(p + size);
			if (p[0] == '\0')
				break;
		}
		if (!checkWords())
			return -1;
		LOG(INFO) << "load parser tree success";
		return 0;
	}
	DLL_EXPORT void sqlParser::setNameQuote(char quote)
	{
		m_quote = quote;
	}
	DLL_EXPORT int sqlParser::LoadParseTreeFromFile(const char* file)
	{
		fileHandle fd = openFile(file, true, false, false);
		if (!fileHandleValid(fd))
		{
			LOG(ERROR) << "open parse tree file:" << file << " failed for " << errno << "," << strerror(errno);
			return -1;
		}
		long size = seekFile(fd, 0, SEEK_END);
		seekFile(fd, 0, SEEK_SET);
		char* buf = (char*)malloc(size + 1);
		if (buf == nullptr)
			return -1;
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
	DLL_EXPORT parseValue sqlParser::parse(handle*& h, const char* database, const char* sql)
	{
		h = new handle;
		if (database != nullptr)
			h->dbName = database;
		if (m_destroyUserDataFunc)
			h->destroyUserDataFunc = m_destroyUserDataFunc;
		handle* currentHandle = h;
		while (true)
		{
			for (map<std::string, SQLWordArray*>::iterator iter = m_parseTreeHead.begin();
				iter != m_parseTreeHead.end(); iter++)
			{
				const char* tmp = sql;
				SQLWord* s = static_cast<SQLWord*>(iter->second);
				if (s->match(currentHandle, tmp) == NOT_MATCH_PTR)
				{
					continue;
				}
				else
				{
					h->sql = sql;
					sql = nextWord(tmp);
					goto PARSE_SUCCESS;
				}
			}
			/*not match after compare to all SQLWords in m_parseTreeHead,return*/
			delete h;
			h = nullptr;
			return parseValue::NOT_MATCH;
		PARSE_SUCCESS:
			if (m_initUserDataFunc)
				m_initUserDataFunc(h);

			statusInfo* s = currentHandle->head;
			while (s)
			{
				if (s->process(h) != OK)
				{
					delete h;
					h = nullptr;
					return parseValue::NOT_MATCH;
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
			if (m_destroyUserDataFunc)
				_h->destroyUserDataFunc = m_destroyUserDataFunc;
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
		sql = (char*)"rename table `mp_time_limit20` to `mp_time_limit_20`";
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
	sqlParser::handle* h;
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

