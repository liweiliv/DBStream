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

	class SQLWord
	{
	public:
		bool m_optional;
		uint32_t m_refs;
		string m_comment;
		uint32_t m_id;
		SQL_TYPE m_sqlType;
		enum SQLWordType
		{
			SQL_ARRAY, SQL_SIGNLE_WORD
		};
		SQLWordType m_type;
		parseValue(*m_parser)(handle* h, const string& sql);
		virtual parseValue match(handle* h, const char*& sql) = 0;
		void include()
		{
			m_refs++;
		}
		bool deInclude()
		{
			return --m_refs == 0;
		}
		SQLWord(SQLWordType t, bool optional = false) :
			m_optional(optional), m_refs(0), m_id(0), m_sqlType(UNSUPPORT),m_type(t), m_parser(
				NULL)
		{
		}
		virtual ~SQLWord() {}
	};

	class SQLSingleWord : public SQLWord
	{
	public:
		enum sqlSingleWordType
		{
			S_CHAR, S_NAME, //dbname,tablename,column name
			S_ARRAY, //"xxxx" or 'xxxx'
			S_STRING,
			S_ANY_STRING,
			S_BRACKETS,
			S_NUMBER
		};

		string m_word;
		sqlSingleWordType m_wtype;
		SQLSingleWord(bool optional, sqlSingleWordType type, string word) :
			SQLWord(SQL_SIGNLE_WORD, optional), m_word(word), m_wtype(type)
		{
		}
#ifdef DEBUG

#define N_MATCH do{ \
    printf("%d,%s \033[1m\033[40;31mnot match \033[0m\n",m_id,m_comment.c_str());return NOT_MATCH;\
    }while(0);
#else
#define N_MATCH    return NOT_MATCH;
#endif

		virtual parseValue match(handle* h, const char*& sql)
		{
			const char* p = nextWord(sql);
			parseValue rtv = OK;
			string matchedWord;
			switch (m_wtype)
			{
			case S_BRACKETS:
			{
				if (*p != '(')
					N_MATCH;
				const char* end = p + 1;
				int32_t bracketCount = 1;
				while (*end != '\0')
				{
					if (*end == '(')
						bracketCount++;
					else if (*end == ')')
					{
						if (--bracketCount <= 0)
							break;
					}
					end++;
				}
				if (*end == '\0')
					N_MATCH;
				if (m_parser != NULL)
					matchedWord.assign(p, end - p);
				sql = end + 1;
				break;
			}
			case S_CHAR:
			{
				char c = *p;
				if (c >= 'A' && c <= 'Z')
					c += 'a' - 'A';
				if (m_word[0] != c)
					N_MATCH;
				if (m_parser != NULL)
					matchedWord = string(p, 1);
				sql = p + 1;
				break;
			}
			case S_ANY_STRING:
			{
				const char* end = endOfWord(p);
				if (end == NULL)
					N_MATCH
					if (isKeyWord(p, end - p))
						N_MATCH
						if (m_parser != NULL)
							matchedWord = string(p, end - p);
				if (rtv != OK)
					return rtv;
				sql = end;
				break;
			}
			case S_NAME:
			{
				const char* nameStart, * nameEnd;
				uint16_t nameSize;
				if (!getName(p, nameStart, nameSize, nameEnd))
					N_MATCH
					if (*p != '\'' && *p != '`' && *p != '"')
					{
						if (isKeyWord(nameStart, nameSize))
							N_MATCH
					}
				if (m_parser != NULL)
					matchedWord = string(nameStart, nameSize);
				if (rtv != OK)
					return rtv;
				sql = nameEnd;
				break;
			}
			case S_STRING:
			{
				if (strncasecmp(m_word.c_str(), p, m_word.size()) != 0
					||(!isSpaceOrComment(p + m_word.size()) && p[m_word.size()] != '\0' && !isKeyChar(p[m_word.size()])))

					N_MATCH
					if (m_parser != NULL)
						matchedWord = m_word;
				if (rtv != OK)
					return rtv;
				sql = p + m_word.size();
				break;
			}
			case S_ARRAY:
			{
				if (*p != '\'' && *p != '"')
					return NOT_MATCH;
				char quote = *p;
				const char* end = p + 1;
				while (true)
				{
					if (*end == '\0')
						break;
					if (*end == quote)
					{
						if (*(end - 1) != '\\')
						{
							if (quote == '\'' && *(end + 1) == '\'')
							{
								end += 2;
								continue;
							}
							break;
						}
						else
						{
							end++;
							continue;
						}

					}
					else
						end++;
				}
				if (*end == '\0')
					N_MATCH
					if (m_parser != NULL)
						matchedWord = string(p + 1, end - p - 1);
				if (rtv != OK)
					return rtv;
				sql = end + 1;
				break;
			}
			case S_NUMBER:
			{
				const char* n = p;
				bool hasDot = false;
				while (n != '\0')
				{
					if (*n > '9' || *n < '0')
					{
						if (*n == '-')
						{
							if (n != p)
								N_MATCH
								n++;
							while (*n == ' ' || *n == '\t')
								n++;
							continue;
						}
						else if (*n == '.')
						{
							if (hasDot)
								N_MATCH
							else
								hasDot = true;
						}
						else
						{
							if (isKeyChar(*n))
							{
								if (n == p)
									N_MATCH
								else
									break;
							}
							else if (isSpaceOrComment(n))
								break;
							else
								N_MATCH;
						}
					}
					n++;
				}
				if (m_parser != NULL)
					matchedWord = string(p, n - p);
				if (rtv != OK)
					return rtv;
				sql = n;
				break;
			}
			default:
				return NOT_SUPPORT;
			}

			if (rtv == OK&&h!=nullptr)
			{
				if (m_sqlType != UNSUPPORT)
					h->type = m_sqlType;
				if (m_parser != nullptr)
				{
					statusInfo* s = new statusInfo;
					s->sql = matchedWord;
					s->parserFunc = m_parser;
					if (h->head == NULL)
						h->head = s;
					else
						h->end->next = s;
					h->end = s;
				}
			}
#ifdef DEBUG
			if (rtv == OK)
				printf("%d,%s \033[1m\033[40;32mmatch \033[0m \n", m_id, m_comment.c_str());
			else
				printf("%d,%s \033[1m\033[40;31mnot match \033[0m\n", m_id, m_comment.c_str());
#endif
			return rtv;
		}
		static SQLSingleWord* create(bool optional,const std::string &str)
		{
			SQLSingleWord* s = nullptr;
			if (str == "_A_")
				s = new SQLSingleWord(optional, SQLSingleWord::S_ARRAY, "");
			else if (str == "_AS_")
				s = new SQLSingleWord(optional, SQLSingleWord::S_ANY_STRING, "");
			else if (str == "_N_")
				s = new SQLSingleWord(optional, SQLSingleWord::S_NAME, "");
			else if (str == "_B_")
				s = new SQLSingleWord(optional, SQLSingleWord::S_BRACKETS, "");
			else if (str == "_NUM_")
				s = new SQLSingleWord(optional, SQLSingleWord::S_NUMBER, "");
			else if (strncmp(str.c_str(),"_S_:", 4) == 0)
				s = new SQLSingleWord(optional, SQLSingleWord::S_STRING, str.c_str() + 4);
			else if (strncmp(str.c_str(),"_C_:", 4) == 0)
				s = new SQLSingleWord(optional, SQLSingleWord::S_CHAR, str.c_str() + 4);
			else
			{
				SET_STACE_LOG_AND_RETURN_(nullptr, -1, "expect STRING type :%s",str.c_str());
			}
			return s;
		}
	};
	class SQLWordArray : public SQLWord
	{
	public:
		list<SQLWord*> m_words;
		bool m_or;
		bool m_loop;
		SQLSingleWord* m_loopCondition;
		SQLWordArray(bool optional, bool _or, bool loop,SQLSingleWord * loopCondition) :
			SQLWord(SQL_ARRAY, optional), m_or(_or),m_loop(loop), m_loopCondition(loopCondition)
		{
		}
		~SQLWordArray()
		{
			for (list<SQLWord*>::iterator iter = m_words.begin();
				iter != m_words.end(); iter++)
			{
				SQLWord* s = static_cast<SQLWord*>(*iter);
				if (s != NULL)
				{
					if (s->deInclude())
						delete (*iter);
				}
			}
		}
		SQLWordArray(const SQLWordArray& s) :
			SQLWord(SQL_ARRAY, s.m_optional), m_or(s.m_or), m_loop(s.m_loop), m_loopCondition(s.m_loopCondition)
		{

		}
		void append(SQLWord* s)
		{
			m_words.push_back(s);
		}
		virtual parseValue match(handle* h, const char*& sql)
		{
			parseValue rtv = OK;

			bool matched = false;
			const char* tmp = sql,*beforeLoop = nullptr;
			statusInfo* top = h->end;
			do
			{
				for (list<SQLWord*>::iterator iter = m_words.begin();
					iter != m_words.end(); iter++)
				{
					SQLWord* s = *iter;
					const char* str = nextWord(sql);
					if ((s) == NULL)
						continue;
					if (s->m_type == SQL_ARRAY)
					{
						rtv = static_cast<SQLWordArray*>(s)->match(h, str);
					}
					else if (s->m_type == SQL_SIGNLE_WORD)
					{
						rtv = static_cast<SQLSingleWord*>(s)->match(h, str);
					}
					if (rtv != OK)
					{
						if (m_or)
							continue;
						if (s->m_optional)
						{
							rtv = OK;
							continue;
						}
						else
							break;
					}
					else
					{
						sql = str;
						if (m_or)
							break;
					}
				}
				if (rtv != OK)
				{
					if (m_loop && matched)
					{
						if(beforeLoop!=nullptr)
							sql = beforeLoop;
						rtv = OK;
					}
					break;
				}
				else
					matched = true;
				if (m_loop)
				{
					if (m_loopCondition != nullptr)
					{
						beforeLoop = sql;
						const char* str = nextWord(sql);
						if (m_loopCondition->match(nullptr, str) == OK)
						{
							sql = str;
							continue;
						}
						else
							break;
					}
				}
				else
					break;
			} while (1);

			if (rtv != OK) //rollback
			{
				for (statusInfo* s = top ? top->next : NULL; s != NULL;)
				{
					statusInfo* tmp = s->next;
					delete s;
					s = tmp;
				}
				h->end = top;
				if (h->end != NULL)
					h->end->next = NULL;
				else
					h->head = NULL;
			}
			if (rtv != OK)
			{
				if (m_parser != NULL)
				{
					statusInfo* s = new statusInfo;
					s->parserFunc = m_parser;
					if (h->head == NULL)
						h->head = s;
					else
						h->end->next = s;
					h->end = s;
				}
				sql = tmp;
			}
			if (rtv == OK)
			{
				if (m_sqlType != UNSUPPORT)
					h->type = m_sqlType;
#ifdef DEBUG
				printf("%d,%s \033[1m\033[40;32mmatch \033[0m:%s \n", m_id, m_comment.c_str(), tmp);
			}
			else
			{
				printf("%d,%s \033[1m\033[40;31mnot match \033[0m:%s\n", m_id, m_comment.c_str(), tmp);
#endif
			}
			return rtv;
		}
	};
	SQLWord* sqlParser::loadSQlWordFromJson(jsonValue* json)
	{
		SQLWord* s = NULL;
		jsonObject* obj = static_cast<jsonObject*>(json);
		jsonValue* value = obj->get("OPT");
		bool optional = false; //default  false
		if (value != NULL)
		{
			if (value->t != jsonObject::J_BOOL)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
			}
			optional = static_cast<jsonBool*>(value)->m_value;
		}
		SQL_TYPE sqlType = UNSUPPORT;
		if ((value = static_cast<jsonObject*>(obj)->get("TYPE")) != NULL || (value = static_cast<jsonObject*>(obj)->get("type")) != NULL)
		{
			if (value->t != jsonObject::J_STRING)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect string type");
			}
			SQL_TYPE_TREE::const_iterator iter = m_sqlTypes.find(static_cast<jsonString*>(value)->m_value.c_str());
			if (iter != m_sqlTypes.end())
				sqlType = iter->second;
		}
		bool OR = false;
		value = obj->get("OR");
		if (value != NULL)
		{
			if (value->t != jsonObject::J_BOOL)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
			}
			OR = static_cast<jsonBool*>(value)->m_value;
		}
		std::string loop;
		value = obj->get("LOOP");
		if (value != NULL)
		{
			if (value->t != jsonObject::J_STRING)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect string type");
			}
			loop = static_cast<jsonString*>(value)->m_value;
		}
		string comment;
		if (NULL != (value = obj->get("COMMENT")))
		{
			if (value->t != jsonObject::J_STRING)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect String");
			}
			comment = static_cast<jsonString*>(value)->m_value;
		}
		if (NULL != (value = obj->get("INCLUDE")))
		{
			if (value->t != jsonObject::J_NUM)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect num");
			}
			map<uint32_t, SQLWord*>::iterator iter = m_parseTree.find(
				static_cast<jsonNum*>(value)->m_value);
			if (iter == m_parseTree.end())
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1,
					"can not find include id %ld in parse tree",
					static_cast<jsonNum*>(value)->m_value);
			}
			iter->second->include(); //it will not be free by parent when destroy
			return iter->second;
		}

		if ((value = obj->get("K")) != NULL)
		{
			s = SQLSingleWord::create(optional, static_cast<jsonString*>(value)->m_value);
			if (s == nullptr)
			{
				return nullptr;
			}
			if ((value = obj->get("F")) != NULL)
			{
				if (value->t != jsonObject::J_STRING)
				{
					SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect STRING type :%s",
						static_cast<jsonString*>(value)->m_value.c_str());
				}
#ifdef OS_LINUX
				if (NULL
					== (static_cast<SQLSingleWord*>(s)->m_parser =
					(parseValue(*)(handle*, const string&)) dlsym(
						m_funcsHandle,
						static_cast<jsonString*>(value)->m_value.c_str())))
#endif
#ifdef OS_WIN
					if (NULL
						== (static_cast<SQLSingleWord*>(s)->m_parser =
						(parseValue(*)(handle*, const string&)) GetProcAddress(
							m_funcsHandle,
							static_cast<jsonString*>(value)->m_value.c_str())))
#endif
					{
						SET_STACE_LOG_AND_RETURN_(NULL, -1,
							"can not get func %s in funcs",
							static_cast<jsonString*>(value)->m_value.c_str());
					}
			}
		}
		else if ((value = obj->get("C")) != NULL)
		{
			if (value->t != jsonObject::J_ARRAY)
			{
				SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect ARRAY type");
			}
			if (!loop.empty())
			{
				if(loop=="always")
					s = new SQLWordArray(optional, OR, true, nullptr);
				else
				{
					SQLSingleWord* loopCondition = SQLSingleWord::create(false, loop);
					if (loopCondition == nullptr)
					{
						SET_STACE_LOG_AND_RETURN_(NULL, -1, "invalid loop condition:%s", loop.c_str());
					}
					s = new SQLWordArray(optional, OR, true, loopCondition);
				}
			}
			else
			{
				s = new SQLWordArray(optional, OR, false,nullptr);
			}
			for (list<jsonValue*>::iterator iter =
				static_cast<jsonArray*>(value)->m_values.begin();
				iter != static_cast<jsonArray*>(value)->m_values.end(); iter++)
			{
				if ((*iter)->t != jsonObject::J_OBJECT)
				{
					delete s;
					SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect OBJECT type");
				}
				SQLWord* child = loadSQlWordFromJson(*iter);
				if (child == NULL)
				{
					delete s;
					SET_STACE_LOG_AND_RETURN_(NULL, -1,
						"parse child parse tree failed");
				}
				static_cast<SQLWordArray*>(s)->append(child);
			}
		}
		if (s)
		{
			static int id = 0;
			s->m_id = id++;
			s->m_comment = json->toString();
			s->m_sqlType = sqlType;
			//printf("%d,%s\n", s->m_id, s->m_comment.c_str());
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
		for (map<uint32_t, SQLWord*>::iterator iter = m_parseTree.begin();
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
		const char* end = fileName + strlen(fileName);
		while (*end != '.' && end != fileName)
			end--;
		string soName = string("lib").append(fileName, end - fileName).append(".dll");
		m_funcsHandle = LoadLibrary(fileName);
		if (m_funcsHandle == NULL)
		{
			fprintf(stderr, "load %s failed for %d,%s\n", fileName, GetLastError(), strerror(GetLastError()));
			return -3;
		}
		m_initUserDataFunc = (void(*)(handle*)) GetProcAddress(m_funcsHandle, "createUserData");
		if (nullptr == m_initUserDataFunc)
		{
			fprintf(stderr, "load %s failed for %d,%s\n", fileName, GetLastError(), strerror(GetLastError()));
			FreeLibrary(m_funcsHandle);
			m_funcsHandle = nullptr;
			return -3;
		}
		m_destroyUserDataFunc = (void(*)(handle*)) GetProcAddress(m_funcsHandle, "destroyUserData");
		if (nullptr == m_destroyUserDataFunc)
		{
			fprintf(stderr, "load %s failed for %d,%s\n", fileName, GetLastError(), strerror(GetLastError()));
			FreeLibrary(m_funcsHandle);
			m_funcsHandle = nullptr;
			return -3;
		}
		return 0;
	}
#endif
	DLL_EXPORT int sqlParser::LoadParseTree(const char* config)
	{
		int32_t size = 0;
		jsonValue* segment = NULL;
		const char* p = nextWord(config);
		while (true)
		{
			if (NULL == (segment = jsonValue::Parse(p, size)))
			{
				printf("load parse tree from %s failed\n", p);
				return -1;
			}

			if (segment->t != jsonValue::J_OBJECT)
			{
				delete segment;
				return -1;
			}
			for (std::list<jsonObject::objectKeyValuePair>::const_iterator iter = static_cast<jsonObject*>(segment)->m_valueList.begin(); iter != static_cast<jsonObject*>(segment)->m_valueList.end(); iter++)
			{
				int id = 0;
				jsonValue* sentence = (*iter).value;
				jsonValue* value;
				if ((value = static_cast<jsonObject*>(sentence)->get("ID")) != NULL)
				{
					if (value->t != jsonObject::J_NUM)
					{
						delete segment;
						SET_STACE_LOG_AND_RETURN_(-1, -1, "expect num");
					}
					id = static_cast<jsonNum*>(value)->m_value;
				}
				else
				{
					delete segment;
					LOG(ERROR)<<static_cast<jsonObject*>(sentence)->toString()<<" do not have ID";
					return -1;
				}
				bool head = false;
				if ((value = static_cast<jsonObject*>(sentence)->get("HEAD")) != NULL)
				{
					if (value->t != jsonObject::J_BOOL)
					{
						delete segment;
						SET_STACE_LOG_AND_RETURN_(-1, -1, "expect bool");
					}
					head = static_cast<jsonBool*>(value)->m_value;
				}

				SQLWord* s = loadSQlWordFromJson(sentence);
				if (s == NULL)
				{
					delete segment;
					SET_STACE_LOG_AND_RETURN_(-1, -1, "load  parse tree failed");
				}
				s->include();
				pair<map<uint32_t, SQLWord*>::iterator, bool> i = m_parseTree.insert(
					pair<uint32_t, SQLWord*>(id, s));
				if (!i.second)
				{
					printf("%s has the same id [%d] with %s\n", sentence->toString().c_str(),
						id, i.first->second->m_comment.c_str());
					delete segment;
					return -1;
				}
				if (head)
					m_parseTreeHead.insert(pair<uint32_t, SQLWord*>(id, s));
			}
			delete segment;
			p = nextWord(p + size);
			if (p[0] == '\0')
				break;
		}
		return 0;
	}
	DLL_EXPORT int sqlParser::LoadParseTreeFromFile(const char* file)
	{
		fileHandle fd = openFile(file, true, false, false);
		if (!fileHandleValid(fd))
			return -1;
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
			for (map<uint32_t, SQLWord*>::iterator iter = m_parseTreeHead.begin();
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
	DLL_EXPORT parseValue sqlParser::parse(handle*& h, const char* sql)
	{
		h = new handle;
		handle* currentHandle = h;
		while (true)
		{
			for (map<uint32_t, SQLWord*>::iterator iter = m_parseTreeHead.begin();
				iter != m_parseTreeHead.end(); iter++)
			{
				const char* tmp = sql;
				SQLWord* s = static_cast<SQLWord*>(iter->second);
				if (s->match(currentHandle, tmp) != OK)
				{
					//   printf("%d,%s \033[1m\033[40;31m not match \033[0m:%.*s\n",iter->first,s->m_comment.c_str(),100,sql);
					continue;
				}
				else
				{
					//    printf("%d,%s \033[1m\033[40;32m match \033[0m:%.*s\n",iter->first,s->m_comment.c_str(),100,sql);
					sql = nextWord(tmp);
					goto PARSE_SUCCESS;
				}
			}
			/*not match after compare to all SQLWords in m_parseTreeHead,return*/
			delete h;
			h = NULL;
			return NOT_MATCH;
		PARSE_SUCCESS:
			if (m_initUserDataFunc)
				m_initUserDataFunc(h);
			if (m_destroyUserDataFunc)
				h->destroyUserDataFunc = m_initUserDataFunc;
			statusInfo* s = currentHandle->head;
			while (s)
			{
				if (s->parserFunc)
					if (OK != s->parserFunc(currentHandle, s->sql))
					{
						delete h;
						return NOT_MATCH;
					}
				s = s->next;
			}
			//  currentHandle->meta.print();
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

