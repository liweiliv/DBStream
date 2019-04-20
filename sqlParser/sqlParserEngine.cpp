/*
 * sqlParserEngine.cpp
 *
 *  Created on: 2018年11月16日
 *      Author: liwei
 */

#include <stdio.h>
#include <fcntl.h>
#define OS_WIN
#include "../util/file.h"
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
#include "../util/stackLog.h"
#include "SQLStringUtil.h"
#include "sqlParser.h"
#include "../util/json.h"
#ifdef OS_WIN
#include "../util/winString.h"
#endif
using namespace std;
namespace SQL_PARSER
{

class SQLWord
{
public:
    bool m_optional;
    uint32_t m_refs;
    string m_comment;
    uint32_t m_id;
    enum SQLWordType
    {
        SQL_ARRAY, SQL_SIGNLE_WORD
    };
    SQLWordType m_type;
    parseValue (*m_parser)(handle * h, const string &sql);
    virtual parseValue match(handle * h, const char *& sql) = 0;
    void include()
    {
        m_refs++;
    }
    bool deInclude()
    {
        return --m_refs == 0;
    }
    SQLWord(SQLWordType t, bool optional = false) :
            m_type(t), m_parser(
            NULL), m_optional(optional), m_refs(0), m_id(0)
    {
    }
};

class SQLSingleWord: public SQLWord
{
public:
    enum sqlSingleWordType
    {
        S_CHAR,
		S_NAME, //dbname,tablename,column name
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
    printf("%d,%s \033[1m\033[40;31mnot match \033[0m:%s\n",m_id,m_comment.c_str(),tmp);return NOT_MATCH;\
    }while(0);
#else
#define N_MATCH    return NOT_MATCH;
#endif

    virtual parseValue match(handle *h, const char *& sql)
    {
        const char * p = nextWord(sql), *tmp = sql;
        parseValue rtv = OK;
        string matchedWord;
        switch (m_wtype)
        {
        case S_BRACKETS:
        {
            if (*p != '(')
                N_MATCH;
            const char * end = p + 1;
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
            const char * end = endOfWord(p);
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
            const char * nameStart, *nameEnd;
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
                    || (!isSpaceOrComment(p + m_word.size())
                            && !isKeyChar(p[m_word.size()])))
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
            const char * end = p + 1;
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
            const char * n = p;
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

        if (rtv == OK && m_parser != NULL)
        {
            statusInfo * s = new statusInfo;
            s->sql = matchedWord;
            s->parserFunc = m_parser;
            if (h->head == NULL)
                h->head = s;
            else
                h->end->next = s;
            h->end = s;
        }
#ifdef DEBUG
        if(rtv==OK)
        printf("%d,%s \033[1m\033[40;32mmatch \033[0m:%s \n",m_id,m_comment.c_str(),tmp);
        else
        printf("%d,%s \033[1m\033[40;31mnot match \033[0m:%s\n",m_id,m_comment.c_str(),tmp);
#endif
        return rtv;
    }
};
class SQLWordArray: public SQLWord
{
public:
    list<SQLWord *> m_words;
    bool m_or;
    bool m_loop;
    SQLWordArray(bool optional, bool _or, bool loop) :
            SQLWord(SQL_ARRAY, optional), m_or(_or), m_loop(loop)
    {
    }
    ~SQLWordArray()
    {
        for (list<SQLWord *>::iterator iter = m_words.begin();
                iter != m_words.end(); iter++)
        {
            SQLWord * s = static_cast<SQLWord*>(*iter);
            if (s != NULL)
            {
                if (s->deInclude())
                    delete (*iter);
            }
        }
    }
    SQLWordArray(const SQLWordArray&s) :
            SQLWord(SQL_ARRAY, s.m_optional), m_or(s.m_or), m_loop(s.m_loop)
    {

    }
    void append(SQLWord *s)
    {
        m_words.push_back(s);
    }
    virtual parseValue match(handle *h, const char *& sql)
    {
        parseValue rtv = OK;
        bool matched = false;
        const char * tmp = sql;
        statusInfo * top = h->end;
        do
        {
            for (list<SQLWord *>::iterator iter = m_words.begin();
                    iter != m_words.end(); iter++)
            {
                SQLWord * s = *iter;
                const char * str = nextWord(sql);
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
                    rtv = OK;
                break;
            }
            else
                matched = true;
        } while (m_loop);

        if (rtv != OK) //rollback
        {
            for (statusInfo * s = top ? top->next : NULL; s != NULL;)
            {
                statusInfo * tmp = s->next;
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
                statusInfo * s = new statusInfo;
                s->parserFunc = m_parser;
                if (h->head == NULL)
                    h->head = s;
                else
                    h->end->next = s;
                h->end = s;
            }
            sql = tmp;
        }
#ifdef DEBUG
        if(rtv==OK)
        printf("%d,%s \033[1m\033[40;32mmatch \033[0m:%s \n",m_id,m_comment.c_str(),tmp);
        else
        {
            printf("%d,%s \033[1m\033[40;31mnot match \033[0m:%s\n",m_id,m_comment.c_str(),tmp);
        }
#endif
        return rtv;
    }
};
SQLWord* sqlParser::loadSQlWordFromJson(jsonValue *json)
{
    SQLWord * s = NULL;
    jsonObject * obj = static_cast<jsonObject*>(json);
    jsonValue * value = obj->get("OPT");
    bool optional = false; //default  false
    if (value != NULL)
    {
        if (value->t != jsonObject::J_BOOL)
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
        }
        optional = static_cast<jsonBool*>(value)->m_value;
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
    bool loop = false;
    value = obj->get("LOOP");
    if (value != NULL)
    {
        if (value->t != jsonObject::J_BOOL)
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool type");
        }
        loop = static_cast<jsonBool*>(value)->m_value;
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
        map<uint32_t, SQLWord *>::iterator iter = m_parseTree.find(
                static_cast<jsonNum*>(value)->m_value);
        if (iter == m_parseTree.end())
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1,
                    "can not find include id %d in parse tree",
                    static_cast<jsonNum*>(value)->m_value);
        }
        iter->second->include(); //it will not be free by parent when destroy
        return iter->second;
    }

    if ((value = obj->get("K")) != NULL)
    {
        if (value->t != jsonObject::J_STRING)
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect STRING type");
        }
        if (static_cast<jsonString*>(value)->m_value == "_A_")
            s = new SQLSingleWord(optional, SQLSingleWord::S_ARRAY, "");
        else if (static_cast<jsonString*>(value)->m_value == "_AS_")
            s = new SQLSingleWord(optional, SQLSingleWord::S_ANY_STRING, "");
        else if (static_cast<jsonString*>(value)->m_value == "_N_")
            s = new SQLSingleWord(optional, SQLSingleWord::S_NAME, "");
        else if (static_cast<jsonString*>(value)->m_value == "_B_")
            s = new SQLSingleWord(optional, SQLSingleWord::S_BRACKETS, "");
        else if (static_cast<jsonString*>(value)->m_value == "_NUM_")
            s = new SQLSingleWord(optional, SQLSingleWord::S_NUMBER, "");
        else if (strncmp(static_cast<jsonString*>(value)->m_value.c_str(),
                "_S_:", 4) == 0)
            s = new SQLSingleWord(optional, SQLSingleWord::S_STRING,
                    static_cast<jsonString*>(value)->m_value.c_str() + 4);
        else if (strncmp(static_cast<jsonString*>(value)->m_value.c_str(),
                "_C_:", 4) == 0)
            s = new SQLSingleWord(optional, SQLSingleWord::S_CHAR,
                    static_cast<jsonString*>(value)->m_value.c_str() + 4);
        else
        {
            SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect STRING type :%s",
                    static_cast<jsonString*>(value)->m_value.c_str());
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
                            (parseValue (*)(handle *, const string&)) dlsym(
                                    m_funcsHandle,
                                    static_cast<jsonString*>(value)->m_value.c_str())))
#else ifdef OS_WIN
			if (NULL
				== (static_cast<SQLSingleWord*>(s)->m_parser =
				(parseValue(*)(handle *, const string&)) GetProcAddress(
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
        s = new SQLWordArray(optional, OR, loop);
        for (list<jsonValue*>::iterator iter =
                static_cast<jsonArray*>(value)->m_values.begin();
                iter != static_cast<jsonArray*>(value)->m_values.end(); iter++)
        {
            if ((*iter)->t != jsonObject::J_OBJECT)
            {
                delete s;
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect OBJECT type");
            }
            SQLWord * child = loadSQlWordFromJson(*iter);
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
        printf("%d,%s\n", s->m_id, s->m_comment.c_str());
    }
    return s;
}
sqlParser::sqlParser() :
        m_funcsHandle(nullptr), m_initUserDataFunc(nullptr), m_destroyUserDataFunc(nullptr)
{
}
sqlParser::~sqlParser()
{
#ifdef OS_LINUX
    if (m_funcsHandle)
        dlclose(m_funcsHandle);
#else ifdef OS_WIN
	if (m_funcsHandle)
		FreeLibrary(m_funcsHandle);
#endif
    for (map<uint32_t, SQLWord *>::iterator iter = m_parseTree.begin();
            iter != m_parseTree.end(); iter++)
    {
        if (iter->second->deInclude())
            delete iter->second;
    }
}
bool fileExist(const char * file)
{
    fileHandle fd = openFile(file, true,false,false);
    if (fd > 0)
    {
        closeFile(fd);
        return true;
    }
    return false;
}
#ifdef OS_LINUX
int sqlParser::LoadFuncs(const char * fileName)
{
    if (!fileExist(fileName))
        return -1;
    string soName;
    string compileCmd;
    const char * end = fileName + strlen(fileName);
    while (*end != '.' && end != fileName)
        end--;
    if (end == fileName)
        end = fileName + strlen(fileName);
    soName = string("lib").append(fileName, end - fileName).append(".so");
    if (!fileExist(soName.c_str()))
        goto COMPILE;
    if (getFileTime(fileName) >= getFileTime(soName.c_str()))
        remove(soName.c_str());
    else
        goto LOAD;
COMPILE:
    compileCmd.assign("g++ -shared -g -fPIC -Wall -o ");
    compileCmd.append(soName).append(" ").append(fileName);
    if (system(compileCmd.c_str()) != 0)
        return -2;
LOAD:
    if (m_funcsHandle != NULL)
        dlclose(m_funcsHandle);
    m_funcsHandle = dlopen(string("./").append(soName).c_str(), RTLD_NOW);
    if (m_funcsHandle == NULL)
    {
        fprintf(stderr, "load %s failed for %s\n", soName.c_str(), dlerror());
        return -3;
    }
	m_initUserDataFunc = (void(*)(handle *)) dlsym(m_funcsHandle, "createUserData");
	m_destroyUserDataFunc = (void(*)(handle *)) dlsym(m_funcsHandle, "destroyUserData");
    return 0;
}
#else ifdef OS_WIN
int sqlParser::LoadFuncs(const char * fileName)
{
	if (m_funcsHandle != NULL)
		FreeLibrary(m_funcsHandle);
	const char * end = fileName + strlen(fileName);
	while (*end != '.' && end != fileName)
		end--;
	string soName  = string("lib").append(fileName, end - fileName).append(".dll");
	m_funcsHandle = LoadLibrary(string("./").append(soName).c_str());
	if (m_funcsHandle == NULL)
	{
		fprintf(stderr, "load %s failed for %d,%s\n", soName.c_str(), GetLastError(), strerror(GetLastError()));
		return -3;
	}
	m_initUserDataFunc = (void(*)(handle *)) GetProcAddress(m_funcsHandle, "createUserData");
	m_destroyUserDataFunc = (void(*)(handle *)) GetProcAddress(m_funcsHandle, "destroyUserData");
	return 0;
}
#endif
int sqlParser::LoadParseTree(const char *config)
{
    int32_t size = 0;
    jsonValue * v = NULL;
    const char * p = nextWord(config);
    while (true)
    {
        if (NULL == (v = jsonValue::Parse(p, size)))
        {
            printf("load parse tree from %s failed\n", p);
            return -1;
        }

        if (v->t != jsonValue::J_OBJECT)
        {
            delete v;
            return -1;
        }
        int id = 0;
        jsonValue * jv;
        if ((jv = static_cast<jsonObject*>(v)->get("ID")) != NULL)
        {
            if (jv->t != jsonObject::J_NUM)
            {
                delete v;
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect num");
            }
            id = static_cast<jsonNum*>(jv)->m_value;
        }
        bool head = false;
        if ((jv = static_cast<jsonObject*>(v)->get("HEAD")) != NULL)
        {
            if (jv->t != jsonObject::J_BOOL)
            {
                delete v;
                SET_STACE_LOG_AND_RETURN_(NULL, -1, "expect bool");
            }
            head = static_cast<jsonBool*>(jv)->m_value;
        }
        SQLWord * s = loadSQlWordFromJson(v);
        if (s == NULL)
        {
            delete v;
            SET_STACE_LOG_AND_RETURN_(-1, -1, "load  parse tree failed");
        }
        s->include();
        pair<map<uint32_t, SQLWord *>::iterator, bool> i = m_parseTree.insert(
                pair<uint32_t, SQLWord*>(id, s));
        if (!i.second)
        {
            printf("%s has the same id [%d] with %s\n", v->toString().c_str(),
                    id, i.first->second->m_comment.c_str());
            delete v;
            return -1;
        }
        if (head)
            m_parseTreeHead.insert(pair<uint32_t, SQLWord*>(id, s));
        delete v;
        p = nextWord(p + size);
        if (p[0] == '\0')
            break;
    }
    return 0;
}
int sqlParser::LoadParseTreeFromFile(const char * file)
{
    fileHandle fd = openFile(file, true,false,false);
    if (fd < 0)
        return -1;
    uint32_t size = seekFile(fd, 0, SEEK_END);
	seekFile(fd, 0, SEEK_SET);
    char * buf = (char*) malloc(size + 1);
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
char * preProcessSql(const char * sql, uint32_t version)
{
    char * newSql = (char*) malloc(strlen(sql) + 1);
    const char * src = jumpOverSpace(sql);
    char * dest = newSql;
    while (*src != '\0')
    {
        src = jumpOverSpace(src);
        const char * end;
        if (strncmp(src, "/*", 2) == 0) //[/*!410000 DEFAULT CHARSET UTF8*/]
        {
            const char * endOfComment = src;
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
            const char * end = realEndOfWord(src);
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
parseValue sqlParser::parse(handle *&h, const char * sql)
{
    h = new handle;
    handle * currentHandle = h;
    while (true)
    {
        for (map<uint32_t, SQLWord *>::iterator iter = m_parseTreeHead.begin();
                iter != m_parseTreeHead.end(); iter++)
        {
            const char * tmp = sql;
            SQLWord * s = static_cast<SQLWord*>(iter->second);
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
		statusInfo * s = currentHandle->head;
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
        handle * _h = new handle;
        currentHandle->next = _h;
        currentHandle = _h;
    }
}
}
;
#ifdef TEST
int main(int argc, char * argv[])
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
    char * sql = NULL;
    if (argc == 1)
        sql = (char*) "rename table `mp_time_limit20` to `mp_time_limit_20`";
    else
    {
        fileHandle fd = openFile(argv[1], true,false,false);
        if (fd < 0)
            return -1;
        uint32_t size = seekFile(fd, 0, SEEK_END);
		seekFile(fd, 0, SEEK_SET);
        sql = (char*) malloc(size + 1);
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
    char * newSql = SQL_PARSER::preProcessSql(sql, 0xfffffffful);
    printf("%s\n", newSql);

}
#endif
