/*
 * json.h
 *
 *  Created on: 2018年11月14日
 *      Author: liwei
 */
#ifndef JSON_H_
#define JSON_H_
#include <map>
#include <list>
#include <string>
#include <stdint.h>
#include "winDll.h"
using namespace std;


class DLL_EXPORT jsonValue
{
public:
    enum type
    {
        J_STRING,
        J_NUM,
        J_OBJECT,
        J_ARRAY,
        J_BOOL,
        J_NULLTYPE
    };

    type t;
    jsonValue(type _t);
    virtual ~jsonValue(){};
    virtual string toString()=0;
    static type getType(const char * data);
    static jsonValue * Parse(const char* data,int &size);
};
class DLL_EXPORT jsonString :public jsonValue
{
public:
    std::string m_value;
    jsonString(const char * data=NULL);
    int parse(const char * data);
    string toString();
    ~jsonString(){}
};
class DLL_EXPORT jsonNum :public jsonValue
{
public:
    long m_value;
    jsonNum(const char * data = NULL);
    int parse(const char * data);
    string toString();
    ~jsonNum(){}
};
class DLL_EXPORT jsonObject :public jsonValue
{
public:
    std::map<std::string, jsonValue *> m_values;
    jsonObject(const char * data=NULL);
    jsonValue * get(const string &s);
	jsonValue* get(const char* s);
    ~jsonObject();
    void clean();
    int parse(const char * data);
    string toString();
};
class DLL_EXPORT jsonArray :public jsonValue
{
public:
    std::list<jsonValue*> m_values;
    jsonArray(const char * data=NULL);
    ~jsonArray();
    void clean();
    int parse(const char * data);
    string toString();
};
class DLL_EXPORT jsonBool :public jsonValue
{
public:
    bool m_value;
    jsonBool(const char * data = NULL);
    int parse(const char * data);
    string toString();
};

#endif
