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
#include "status.h"
using namespace std;
enum class JSON_TYPE
{
	J_STRING,
	J_NUM,
	J_OBJECT,
	J_ARRAY,
	J_BOOL,
	J_NULL_TYPE,
	J_INVALID_TYPE
};
class DLL_EXPORT jsonValue
{
public:
	JSON_TYPE t;
	jsonValue(JSON_TYPE _t);
	virtual ~jsonValue() {};
	virtual string toString(int level = 0)const = 0;
	static JSON_TYPE getType(const char* data);
	static dsStatus& parse(jsonValue* &value,const char* data, int& size);
	virtual dsStatus& parse(const char* data, int& size) = 0;
};
class DLL_EXPORT jsonString :public jsonValue
{
public:
	std::string m_value;
	jsonString(const char* data = nullptr);
	dsStatus& parse(const char* data,int & size);
	string toString(int level = 0) const;
	~jsonString() {}
};
class DLL_EXPORT jsonNum :public jsonValue
{
public:
	long m_value;
	jsonNum(const char* data = nullptr);
	dsStatus& parse(const char* data, int& size);
	string toString(int level = 0) const;
	~jsonNum() {}
};
typedef std::map<std::string, jsonValue*> jsonObjectMap;
class DLL_EXPORT jsonObject :public jsonValue
{
public:
	std::list<std::pair<std::string, jsonValue*> > m_valueList;
	jsonObjectMap m_values;
	jsonObject(const char* data = nullptr);
	const jsonValue* get(const string& s)const;
	const jsonValue* get(const char* s)const;
	~jsonObject();
	void clean();
	dsStatus& parse(const char* data, int& size);
	string toString(int level = 0) const;
};
class DLL_EXPORT jsonArray :public jsonValue
{
public:
	std::list<jsonValue*> m_values;
	jsonArray(const char* data = nullptr);
	~jsonArray();
	void clean();
	dsStatus& parse(const char* data, int& size);
	string toString(int level = 0) const;
};
class DLL_EXPORT jsonBool :public jsonValue
{
public:
	bool m_value;
	jsonBool(const char* data = nullptr);
	dsStatus& parse(const char* data, int& size);
	string toString(int level = 0)const;
};

#endif
