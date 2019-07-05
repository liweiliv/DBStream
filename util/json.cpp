/*
 * json.cpp
 *
 *  Created on: 2018年10月30日
 *      Author: liwei
 */
#include "json.h"

#include <string.h>
#include <stdio.h>
#ifdef OS_WIN
#include "winString.h"
#endif

jsonValue::type jsonValue::getType(const char * data)
{
    if (data == NULL)
        return J_NULLTYPE;
    const char * ptr = data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (*ptr == '{')
        return J_OBJECT;
    else if (*ptr == '[')
        return J_ARRAY;
    else if (*ptr == '"')
        return J_STRING;
    else if (*ptr <= '9' && *ptr >= '0')
        return J_NUM;
    else if (*ptr == '-' && ptr[1] <= '9' && ptr[1] >= '0')
        return J_NUM;
    else if(strncasecmp(ptr,"true",4)==0||strncasecmp(ptr,"false",5)==0)
        return J_BOOL;
    else
        return J_NULLTYPE;
}
jsonValue::jsonValue(jsonValue::type _t):t(_t){}

jsonValue * jsonValue::Parse(const char* data, int &size)
{
    switch (getType(data))
    {
    case J_NUM:
    {
        jsonNum * j = new jsonNum();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case J_STRING:
    {
        jsonString * j = new jsonString();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case J_OBJECT:
    {
        jsonObject * j = new jsonObject();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case J_ARRAY:
    {
        jsonArray * j = new jsonArray();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;
    }
    case J_BOOL:
    {
        jsonBool * j = new jsonBool();
        size = j->parse(data);
        if(size<0)
        {
            delete j;
            return NULL;
        }
        else
            return j;

    }
    default:
        return NULL;
    }
}

jsonString::jsonString(const char * data ) :
        jsonValue(J_STRING)
{
    parse(data);
}
string jsonString::toString()
{
    string s("\"");
    s.append(m_value).append("\"");
    return s;
}
int jsonString::parse(const char * data)
{
    if (data == NULL)
        return 0;
    m_value.clear();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '"')
        return -1;
    const char * e = strchr(ptr + 1, '"');
    if (e == NULL)
        return -2;
    m_value.assign(ptr + 1, e - ptr - 1);
    return (int)(e - data + 1);
}
jsonNum::jsonNum(const char * data ) :
        jsonValue(J_NUM), m_value(0)
{
    parse(data);
}
int jsonNum::parse(const char * data)
{
    m_value = 0;
    bool flag = true;
    if (data == NULL)
        return 0;
    const char * ptr = data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (*ptr == '-')
    {
        flag = false;
        ptr++;
    }
    if (*ptr > '9' || *ptr < '0')
        return -1;
    while (*ptr <= '9' && *ptr >= '0')
    {
        m_value *= 10;
        m_value += ptr[0] - '0';
        ptr++;
    }
    if (!flag)
        m_value = -m_value;
    return (int)(ptr - data);
}
string jsonNum::toString()
{
    char buf[32] = {0};
    sprintf(buf,"%ld",m_value);
    return buf;
}
jsonObject::jsonObject(const char * data ) :
        jsonValue(J_OBJECT)
{
    parse(data);
}
jsonObject::~jsonObject()
{
    clean();
}
jsonValue * jsonObject::get(const string &s)
{
	std::map<std::string, jsonValue*>::iterator iter = m_values.find(s);
	if (iter == m_values.end())
		return nullptr;
	return iter->second;
}
jsonValue* jsonObject::get(const char* s)
{
	std::map<std::string, jsonValue*>::iterator iter = m_values.find(s);
	if (iter == m_values.end())
		return nullptr;
	return iter->second;
}
void jsonObject::clean()
{
    for (std::map< std::string, jsonValue* >::iterator i = m_values.begin(); i != m_values.end();
            i++)
    {
		if (i->second != nullptr)
			delete i->second;
    }
    m_values.clear();
}
int jsonObject::parse(const char * data)
{
    if (data == NULL)
        return 0;
    clean();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '{')
        return -1;
    ptr++;
    while (*ptr != 0)
    {
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == '}')
            break;
		jsonString k;
		int size = k.parse(ptr);
        if (k.m_value.empty())
        {
            clean();
            return -1;
        }
        ptr += size;
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr != ':')
        {
            clean();
            return -1;
        }
        ptr++;
        jsonValue * v = jsonValue::Parse(ptr, size);
        if (v == NULL)
        {
            clean();
            return -1;
        }
        ptr += size;
        if (!m_values.insert(std::pair<std::string, jsonValue*>(k.m_value, v)).second)
        {
            clean();
            return -1;
        }
        objectKeyValuePair kv(k.m_value.c_str(), v);
        m_valueList.push_back(kv);

        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == '}')
            break;
        else if (*ptr != ',')
        {
            clean();
            return -1;
        }
        ptr++;
    }
    return (int)(ptr - data + 1);
}
string jsonObject::toString()
{
    string s = "{";
    bool first = true;
    for(std::map<std::string, jsonValue*>::iterator i = m_values.begin(); i != m_values.end();i++)
    {
		jsonValue* v = i->second;
        if(v!=NULL)
        {
            if(!first)
                s.append(",");
            else
                first = false;
            s.append("\"").append(i->first).append("\":").append(v->toString());
        }
    }
    s.append("}");
    return s;
}
jsonArray::jsonArray(const char * data) :
        jsonValue(J_ARRAY)
{
    parse(data);
}
jsonArray::~jsonArray()
{
    clean();
}
void jsonArray::clean()
{
    for (list<jsonValue*>::iterator i = m_values.begin(); i != m_values.end();
            i++)
    {
        if (*i != NULL)
        {
            delete (*i);
        }
    }
    m_values.clear();
}
int jsonArray::parse(const char * data)
{
    if (data == NULL)
        return 0;
    clean();
    const char * ptr = data;
    string value;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if (ptr[0] != '[')
        return -1;
    ptr++;
    while (*ptr != 0)
    {
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == ']')
            break;
        int size = 0;
        jsonValue * v = jsonValue::Parse(ptr, size);
        if (v == NULL)
        {
            clean();
            return -1;
        }
        m_values.push_back(v);
        ptr+=size;
        while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
            ptr++;
        if (*ptr == ']')
            break;
        else if (*ptr != ',')
        {
            clean();
            return -1;
        }
        ptr++;
    }
    return (int)(ptr - data + 1);
}
string jsonArray::toString()
{
    string s = "[";
    bool first = true;
    for(list<jsonValue*>::iterator i = m_values.begin(); i != m_values.end();i++)
    {
        jsonValue * v = *i;
        if(v!=NULL)
        {
            if(!first)
                s.append(",");
            else
                first = false;
            s+=v->toString();
        }
    }
    s.append("]");
    return s;
}
jsonBool::jsonBool(const char * data):jsonValue(J_BOOL),m_value(false)
{
    parse(data);
}
int jsonBool::parse(const char * data)
{
    if(data==NULL)
    {
        m_value = false;
        return 0;
    }
    const char * ptr =data;
    while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n')
        ptr++;
    if(strncasecmp(ptr,"true",4)==0)
    {
        m_value = true;
        return (int)(4+ptr-data);
    }
    else if(strncasecmp(ptr,"false",5)==0)
    {
        m_value = false;
        return (int)(5+ptr-data);
    }
    else
        return -1;
}
string jsonBool::toString()
{
    return m_value?"true":"false";
}


