/*
 * json.cpp
 *
 *  Created on: 2018年10月30日
 *      Author: liwei
 */
#include "json.h"
#include <string.h>
#include <stdio.h>
#include "winString.h"
#define min(a,b) (a)>(b)?(b):(a)

JSON_TYPE jsonValue::getType(const char* data)
{
	if (data == nullptr)
		return JSON_TYPE::J_INVALID_TYPE;
	const char* ptr = data;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (*ptr == '{')
		return JSON_TYPE::J_OBJECT;
	else if (*ptr == '[')
		return JSON_TYPE::J_ARRAY;
	else if (*ptr == '"')
		return JSON_TYPE::J_STRING;
	else if (*ptr <= '9' && *ptr >= '0')
		return JSON_TYPE::J_NUM;
	else if (*ptr == '-' && ptr[1] <= '9' && ptr[1] >= '0')
		return JSON_TYPE::J_NUM;
	else if (strncasecmp(ptr, "true", 4) == 0 || strncasecmp(ptr, "false", 5) == 0)
		return JSON_TYPE::J_BOOL;
	else if (strncasecmp(ptr, "null", 4) == 0)
		return JSON_TYPE::J_NULL_TYPE;
	else
		return JSON_TYPE::J_INVALID_TYPE;
}
jsonValue::jsonValue(JSON_TYPE _t) :t(_t) {}

dsStatus& jsonValue::parse(jsonValue*& value, const char* data, int& size)
{
	jsonValue* v = nullptr;
	switch (getType(data))
	{
	case JSON_TYPE::J_NUM:
		v = new jsonNum();
		break;
	case JSON_TYPE::J_STRING:
		v = new jsonString();
		break;
	case JSON_TYPE::J_OBJECT:
		v = new jsonObject();
		break;
	case JSON_TYPE::J_ARRAY:
		v = new jsonArray();
		break;
	case JSON_TYPE::J_BOOL:
		v = new jsonBool();
		break;
	case JSON_TYPE::J_NULL_TYPE:
	{
		const char* ptr = data;
		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		value = nullptr;
		size = ptr - data + 4;
		dsOk();
	}
	default:
		dsFailed(-1, "unpexpect type");
	}
	if (!dsCheck(v->parse(data, size)))
	{
		delete v;
		dsReturn(getLocalStatus());
	}
	else
	{
		value = v;
		dsOk();
	}
}

jsonString::jsonString(const char* data) :
	jsonValue(JSON_TYPE::J_STRING)
{
	if (data == nullptr)
		return;
	int size = 0;
	parse(data, size);
}
string jsonString::toString(int level) const
{
	string s;
	for (int l = 0; l < level; l++)
		s.append("\t");
	s.append("\"");
	s.append(m_value).append("\"");
	return s;
}
dsStatus& jsonString::parse(const char* data, int& size)
{
	size = 0;
	if (data == nullptr)
	{
		dsFailed(-1, "data is null");
	}
	m_value.clear();
	const char* ptr = data;
	string value;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (ptr[0] != '"')
		dsFailed(-1, "expect [\"] @ " << string(ptr, min(strlen(ptr), 100)));
	const char* e = strchr(ptr + 1, '"');
	if (e == nullptr)
		dsFailed(-1, "expect [\"] @ " << string(ptr + 1, min(strlen(ptr + 1), 50)));
	m_value.assign(ptr + 1, e - ptr - 1);
	size = (int)(e - data + 1);
	dsOk();
}
jsonNum::jsonNum(const char* data) :
	jsonValue(JSON_TYPE::J_NUM), m_value(0)
{
	if (data == nullptr)
		return;
	int size = 0;
	parse(data, size);
}
dsStatus& jsonNum::parse(const char* data, int& size)
{
	size = 0;
	m_value = 0;
	bool flag = true;
	if (data == nullptr)
		dsFailed(-1, "data is null");
	const char* ptr = data;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (*ptr == '-')
	{
		flag = false;
		ptr++;
	}
	if (*ptr > '9' || *ptr < '0')
		dsFailed(-1, "expect [0-9] @ " << string(ptr, min(strlen(ptr), 100)));
	while (*ptr <= '9' && *ptr >= '0')
	{
		m_value *= 10;
		m_value += ptr[0] - '0';
		ptr++;
	}
	if (!flag)
		m_value = -m_value;
	size = (int)(ptr - data);
	dsOk();
}
string jsonNum::toString(int level)const
{
	string s;
	for (int l = 0; l < level; l++)
		s.append("\t");
	char buf[32] = { 0 };
	sprintf(buf, "%ld", m_value);
	s.append(buf);
	return s;
}
jsonObject::jsonObject(const char* data) :
	jsonValue(JSON_TYPE::J_OBJECT)
{
	if (data == nullptr)
		return;
	int size = 0;
	parse(data, size);
}
jsonObject::~jsonObject()
{
	clean();
}
const jsonValue* jsonObject::get(const std::string& s)const
{
	std::map<std::string, jsonValue*>::const_iterator iter = m_values.find(s);
	if (iter == m_values.end())
		return nullptr;
	return iter->second;
}
const jsonValue* jsonObject::get(const char* s) const
{
	std::map<std::string, jsonValue*>::const_iterator iter = m_values.find(s);
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
dsStatus& jsonObject::parse(const char* data, int& size)
{
	if (data == nullptr)
		dsFailed(-1, "data is null");
	clean();
	const char* ptr = data;
	string value;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (ptr[0] != '{')
		dsFailed(-1, "expect [{] @ " << string(ptr, min(strlen(ptr), 100)));
	ptr++;
	while (*ptr != 0)
	{
		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		if (*ptr == '}')
			break;
		jsonString k;
		int childSize = 0;
		dsReturnIfFailed(k.parse(ptr, childSize));
		ptr += childSize;
		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		if (*ptr != ':')
		{
			clean();
			dsFailed(-1, "expect [:] @ " << string(ptr, min(strlen(ptr), 100)));
		}
		ptr++;
		jsonValue* v = nullptr;
		if (!dsCheck(jsonValue::parse(v, ptr, childSize)))
		{
			clean();
			dsReturn(getLocalStatus());
		}
		if (!m_values.insert(std::pair<std::string, jsonValue*>(k.m_value, v)).second)
		{
			clean();
			dsFailed(-1, "dupilcate key @ " << string(ptr, min(strlen(ptr), 100)));
		}
		ptr += childSize;
		std::pair<std::string, jsonValue*> kv(k.m_value.c_str(), v);
		m_valueList.push_back(kv);

		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		if (*ptr == '}')
			break;
		else if (*ptr != ',')
		{
			clean();
			dsFailed(-1, "expect [,] @ " << string(ptr, min(strlen(ptr), 100)));
		}
		ptr++;
	}
	size = (int)(ptr - data + 1);
	dsOk();
}
string jsonObject::toString(int level)const
{
	string s;
	for (int l = 0; l < level; l++)
		s.append("\t");
	s.append("{\n");
	bool first = true;
	for (std::map<std::string, jsonValue*>::const_iterator i = m_values.begin(); i != m_values.end(); i++)
	{
		jsonValue* v = i->second;
		if (v != nullptr)
		{
			if (!first)
				s.append(",\n");
			else
				first = false;
			if (v->t != JSON_TYPE::J_ARRAY && v->t != JSON_TYPE::J_OBJECT)
			{
				for (int l = 0; l <= level; l++)
					s.append("\t");
				s.append("\"").append(i->first).append("\":").append(v->toString());
			}
			else
			{
				for (int l = 0; l <= level; l++)
					s.append("\t");
				s.append("\"").append(i->first).append("\":\n");
				s.append(v->toString(level + 1));
			}
		}
	}
	s.append("\n");
	for (int l = 0; l < level; l++)
		s.append("\t");
	s.append("}");
	return s;
}
jsonArray::jsonArray(const char* data) :
	jsonValue(JSON_TYPE::J_ARRAY)
{
	if (data == nullptr)
		return;
	int size = 0;
	parse(data, size);
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
		if (*i != nullptr)
		{
			delete (*i);
		}
	}
	m_values.clear();
}

dsStatus& jsonArray::parse(const char* data, int& size)
{
	if (data == nullptr)
		dsFailed(-1, "data is null");
	clean();
	const char* ptr = data;
	string value;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (ptr[0] != '[')
		dsFailed(-1, "expect '[' @ " << string(ptr, min(strlen(ptr), 100)));
	ptr++;
	while (*ptr != 0)
	{
		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		if (*ptr == ']')
			break;
		int childSize = 0;
		jsonValue* v = nullptr;
		if (!dsCheck(jsonValue::parse(v, ptr, childSize)))
		{
			clean();
			dsReturn(getLocalStatus());
		}
		m_values.push_back(v);
		ptr += childSize;
		while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
			ptr++;
		if (*ptr == ']')
			break;
		else if (*ptr != ',')
		{
			clean();
			dsFailed(-1, "expect [,] @ " << string(ptr, min(strlen(ptr), 100)));
		}
		ptr++;
	}
	size = (int)(ptr - data + 1);
	dsOk();
}
string jsonArray::toString(int level)const
{
	string s;
	for (int l = 0; l < level; l++)
		s.append("\t");
	s.append("[\n");
	bool first = true;
	for (list<jsonValue*>::const_iterator i = m_values.begin(); i != m_values.end(); i++)
	{
		jsonValue* v = *i;
		if (v != nullptr)
		{
			if (!first)
				s.append(",\n");
			else
				first = false;
			s += v->toString(level + 1);
		}
	}
	s.append("\n");
	for (int l = 0; l < level; l++)
		s.append("\t");
	s.append("]");
	return s;
}
jsonBool::jsonBool(const char* data) :jsonValue(JSON_TYPE::J_BOOL), m_value(false)
{
	if (data == nullptr)
		return;
	int size = 0;
	parse(data, size);
}
dsStatus& jsonBool::parse(const char* data, int& size)
{
	if (data == nullptr)
	{
		m_value = false;
		dsFailed(-1, "data is null");
	}
	const char* ptr = data;
	while (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == '\r')
		ptr++;
	if (strncasecmp(ptr, "true", 4) == 0)
	{
		m_value = true;
		size = (int)(4 + ptr - data);
		dsOk();
	}
	else if (strncasecmp(ptr, "false", 5) == 0)
	{
		m_value = false;
		size = (int)(5 + ptr - data);
		dsOk();
	}
	else
		dsFailed(-1, "expect [true] or [false] @ " << string(ptr, min(strlen(ptr), 100)));

}
string jsonBool::toString(int level)const
{
	return m_value ? "true" : "false";
}


