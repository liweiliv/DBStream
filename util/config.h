/*
 * config.h
 *
 *  Created on: 2019年1月18日
 *	  Author: liwei
 */

#pragma once
#include <map>
#include <string>
#include "thread/shared_mutex.h"
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <limits.h>
#include "itoaSse.h"
#include "glog/logging.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
typedef spp::sparse_hash_map<std::string, std::string, StrHash, StrCompare> SECTION_TYPE;
typedef spp::sparse_hash_map<std::string, SECTION_TYPE*, StrHash, StrCompare> CONFIG_TYPE;
class config {
private:
	CONFIG_TYPE m_sections;
	std::string m_filePath;
	shared_mutex m_lock;
public:
	static bool char2num(const char* str, long& num)
	{
		num = 0;
		while (*(str) <= '9' && *(str) >= 0)
		{
			num = num * 10 + (*str) - '0';
			str++;
		}
		if (*str != '\0')
			return false;
		return true;
	}
	config(const char* path = nullptr) :m_filePath(path == nullptr ? "" : path)
	{
	}
	~config()
	{
		for (CONFIG_TYPE::iterator siter = m_sections.begin(); siter != m_sections.end(); siter++)
		{
			siter->second->clear();
			delete siter->second;
		}
		m_sections.clear();
	}
	static bool checkNumberString(const char* num, int64_t max, int64_t min)
	{
		for (int idx = strlen(num) - 1; idx >= 0; idx--)
		{
			if (num[idx] > '9' || num[idx] < '0')
				return false;
		}
		int64_t _num = atol(num);
		if (_num > max || _num < min)
			return false;
		return true;
	}
	int64_t getLong(const char* section, const char* key, int64_t defaultValue, int64_t min, int64_t max)
	{
		assert(defaultValue <= max && defaultValue >= min && min <= max);
		std::string c = get(section, key, NULL);
		if (c.empty())
			return defaultValue;
		int64_t v = atol(c.c_str());
		if (v < min)
			return min;
		if (v > max)
			return max;
		return v;
	}
	std::string get(const char* section, const char* key, const char* defaultValue = NULL)
	{
		m_lock.lock_shared();
		CONFIG_TYPE::const_iterator secIter = m_sections.find(section);
		if (secIter == m_sections.end())
		{
			m_lock.unlock_shared();
			return defaultValue ? defaultValue : std::string();
		}
		SECTION_TYPE::const_iterator kvIter = secIter->second->find(key);
		if (kvIter == secIter->second->end())
		{
			m_lock.unlock_shared();
			return defaultValue ? defaultValue : std::string();
		}
		else
		{
			std::string _value = kvIter->second;
			m_lock.unlock_shared();
			return _value;
		}
	}
	void set(const char* section, const char* key, const char* value)
	{
		SECTION_TYPE* sec = nullptr;
		m_lock.lock();
		CONFIG_TYPE::iterator secIter = m_sections.find(section);
		if (secIter == m_sections.end())
		{
			sec = new SECTION_TYPE();
			m_sections.insert(std::pair<std::string, SECTION_TYPE*>(section, sec));
		}
		else
			sec = secIter->second;
		sec->insert(std::pair<std::string, std::string>(key, value));
		m_lock.unlock();
	}
	static std::string trim(const char* str, size_t size)
	{
		const char* start = str, * end = str + size - 1;
		while (start - str < (uint32_t)size && (*start == '\t' || *start == ' ' || *start == '\n' || *start == '\r'))
			start++;
		while (end >= start && (*end == '\t' || *end == ' ' || *end == '\n' || *end == '\r'))
			end--;
		if (end < start)
			return std::string();
		else
			return std::string(start, end - start + 1);
	}
	int load()
	{
		if (m_filePath.empty())
		{
			LOG(ERROR) << "parameter filePath is empty ,load config failed";
			return -1;
		}
		FILE* fp = fopen(m_filePath.c_str(), "r");
		if (fp == NULL)
		{
			LOG(ERROR) << "open filePath  " << m_filePath << " failed for: " << errno << " , " << strerror(errno);
			return -1;
		}
		char buf[1024] = { 0 };
		char* hashtag;
		std::string section;
		while (!feof(fp))
		{
			if (NULL == fgets(buf, 1023, fp))
				break;
			/*trim and filter data after [#]*/
			while ((hashtag = strchr(buf, '#')) != NULL)
			{
				if (hashtag != &buf[0] && *(hashtag - 1) == '\\' && (hashtag < &buf[2] || *(hashtag - 2) != '\\'))//[\#] is Escape
					continue;
				*hashtag = '\0';
				break;
			}
			int16_t size = strlen(buf);
			std::string line = trim(buf, size);
			if (line.empty())
				continue;
			size_t equalPos = line.find('=');
			if (equalPos == std::string::npos)//section
			{
				if (line.c_str()[0] != '[' || line.size() <= 2 || line.c_str()[line.size() - 1] != ']')
				{
					fclose(fp);
					return -1;
				}
				section = line.substr(1, line.size() - 2);
				continue;
			}
			else //key and value
			{
				std::string key = trim(line.c_str(), equalPos);
				std::string value = trim(line.c_str() + equalPos + 1, line.size() - equalPos);
				set(section.c_str(), key.c_str(), value.c_str());
			}

		}
		fclose(fp);
		return 0;
	}
	int save(const char* file)
	{
		remove(file);
		FILE* fp = fopen(file, "w+");
		if (fp == nullptr)
		{
			LOG(ERROR) << "save conf to file" << file << " failed for open file failed,errno" << errno << "," << strerror(errno);
			return -1;
		}
		for (CONFIG_TYPE::const_iterator secIter = m_sections.begin(); secIter != m_sections.end(); secIter++)
		{
			if (secIter->first.size() != fwrite(secIter->first.c_str(), 1, secIter->first.size(), fp) || 1 != fwrite("\n", 1, 1, fp))
			{
				LOG(ERROR) << "save conf to file" << file << " failed for write file failed,errno" << errno << "," << strerror(errno);
				return -1;
			}
			for (SECTION_TYPE::const_iterator confIter = secIter->second->begin(); confIter != secIter->second->end(); confIter++)
			{
				if (confIter->first.size() != fwrite(confIter->first.c_str(), 1, confIter->first.size(), fp) ||
					3 != fwrite(" = ", 1, 3, fp) ||
					confIter->second.size() != fwrite(confIter->second.c_str(), 1, confIter->second.size(), fp)
					|| 1 != fwrite("\n", 1, 1, fp))
				{
					LOG(ERROR) << "save conf to file" << file << " failed for write file failed,errno" << errno << "," << strerror(errno);
					return -1;
				}
			}
		}
		fclose(fp);
		return 0;
	}
private:
	template<typename T>
	static std::string getIntConf(const char* str, T& value, bool canBeNegative)
	{
		std::string conf = trim(str, strlen(str));
		const char* pos = conf.c_str();
		bool positive = str[0] != '-';
		if (!positive)
		{
			if (canBeNegative)
				pos++;
			else
				return conf + " is negative number";
		}
		T tmpValue = 0;
		while (*pos != '\0')
		{
			if (*pos < '0' || *pos>'9')
			{
				if (*(pos + 1) == '\0')
				{
					if (*pos == 'k' || *pos == 'K')
					{
						//overflow
						if (tmpValue * 1024 < tmpValue)
							return conf + " is to large";
						tmpValue *= 1024;
						break;
					}
					else if (*pos == 'm' || *pos == 'M')
					{
						//overflow
						if (tmpValue * 1024 * 1024 < tmpValue)
							return conf + " is to large";
						tmpValue *= 1024 * 1024;
						break;
					}
					else if (*pos == 'g' || *pos == 'G')
					{
						//overflow
						if (tmpValue * 1024 * 1024 * 1024 < tmpValue)
							return conf + " is to large";
						tmpValue *= 1024 * 1024 * 1024;
						break;
					}
					else if (*pos == 't' || *pos == 'T')
					{
						//overflow
						if (tmpValue * 1024LL * 1024LL * 1024LL * 1024LL < tmpValue)
							return conf + " is to large";
						tmpValue *= 1024LL * 1024LL * 1024LL * 1024LL;
						break;
					}
					else if (*pos == 'p' || *pos == 'P')
					{
						//overflow
						if (tmpValue * 1024LL * 1024LL * 1024LL * 1024LL * 1024LL < tmpValue)
							return conf + " is to large";
						tmpValue *= 1024LL * 1024LL * 1024LL * 1024LL * 1024LL;
						break;
					}
				}
				return conf + " is not number";
			}
			//overflow
			if (tmpValue * 10 + (*pos) - '0' < tmpValue)
				return conf + " is to large";
			tmpValue = tmpValue * 10 + (*pos) - '0';
			pos++;
		}
		value = positive ? tmpValue : -tmpValue;
		return "";
	}
public:
	static std::string getInt32(const char* str, int32_t& value, int32_t min = INT32_MIN, int32_t max = INT32_MAX)
	{
		int32_t tmpValue;
		std::string  rtv = getIntConf(str, tmpValue, true);
		if (!rtv.empty())
			return rtv;
		if (tmpValue > max)
		{
			char buf[32];
			i32toa_sse2(max, buf);
			return std::string(str) + " is greater than max value " + buf;
		}
		if (tmpValue < min)
		{
			char buf[32];
			i32toa_sse2(min, buf);
			return std::string(str) + " is less than min value " + buf;
		}
		value = tmpValue;
		return "";
	}
	static std::string getUint32(const char* str, uint32_t& value, uint32_t min = 0, uint32_t max = UINT32_MAX)
	{
		uint32_t tmpValue;
		std::string rtv = getIntConf(str, tmpValue, false);
		if (!rtv.empty())
			return rtv;
		if (tmpValue > max)
		{
			char buf[32];
			u32toa_sse2(max, buf);
			return std::string(str) + " is greater than max value " + buf;
		}
		if (tmpValue < min)
		{
			char buf[32];
			u32toa_sse2(min, buf);
			return std::string(str) + " is less than min value " + buf;
		}
		value = tmpValue;
		return "";
	}
	static std::string getInt64(const char* str, int64_t& value, int64_t min = INT64_MIN, int64_t max = INT64_MAX)
	{
		int64_t tmpValue;
		std::string rtv = getIntConf(str, tmpValue, true);
		if (!rtv.empty())
			return rtv;
		if (tmpValue > max)
		{
			char buf[40];
			i64toa_sse2(max, buf);
			return std::string(str) + " is greater than max value " + buf;
		}
		if (tmpValue < min)
		{
			char buf[40];
			i64toa_sse2(min, buf);
			return std::string(str) + " is less than min value " + buf;
		}
		value = tmpValue;
		return "";
	}
	static std::string getUint64(const char* str, uint64_t& value, uint64_t min = 0, uint64_t max = UINT64_MAX)
	{
		uint64_t tmpValue;
		std::string rtv = getIntConf(str, tmpValue, false);
		if (!rtv.empty())
			return rtv;
		if (tmpValue > max)
		{
			char buf[40];
			u64toa_sse2(max, buf);
			return std::string(str) + " is greater than max value " + buf;
		}
		if (tmpValue < min)
		{
			char buf[40];
			u64toa_sse2(min, buf);
			return std::string(str) + " is less than min value " + buf;
		}
		value = tmpValue;
		return "";
	}
};

