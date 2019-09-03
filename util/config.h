/*
 * config.h
 *
 *  Created on: 2019年1月18日
 *	  Author: liwei
 */

#ifndef CONFIG_H_
#define CONFIG_H_
#include <map>
#include <string>
#include "shared_mutex.h"
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include "glog/logging.h"
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
typedef spp::sparse_hash_map<std::string,std::string,StrHash,StrCompare> SECTION_TYPE;
typedef spp::sparse_hash_map<std::string,SECTION_TYPE*,StrHash,StrCompare> CONFIG_TYPE;
class config{
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
	config(const char * path):m_filePath(path==nullptr?"":path)
	{
	}
	~config()
	{
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
	std::string get(const char* section,const char* key,const char * defaultValue = NULL)
	{
		m_lock.lock_shared();
		CONFIG_TYPE::const_iterator secIter =  m_sections.find(section);
		if(secIter == m_sections.end())
		{
			m_lock.unlock_shared();
			return defaultValue?defaultValue:std::string();
		}
		SECTION_TYPE::const_iterator kvIter = secIter->second->find(key);
		if(kvIter == secIter->second->end())
		{
			m_lock.unlock_shared();
			return defaultValue?defaultValue:std::string();
		}
		else
		{
			std::string _value = kvIter->second;
			m_lock.unlock_shared();
			return _value;
		}
	}
	void set(const char* section,const char* key,const char *value)
	{
		char* _value = new char[strlen(value) + 1];
		strcpy(_value, value);
		SECTION_TYPE * sec = nullptr;
		m_lock.lock();
		CONFIG_TYPE::iterator secIter =  m_sections.find(section);
		if(secIter == m_sections.end())
		{
			sec  = new SECTION_TYPE();
			m_sections.insert(std::pair<std::string,SECTION_TYPE*>(section,sec));
		}
		else
			sec = secIter->second;
		sec->insert(std::pair<std::string,std::string>(key,value));
		m_lock.unlock();
	}
	static std::string trim(const char * str,size_t size)
	{
		const char *start = str,*end = str+size-1;
		while(start-str<(uint32_t)size&&(*start=='\t'||*start==' '||*start=='\n'||*start=='\r'))
			start++;
		while(end>=start&&(*end=='\t'||*end==' '||*end=='\n'||*end=='\r'))
			end--;
		if(end<start)
			return std::string();
		else
			return std::string(start,end-start+1);
	}
	int load()
	{
		if(m_filePath.empty())
		{
			LOG(ERROR)<<"parameter filePath is empty ,load config failed";
			return -1;
		}
		FILE * fp = fopen(m_filePath.c_str(),"r");
		if(fp == NULL)
		{
			LOG(ERROR)<<"open filePath  "<<m_filePath<<" failed for: "<<errno<<" , "<<strerror(errno);
			return -1;
		}
		char buf[1024] = {0};
		char * hashtag;
		std::string section;
		while(!feof(fp))
		{
			if(NULL == fgets(buf,1023,fp))
				break;
			/*trim and filter data after [#]*/
			while((hashtag = strchr(buf,'#'))!=NULL)
			{
				if(hashtag!=&buf[0]&&*(hashtag-1)=='\\'&&(hashtag<&buf[2]||*(hashtag-2)!='\\'))//[\#] is Escape
					continue;
				*hashtag = '\0';
				break;
			}
			int16_t size = strlen(buf);
			std::string line = trim(buf,size);
			if(line.empty())
				continue;
			size_t equalPos = line.find('=');
			if(equalPos==std::string::npos)//section
			{
				section = line;
				continue;
			}
			else //key and value
			{
				std::string key = trim(line.c_str(),equalPos);
				std::string value = trim(line.c_str()+equalPos+1,line.size()-equalPos);
				set(section.c_str(),key.c_str(),value.c_str());
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
			LOG(ERROR) << "save conf to file"<<file<<" failed for open file failed,errno"<<errno<<","<<strerror(errno);
			return -1;
		}
		for(CONFIG_TYPE::const_iterator secIter =  m_sections.begin();secIter!=m_sections.end();secIter++)
		{
			if (secIter->first.size() != fwrite(secIter->first.c_str(),1, secIter->first.size(), fp)|| 1!=fwrite("\n", 1,1, fp))
			{
				LOG(ERROR) << "save conf to file" << file << " failed for write file failed,errno" << errno << "," << strerror(errno);
				return -1;
			}
			for (SECTION_TYPE::const_iterator confIter = secIter->second->begin(); confIter!=secIter->second->end(); confIter++)
			{
				if (confIter->first.size() != fwrite(confIter->first.c_str(),1, confIter->first.size(), fp) ||
					3!= fwrite(" = ",1, 3, fp)||
					confIter->second.size() != fwrite(confIter->second.c_str(),1, confIter->second.size(), fp)
					||1 != fwrite("\n", 1,1, fp))
				{
					LOG(ERROR) << "save conf to file" << file << " failed for write file failed,errno" << errno << "," << strerror(errno);
					return -1;
				}
			}
		}
		fclose(fp);
		return 0;
	}
};
#endif /* CONFIG_H_ */

