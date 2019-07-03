/*
 * config.h
 *
 *  Created on: 2019年1月18日
 *      Author: liwei
 */

#ifndef CONFIG_H_
#define CONFIG_H_
#include <map>
#include <string>
#include "shared_mutex.h"
#include <stdio.h>
#include <errno.h>
#include "../glog/logging.h"
class config{
private:
    std::map<std::string, std::map<std::string,std::string>* > m_sections;
    std::string m_filePath;
    shared_mutex m_lock;
public:
	config(const char * path):m_filePath(path)
	{
	}
	~config()
	{
		for (std::map<std::string, std::map<std::string, std::string>* >::iterator iter = m_sections.begin(); iter != m_sections.end(); iter++)
		{
			delete iter->second;
		}
		m_sections.clear();
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
        std::map<std::string, std::map<std::string,std::string>* >::const_iterator secIter = m_sections.find(section);
        if(secIter == m_sections.end())
        {
			m_lock.unlock_shared();
            return defaultValue?defaultValue:std::string();
        }
        std::map<std::string,std::string>::const_iterator kvIter = secIter->second->find(key);
        if(kvIter == secIter->second->end())
        {
			m_lock.unlock_shared();
            return defaultValue?defaultValue:std::string();
        }
        else
        {
            std::string value = kvIter->second;
			m_lock.unlock_shared();
            return value;
        }
    }
    void set(const char* section,const char* key,const char *value)
    {
		m_lock.lock();
        std::map<std::string, std::map<std::string,std::string>* >::iterator secIter = m_sections.find(section);
        if(secIter == m_sections.end())
            secIter = m_sections.insert(
                    std::pair<std::string, std::map<std::string,std::string>* >(section,new std::map<std::string,std::string>())).first;
        std::map<std::string,std::string>::iterator kvIter = secIter->second->find(key);
        if(kvIter == secIter->second->end())
            secIter->second->insert(std::pair<std::string,std::string>(key,value));
        else
            kvIter->second = value;
		m_lock.unlock();
    }
    static std::string trim(const char * str,size_t size)
    {
        const char *start = str,*end = str+size-1;
        while(start-str<(uint32_t)size&&(*start=='\t'||*start==' '||*start=='\n'))
            start++;
        while(end>=start&&(*end=='\t'||*end==' '||*end=='\n'))
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
        char * equal,*hashtag;
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

            if(NULL==(equal = strstr(line.c_str(),"=")))//section
            {
                section = line;
                continue;
            }
            else //key and value
            {
                std::string key = trim(line.c_str(),equal-line.c_str()-1);
                std::string value = trim(equal+1,line.size()-(equal-line.c_str()+1));
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
		for (std::map<std::string, std::map<std::string, std::string>* >::iterator secIter = m_sections.begin(); secIter != m_sections.end(); secIter++)
		{
			if (secIter->first.size() != fwrite(secIter->first.c_str(),1, secIter->first.size(), fp)|| 1!=fwrite("\n", 1,1, fp))
			{
				LOG(ERROR) << "save conf to file" << file << " failed for write file failed,errno" << errno << "," << strerror(errno);
				return -1;
			}
			for (std::map<std::string, std::string>::iterator confIter = secIter->second->begin(); confIter != secIter->second->end(); confIter++)
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
