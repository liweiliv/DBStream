/*
 * store.h
 *
 *  Created on: 2019年2月13日
 *      Author: liwei
 */

#ifndef STORE_H_
#define STORE_H_
#include "schedule.h"
#include "blockManager.h"
#include "../util/config.h"
#include <glog/logging.h>
namespace STORE{
#define MAIN_STREAM "mainStream"
#define GENERATED_STREAM "generatedStream"
class record;
class store{
private:
    schedule m_schedule;
    blockManager m_mainStreamblockManager;
	blockManager m_genratedStreamBlockManager;
    config * m_conf;
public:
    store(config * conf):m_schedule(conf), m_mainStreamblockManager(MAIN_STREAM,conf), m_genratedStreamBlockManager(GENERATED_STREAM,conf),m_conf(conf)
    {

    }
    int start()
    {
        if(0!=m_schedule.start())
        {
            LOG(ERROR)<<"schedule module start failed";
            return -1;
        }
        if(0!= m_mainStreamblockManager.start())
        {
            LOG(ERROR)<<"blockManager module start failed";
            m_schedule.stop();
            return -1;
        }
		if (0 != m_genratedStreamBlockManager.start())
		{
			LOG(ERROR) << "m_genratedStreamBlockManager module start failed";
			m_schedule.stop();
			return -1;
		}
        return 0;
    }
    std::string updateConfig(const char *key,const char * value)
    {
        if(strncmp(key,C_SCHEDULE ".",sizeof(C_SCHEDULE))==0)
            return m_schedule.updateConfig(key,value);
        else if (strncmp(key, MAIN_STREAM ".",sizeof(MAIN_STREAM))==0)
            return m_mainStreamblockManager.updateConfig(key,value);
		else if (strncmp(key, GENERATED_STREAM ".", sizeof(GENERATED_STREAM)) == 0)
			return m_genratedStreamBlockManager.updateConfig(key, value);
        else
            return std::string("unknown config:")+key;
    }
};



}
#endif /* STORE_H_ */
