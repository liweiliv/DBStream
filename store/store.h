/*
 * store.h
 *
 *  Created on: 2019年2月13日
 *      Author: liwei
 */

#ifndef STORE_H_
#define STORE_H_
#include "../util/config.h"
#include <glog/logging.h>
namespace STORE{
#define MAIN_STREAM "mainStream"
#define GENERATED_STREAM "generatedStream"
class blockManager;
class schedule;
class store{
private:
    schedule *m_schedule;
    blockManager *m_mainStreamblockManager;
	blockManager *m_genratedStreamBlockManager;
    config * m_conf;
public:
	store(config* conf);
	int start();
	std::string updateConfig(const char* key, const char* value);
};



}
#endif /* STORE_H_ */
