/*
 * store.h
 *
 *  Created on: 2019年2月13日
 *      Author: liwei
 */

#ifndef STORE_H_
#define STORE_H_
#include "../util/config.h"
#include "../glog/logging.h"
class bufferPool;
namespace META{
class metaDataCollection;
}
namespace DATABASE_INCREASE {
	struct record;
}
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
	META::metaDataCollection* m_metaDataCollection;
	config * m_conf;
	bufferPool* m_bufferPool;
public:
	store(config* conf);
	int start();
	int stop();
	void begin();
	int insert(DATABASE_INCREASE::record* r);
	void commit();
	bool checkpoint(uint64_t& timestamp, uint64_t logOffset);
	std::string updateConfig(const char* key, const char* value);
};



}
#endif /* STORE_H_ */
