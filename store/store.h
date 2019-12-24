/*
 * store.h
 *
 *  Created on: 2019年2月13日
 *      Author: liwei
 */

#ifndef STORE_H_
#define STORE_H_
#include "util/config.h"
#include "glog/logging.h"
#include "util/winDll.h"
class bufferPool;
namespace META{
class metaDataCollection;
}
namespace DATABASE_INCREASE {
	struct record;
}
namespace DATABASE {
	class database;
}
namespace STORE{
#define MAIN_STREAM "mainStream"
#define GENERATED_STREAM "generatedStream"
class schedule;
DLL_EXPORT class store{
private:
	schedule *m_schedule;
	DATABASE::database *m_mainStream;
	DATABASE::database *m_genratedStream;
	META::metaDataCollection* m_metaDataCollection;
	config * m_conf;
	bufferPool* m_bufferPool;
public:
	DLL_EXPORT store(config* conf);
	DLL_EXPORT ~store();

	DLL_EXPORT int start();
	DLL_EXPORT int stop();
	DLL_EXPORT void begin();
	DLL_EXPORT int insert(DATABASE_INCREASE::record* r);
	DLL_EXPORT void commit();
	DLL_EXPORT bool checkpoint(uint64_t& timestamp, uint64_t &logOffset);
	DLL_EXPORT std::string updateConfig(const char* key, const char* value);
};



}
#endif /* STORE_H_ */
