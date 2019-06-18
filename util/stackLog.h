/*
 * stackLog.h
 *
 *  Created on: 2017年9月20日
 *      Author: liwei
 */

#ifndef _STACKLOG_H_
#define _STACKLOG_H_
#include <string>
#include <string.h>
#include "winDll.h"
//#include <glog/logging.h>
#ifdef OS_WIN
static inline const char * basename(const char * path)
{
	const char * backSlant = strchr(path,'\\');
	if (backSlant == nullptr)
		return path;
	for (const char * nextBackSlant = strchr(backSlant + 1, '\\'); nextBackSlant != nullptr; nextBackSlant = strchr(backSlant + 1, '\\'))
		backSlant = nextBackSlant;
	return backSlant + 1;
}
#endif
DLL_EXPORT int initStackLog();
DLL_EXPORT int destroyStackLog();
DLL_EXPORT void cleanStackLog();
#define SET_STACE_LOG(code,...) setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__)
#define SET_STACE_LOG_AND_RETURN(rtv,code,...) setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__);return (rtv);
#define SET_STACE_LOG_AND_RETURN_(rtv,code,...) printf(__VA_ARGS__);printf("\n"); setStackLog(__LINE__,__func__,basename(__FILE__),code,__VA_ARGS__);return (rtv);

DLL_EXPORT void setStackLog(int codeLine,const char * func,const char * file,int code,const char * fmt,...);
DLL_EXPORT int getChildLogDetail(int &code,const char *&log);
DLL_EXPORT int getChildLog(std::string &errorLog);
DLL_EXPORT void  getFullStackLog(std::string &log);
#endif /* _STACKLOG_H_ */
