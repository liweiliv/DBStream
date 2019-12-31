/*
 * valgrindTestUtil.h
 *
 *  Created on: 2019年12月27日
 *      Author: liwei
 */

#ifndef UTIL_VALGRINDTESTUTIL_H_
#define UTIL_VALGRINDTESTUTIL_H_
#ifdef OS_LINUX
#ifdef VLGRIND_TEST
void vSave(const void * mem,size_t size);
#else
#define vSave(mem,size)
#endif
#else
#define vSave(mem,size)
#endif




#endif /* UTIL_VALGRINDTESTUTIL_H_ */
