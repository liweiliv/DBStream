/*
 * file_opt.h
 *
 *  Created on: 2017年2月14日
 *      Author: liwei
 */

#ifndef LIB_UTIL_FILE_OPT_H_
#define LIB_UTIL_FILE_OPT_H_
#include <stdint.h>
#ifdef __cplusplus
extern "C"
{
#endif
int64_t file_read(int fd, unsigned char *buf, uint64_t count);
int64_t file_write(int fd,unsigned char *buf, uint64_t count);
/*
 * -1 文件存在
 * 0 文件不存在
 * 其他为错误号
 */
int check_file_exist(const char *filename);
#ifdef __cplusplus
}
#endif

#endif /* LIB_UTIL_FILE_OPT_H_ */
