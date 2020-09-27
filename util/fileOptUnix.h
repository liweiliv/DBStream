/*
 * fileOpt.h
 *
 *  Created on: 2019年1月23日
 *      Author: liwei
 */
#pragma once
#ifdef OS_LINUX
#include <errno.h>
#include <error.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#include <dirent.h>
#include <string.h>
#define fileHandle int
static fileHandle openFile(const char *file, bool readFlag, bool writeFlag, bool createFlag)
{
	int fd = -1;
	int flag = 0;
	if (readFlag && writeFlag)
		flag = O_RDWR;
	else if (writeFlag)
		flag = O_WRONLY;
	if (createFlag)
		flag |= O_CREAT;
	fd = open(file, flag, createFlag ? S_IRUSR | S_IWUSR | S_IRGRP : 0);
	return fd;
}
#define INVALID_HANDLE_VALUE -1
static bool fileHandleValid(fileHandle fd)
{
	return fd >=0;
}
static inline int64_t readFile(fileHandle fd, char *buf, uint64_t count)
{
    int64_t readbytes;
    uint64_t  save_count=0;
    for (;;)
    {
        errno= 0;
        readbytes = read(fd, buf+save_count, count-save_count);
        if (readbytes != (int64_t)(count-save_count))
        {
            if ((readbytes == 0 || (int) readbytes == -1)&&errno == EINTR)
                continue; /* Interrupted */

            if (readbytes == -1)
                return -errno; /* Return with error */
            else if(readbytes==0)
                return save_count;
            else
                save_count += readbytes;
        }
        else
        {
            save_count += readbytes;
            break;
        }
    }
    return save_count;
}
static inline  int64_t writeFile(fileHandle fd,const char *buf, size_t count)
{
    uint64_t writebytes, save_count=0;
    for (;;)
    {
        errno= 0;
        writebytes = write(fd, buf+save_count, count-save_count);
        if (writebytes != count-save_count)
        {
            if ((writebytes == 0 || (int) writebytes == -1)&&errno == EINTR)
                continue; /* Interrupted */
            if (writebytes == (size_t) -1)
                return -errno; /* Return with error */
            else if(writebytes==0)
                return save_count;
            else
                save_count += writebytes;
        }
        else
        {
            save_count += writebytes;
            break;
        }
    }
    return save_count;
}
static int truncateFile(fileHandle fd, uint64_t offset)
{
	return ftruncate(fd, offset);
}
static int closeFile(fileHandle fd)
{
	return close(fd);
}
/*
 * -1 文件存在
 * 0 文件不存在
 * 其他为错误号
 */
static inline  int checkFileExist(const char *fileName, int mode)
{
	return access(fileName, mode);
}
#define seekFile lseek64
static long getFileTime(const char * file)
{
	fileHandle fd = openFile(file, true, false, false);
	if (fd < 0)
		return -1;
	struct stat st;
	fstat(fd, &st);
	closeFile(fd);
	return st.st_mtime;
}
static int64_t getFileSize(fileHandle fd)
{
	struct stat stbuf;
	if ((fstat(fd, &stbuf) != 0) || (!S_ISREG(stbuf.st_mode)))
	{
		return -1;
	}
	return stbuf.st_size;
}
static int64_t getFileSize(const char* fileName)
{
	struct stat stbuf;
	fileHandle fd = openFile(fileName, true,false,false);
	if (fd == INVALID_HANDLE_VALUE)
		return -1;
	if ((fstat(fd, &stbuf) != 0) || (!S_ISREG(stbuf.st_mode))) 
	{
		closeFile(fd);
		return -1;
	}
	closeFile(fd);
	return stbuf.st_size;
}
static int getFileSizeAndTimestamp(const char* fileName,int64_t *size,int64_t* timestamp)
{
	*size = *timestamp = 0;
	fileHandle fd = openFile(fileName, true, false, false);
	if (fd == INVALID_HANDLE_VALUE)
		return -1;
	struct stat stbuf;
	if ((fstat(fd, &stbuf) != 0) || (!S_ISREG(stbuf.st_mode)))
	{
		closeFile(fd);
		return -1;
	}
	*size = stbuf.st_size;
	*timestamp = stbuf.st_mtime;
	closeFile(fd);
	return 0;
}
static int removeDir(const char * dir)
{
	char cur_dir[] = ".";
	char up_dir[] = "..";
	char dir_name[258];
	DIR *dirp;
	struct dirent *dp;
	struct stat dir_stat;

		// 参数传递进来的目录不存在，直接返回
		if ( 0 != access(dir, F_OK) ) {
			return 0;
		}

		// 获取目录属性失败，返回错误
		if ( 0 > stat(dir, &dir_stat) ) {
			perror("get directory stat error");
			return -1;
		}

		if ( S_ISREG(dir_stat.st_mode) ) {	// 普通文件直接删除
			remove(dir);
		} else if ( S_ISDIR(dir_stat.st_mode) ) {	// 目录文件，递归删除目录中内容
			dirp = opendir(dir);
			while ( (dp=readdir(dirp)) != NULL ) {
				// 忽略 . 和 ..
				if ( (0 == strcmp(cur_dir, dp->d_name)) || (0 == strcmp(up_dir, dp->d_name)) ) {
					continue;
				}

				sprintf(dir_name, "%s/%s", dir, dp->d_name);
				removeDir(dir_name);   // 递归调用
			}
			closedir(dirp);

			rmdir(dir);		// 删除空目录
		} else {
			perror("unknow file type!");
		}
		return 0;
}
#endif
