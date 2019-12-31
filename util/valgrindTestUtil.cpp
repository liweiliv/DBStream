/*
 * valgrindTestUtil.cpp
 *
 *  Created on: 2019年12月27日
 *      Author: liwei
 */
#include "file.h"
#ifdef OS_LINUX
static int fd = -1;
void vSave(const void * mem,size_t size)
{
	if(fd < 0)
		fd = open(".VLOG",O_RDWR|O_DIRECT|O_CREAT,S_IRUSR | S_IWUSR | S_IRGRP );
	write(fd,mem,size);
}
#endif


