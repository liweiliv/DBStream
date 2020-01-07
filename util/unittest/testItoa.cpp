/*
 * testItoa.cpp
 *
 *  Created on: 2020年1月7日
 *      Author: liwei
 */

#include "util/itoaSse.h"
#include <string.h>
#include <assert.h>
#define initBuffer(buffer)do{\
		for(int i=0;i<sizeof(buffer)-1;i++)\
			buffer[i] = '0';\
		buffer[sizeof(buffer)-1]=1;\
}while(0);
int testitoab()
{
	char buffer[13];
	initBuffer(buffer);
	int v = 12345;
	int len = i32toa_sse2b(v,&buffer[12]);
	assert(len==6);
	assert(strcmp(buffer,"000000012345")==0);
	initBuffer(buffer);

	v = 0;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==2);
	assert(strcmp(buffer,"000000000000")==0);

	v = 1;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==2);
	assert(strcmp(buffer,"000000000001")==0);

	v = 12;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==3);
	assert(strcmp(buffer,"000000000012")==0);

	v = 123;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==4);
	assert(strcmp(buffer,"000000000123")==0);


	v = 1234;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==5);
	assert(strcmp(buffer,"000000001234")==0);

	v = 12345;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==6);
	assert(strcmp(buffer,"000000012345")==0);

	v = 123456;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==7);
	assert(strcmp(buffer,"000000123456")==0);

	v = 1234567;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==8);
	assert(strcmp(buffer,"000001234567")==0);

	v = 12345678;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==9);
	assert(strcmp(buffer,"000012345678")==0);

	v = 123456789;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==10);
	assert(strcmp(buffer,"000123456789")==0);

	v = 1234567891;
	len = i32toa_sse2b(v,&buffer[12]);
	assert(len==11);
	assert(strcmp(buffer,"001234567891")==0);

	return 0;
}
int main()
{
	testitoab();
}

