#include "meta/columnType.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
int test()
{
	char tstr[128] = { 0 }, tstrd[128] = { 0 };

	META::Timestamp timestamp;
	timestamp.seconds = 0x3ffffffffULL;
	timestamp.nanoSeconds = 999999999;
	assert(timestamp.seconds == 0x3ffffffffULL);
	assert(timestamp.nanoSeconds == 999999999);
	sprintf(tstr, "%llu.%u", 0x3ffffffffULL, 999999999/1000);
	timestamp.toString(tstrd);
	assert(strcmp(tstr, tstrd) == 0);

	META::DateTime datetime;
	datetime.set(0x1ffff, 12, 31, 24, 60, 60, 999999);
	assert(datetime.year = 0x1ffff);
	assert(datetime.month = 12);
	assert(datetime.day = 31);
	assert(datetime.hour = 24);
	assert(datetime.min = 60);
	assert(datetime.sec = 60);
	assert(datetime.usec = 999999);
	sprintf(tstr, "%d-%u-%u %u:%u:%u.%u", 0x1ffff, 12,31,24,60,60, 999999);
	datetime.toString(tstrd);
	assert(strcmp(tstr, tstrd) == 0);


	META::Time time;
	time.set(1234567, 60, 60, 999999999);
	assert(time.hour = 1234567);
	assert(time.min = 60);
	assert(time.sec = 60);
	assert(time.nsec = 999999999);
	sprintf(tstr, "%d:%d:%d.%u", 1234567,60,60, 999999999 / 1000);
	time.toString(tstrd);
	assert(strcmp(tstr, tstrd) == 0);

	META::Date date;
	date.set(1234567, 12, 31);
	assert(date.year = 1234567);
	assert(date.month = 12);
	assert(date.day = 31);
	sprintf(tstr, "%d-%d-%d", 1234567, 12, 31);
	date.toString(tstrd);
	assert(strcmp(tstr, tstrd) == 0);
	return 0;
}
int main()
{
	test();
}
