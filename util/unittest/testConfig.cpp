#include "../config.h"
int main()
{
	config conf(nullptr);
	conf.set("a","test1","123456");
	conf.set("A","test1","1234567");
	conf.set("a","test12","12345678");
	conf.set("a","Test1","23456");
	conf.set("a","word","drow");
	assert(strcmp(conf.get("a","test1").c_str(),"123456")==0);
	assert(strcmp(conf.get("A","test1").c_str(),"1234567")==0);
	assert(strcmp(conf.get("a","test12").c_str(),"12345678")==0);
	assert(strcmp(conf.get("a","Test1").c_str(),"23456")==0);
	assert(strcmp(conf.get("a","word").c_str(),"drow")==0);
	assert(conf.getLong("A","test1",0,0,65535)==65535);
	assert(conf.getLong("A","test1",0,0,6553500)==1234567);
	conf.save("1.cnf");
}
