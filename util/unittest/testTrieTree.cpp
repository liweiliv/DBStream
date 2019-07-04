#include "../trieTree.h"
#include <assert.h>
#include <set>
#include <string>
#include <string.h>
#define assertCmp(s) assert(strcmp((char*)t.find((uint8_t*)(s)),(s))==0)
int main()
{
	trieTree t;
	std::set<std::string> s;
	t.insert((uint8_t*)"abc",(void*)"abc");
	s.insert("abc");
	t.insert((uint8_t*)"A",(void*)"A");
	s.insert("A");
	t.insert((uint8_t*)"abcd",(void*)"abcd");
	s.insert("abcd");
	t.insert((uint8_t*)"a",(void*)"a");
	s.insert("a");
	t.insert((uint8_t*)"123",(void*)"123");
	s.insert("123");
	t.insert((uint8_t*)"2345",(void*)"2345");
	s.insert("2345");
	t.insert((uint8_t*)"12345",(void*)"12345");
	s.insert("12345");
	t.insert((uint8_t*)"123456",(void*)"123456");
	s.insert("123456");
	t.insert((uint8_t*)"0123",(void*)"0123");
	s.insert("0123");
	assertCmp("abc");
	assertCmp("A");
	assertCmp("abcd");
	assertCmp("a");
	assertCmp("123");
	assertCmp("0123");
	assertCmp("2345");
	assertCmp("12345");
	assertCmp("123456");
	std::set<std::string>::iterator siter = s.begin();
	trieTree::iterator titer  = t.begin();
	for(;siter!=s.end();siter++)
	{
		assert((*siter).compare((const char*)titer.value())==0);
		assert((*siter).compare((const char*)titer.key())==0);
		titer.next();
	}
}
