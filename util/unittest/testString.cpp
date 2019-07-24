/*
 * testString.cpp
 *
 *  Created on: 2019年7月24日
 *      Author: liwei
 */
#include "util/String.h"
#include <stdio.h>
#include <assert.h>
void test()
{
	std::String s("abcd");
	assert(s=="abcd");
	std::String s1 ;
	s1 = s<<123;
	s1 = s1<<43254u<<5323ul<<432l<<std::string("dwad");
	assert(s1=="abcd123432545323432dwad");
}
int main()
{
	test();
}


