/*
 * json_binary_to_string.h
 *
 *  Created on: 2018年4月2日
 *      Author: liwei
 */

#ifndef MYSQL_FUNCS_JSON_BINARY_TO_STRING_H_
#define MYSQL_FUNCS_JSON_BINARY_TO_STRING_H_
#include<stdint.h>
namespace json_binary
{
 class Value;
}
bool json_to_str(json_binary::Value * value,char * str,uint32_t &size);


#endif /* MYSQL_FUNCS_JSON_BINARY_TO_STRING_H_ */
