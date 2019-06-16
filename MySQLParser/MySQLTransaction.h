/*
 * MySQLTransaction.h
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */

#ifndef MYSQLTRANSACTION_H_
#define MYSQLTRANSACTION_H_


#include <stdint.h>

struct MySQLTransaction
{
	uint32_t recordCount;
	struct MySQLTransaction * next;
	void *records[1];
};
void clearMySQLTransaction(MySQLTransaction * trans);
struct  MySQLLogWrapper
{
	MySQLTransaction * transaction;
	uint64_t fileID;
	uint64_t offset;
	void * rawData;
	size_t rawDataSize;
};


#endif /* MYSQLTRANSACTION_H_ */
