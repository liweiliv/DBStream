/*
 * parseMem.h
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */

#ifndef PARSEMEM_H_
#define PARSEMEM_H_
#include <stdint.h>
struct _memp_ring;
class BinlogRecordImpl;
class parseMem
{
private:
	char * m_parseMem;
	uint64_t m_parseMemSize;
	uint64_t m_parseMemUsedSize;
	_memp_ring * m_mempool;
public:
	parseMem(_memp_ring * mempool);
	~parseMem();
	void reset();
	void init(BinlogRecordImpl * record);
	char * alloc(uint64_t size);
	void revertMem(uint64_t size);
};



#endif /* PARSEMEM_H_ */
