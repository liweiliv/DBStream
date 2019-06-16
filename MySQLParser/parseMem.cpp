/*
 * parseMem.cpp
 *
 *  Created on: 2018年6月28日
 *      Author: liwei
 */
#include "parseMem.h"
#include "MempRing.h"
#include "BR.h"
#include "MD.h"
#include <assert.h>
 parseMem::parseMem(_memp_ring * mempool) :
		m_parseMem(NULL), m_parseMemSize(0), m_parseMemUsedSize(0), m_mempool(mempool)
{
}
 parseMem::~parseMem()
{
	reset();
}
void  parseMem::reset()
{
	if (m_parseMem && m_parseMemSize > m_parseMemUsedSize)
		ring_realloc(m_mempool, m_parseMem, m_parseMemUsedSize);
	m_parseMem = NULL;
	m_parseMemSize = m_parseMemUsedSize = 0;
}
void  parseMem::init(BinlogRecordImpl * record)
{
	ITableMeta* meta = record->getTableMeta();
	if (meta == NULL)
		m_parseMemSize = 512; //defualt alloc 512 byte
	else
		m_parseMemSize = meta->getColCount() * 64;
	m_parseMem = (char*) ring_alloc(m_mempool, m_parseMemSize);
	*(char**)(m_parseMem) = NULL;
	m_parseMemUsedSize = sizeof(char**);
	record->setUserData(m_parseMem);
}
char *  parseMem::alloc(uint64_t size)
{
	if (size > m_parseMemSize - m_parseMemUsedSize)
	{
		ring_realloc(m_mempool, m_parseMem, m_parseMemUsedSize);
		if (size < 1024)
			m_parseMemSize = size * 16;
		else if (size < 1024 * 512)
			m_parseMemSize = size * 2;
		else
			m_parseMemSize = size;
		char * _tmp = (char*) ring_alloc(m_mempool, m_parseMemSize);
		*(char**) m_parseMem = _tmp;
		m_parseMem = _tmp;
		*(char**) m_parseMem = NULL;
		m_parseMemUsedSize = sizeof(char**);
	}
	char * rtv = m_parseMem + m_parseMemUsedSize;
	m_parseMemUsedSize += size;
	return rtv;
}
void  parseMem::revertMem(uint64_t size)
{
	assert(size<m_parseMemUsedSize-sizeof(char**));
	m_parseMemUsedSize -= size;
}


