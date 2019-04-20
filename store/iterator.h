/*
 * iterator.h
 *
 *  Created on: 2019年1月15日
 *      Author: liwei
 */
//#include "stream.h"
#include <stdint.h>
namespace STORE
{
#define ITER_FLAG_WAIT 0x01 //wait when blocked
class iterator
{
public:
    enum status{
        OK,
        BLOCKED,
        INVALID,
        ENDED
    };
	status m_status;
	unsigned int m_flag;
	//STORE::DBStream * m_stream;
public:
	iterator(uint32_t flag = 0)/*STORE::DBStream * stream = nullptr,uint32_t flag = 0):m_stream(stream),*/:m_status(INVALID), m_flag(flag)
	{}
	virtual bool valid() = 0;
    virtual bool next() = 0;
    virtual void* value() const = 0;
    virtual bool end() = 0;
	inline void waitUp()
	{
		//assert(m_flag&ITER_FLAG_WAIT&&m_stream != nullptr);
		//m_stream->wakeUp();
	}
};
}
