/*
 * iterator.h
 *
 *  Created on: 2019年1月15日
 *      Author: liwei
 */
//#include "stream.h"
#include <stdint.h>
#include "filter.h"
namespace STORE
{
#define ITER_FLAG_WAIT 0x01 //wait when blocked
#define ITER_FLAG_SCHEDULE 0x02 //created by schedule
class iterator
{
public:
    enum status{
        OK,
        UNINIT,
        BLOCKED,
        INVALID,
        ENDED
    };
	status m_status;
	std::string m_errInfo;
	unsigned int m_flag;
	filter *m_filter;
	//STORE::DBStream * m_stream;
public:
	iterator(uint32_t flag , filter * filter)/*STORE::DBStream * stream = nullptr,uint32_t flag = 0):m_stream(stream),*/:m_status(INVALID), m_flag(flag),m_filter(filter)
	{}
	virtual bool valid() = 0;
    virtual status next() = 0;
    virtual const void* value() const = 0;
    virtual bool end() = 0;
	inline void waitUp()
	{
		//assert(m_flag&ITER_FLAG_WAIT&&m_stream != nullptr);
		//m_stream->wakeUp();
	}
};
}
