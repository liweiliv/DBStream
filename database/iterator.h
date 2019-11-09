#pragma once
/*
 * iterator.h
 *
 *  Created on: 2019年1月15日
 *      Author: liwei
 */
#include <stdint.h>
#include "filter.h"
#include "meta/columnType.h"
namespace DATABASE
{
#define ITER_FLAG_WAIT 0x01 //wait when blocked
#define ITER_FLAG_SCHEDULE 0x02 //created by schedule
#define ITER_FLAG_DESC   0x05  //iter is decrease
	class iterator
	{
	protected:
		enum class status {
			OK,
			UNINIT,
			BLOCKED,
			INVALID,
			ENDED
		};
		META::COLUMN_TYPE m_keyType;
		status m_status;
		std::string m_errInfo;
		unsigned int m_flag;
		filter* m_filter;
	public:
		iterator(uint32_t flag, filter* filter)/*STORE::DBStream * stream = nullptr,uint32_t flag = 0):m_stream(stream),*/ :m_status(status::INVALID), m_flag(flag), m_filter(filter)
		{}
		virtual ~iterator() {};
		virtual bool valid() = 0;
		virtual status next() = 0;
		virtual bool seek(const void* key) = 0;
		virtual const void* value() = 0;
		virtual const void* key() const = 0;
		inline META::COLUMN_TYPE keyType() { return m_keyType; }
		inline status getStatus() {
			return m_status;
		}
		inline bool increase()
		{
			return !(m_flag & ITER_FLAG_DESC);
		}
		virtual bool end() = 0;
	};
}
