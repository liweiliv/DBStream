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
	class Iterator
	{
	public:
		enum class Status {
			OK,
			UNINIT,
			BLOCKED,
			INVALID,
			ENDED
		};
	protected:
		META::COLUMN_TYPE m_keyType;
		Status m_status;
		std::string m_errInfo;
		unsigned int m_flag;
		Filter* m_filter;
	public:
		Iterator(uint32_t flag, Filter* filter)/*DB_INSTANCE::DBStream * stream = nullptr,uint32_t flag = 0):m_stream(stream),*/ :m_status(Status::INVALID), m_flag(flag), m_filter(filter)
		{}
		virtual ~Iterator() {};
		virtual bool valid() = 0;
		virtual Status next() = 0;
		virtual bool seek(const void* key) = 0;
		virtual const void* value() = 0;
		virtual const void* key() const = 0;
		inline META::COLUMN_TYPE keyType() { return m_keyType; }
		inline Status getStatus() {
			return m_status;
		}
		inline bool increase()
		{
			return !(m_flag & ITER_FLAG_DESC);
		}
		virtual bool end() = 0;
	};
}
