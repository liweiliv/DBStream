#pragma once
#include <stdint.h>
#include <string>
#include "job.h"
namespace DB_INSTANCE {
	class iterator;
	class streamProcesser:public jobProcess
	{
	private:
		std::string m_define;
		uint16_t m_sourceItersCount;
		iterator ** m_sourceIters;
	public:
		bool process()
		{

		}
	};
}
