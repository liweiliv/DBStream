#pragma once
#include "../../function.h"
#include "../../sqlStack.h"
#include "util/timer.h"
#include "meta/columnType.h"
namespace SQL_PARSER
{
	class mysqlFunction :public function {
	private:
		static inline DS now(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			*returnValue = timer::getNowTimestamp();
			dsOk();
		}
	};
}