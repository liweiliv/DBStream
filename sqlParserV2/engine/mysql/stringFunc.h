#pragma once
#include "util/status.h"
#include "meta/charset.h"
#include "../../field.h"
namespace SQL_PARSER {
	class stringFuncs {
	public:
		static inline DS ASCII(const uint64_t* argvList, uint8_t count, uint64_t*& returnValue, sqlHandle* handle)
		{
			const stringField* str = (const stringField*)argvList[0];
			if (str == nullptr || str->length == 0)
				dsFailed(1, "empty string");
			returnValue = str->str[0];
			dsOk();
		}

		
	};
}