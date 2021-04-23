#pragma once
#include "token.h"
#include "util/status.h"
#include "util/winDll.h"
#include "instanceInfo.h"
namespace SQL_PARSER
{

	struct sqlParserStack;
	class literalTranslate;
	struct sql;
	struct sqlHandle {
		void* userData;
		uint32_t uid;
		uint32_t tid;
		const instanceInfo* instance;
		std::string currentDatabase;
		sqlParserStack* stack;
		const literalTranslate * literalTrans;
		sql* sqlList;
		sql* tail;
		uint32_t sqlCount;
		DLL_EXPORT sqlHandle();
		DLL_EXPORT ~sqlHandle();
	};
}