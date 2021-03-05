#pragma once
#include "token.h"
#include "util/status.h"
namespace SQL_PARSER
{
	struct sqlHandle;
	typedef dsStatus& (*parserFuncType)(sqlHandle*, const token*);
	struct sqlValueFuncPair {
		const token* value;
		parserFuncType func;
		sqlValueFuncPair* next;
		inline void init(const token* value, parserFuncType func)
		{
			this->value = value;
			this->func = func;
			this->next = nullptr;
		}
	};
	struct sql {
		uint32_t id;
		const char* currentDatabase;
		sqlValueFuncPair * head;
		sqlValueFuncPair* tail;
		void* userData;
		sql* next;
		inline void init(uint32_t id,const char * currentDatabase)
		{
			this->id = id;
			this->currentDatabase = currentDatabase;
			head = nullptr;
			userData = nullptr;
			next = nullptr;
		}
		inline void add(sqlValueFuncPair* pair)
		{
			tail->next = pair;
			tail = pair;
		}
		inline dsStatus& semanticAnalysis(sqlHandle * handle)
		{
			for (sqlValueFuncPair* p = head; p != nullptr; p = p->next)
				dsReturnIfFailed(p->func(handle, p->value));
			dsOk();
		}
	};
	struct sqlHandle {
		void* userData;
		uint32_t uid;
		uint32_t tid;
		std::string currentDatabase;
		sql* sqlList;
		sql* tail;
		uint32_t sqlCount;
	};
}