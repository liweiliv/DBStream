#pragma once
#include <stdint.h>
#include "util/likely.h"
#include "meta/metaData.h"
namespace SQL_PARSER
{
	static constexpr auto MAX_EXPRESSION_OPERATION_COUNT = 1024;
	static constexpr auto MAX_EXPRESSION_FIELD_COUNT = 1024;
	template<typename T>
	struct sqlStack {
		T s[MAX_EXPRESSION_OPERATION_COUNT];
		uint32_t t;
		sqlStack()
		{
			memset(s, 0, sizeof(s));
			t = 0;
		}
		inline void pop()
		{
			if (likely(t > 0))
				t--;
		}
		inline T& top() { return s[t - 1]; }
		inline T& popAndGet()
		{
			if (likely(t > 0))
			{
				t--;
				return s[t];
			}
			else
				abort();
		}
		inline bool push(T v)
		{
			if (likely(t < MAX_EXPRESSION_OPERATION_COUNT))
			{
				s[t++] = v;
				return true;
			}
			else
			{
				return false;
			}
		}
		inline uint32_t size()
		{
			return t;
		}
		inline bool empty()
		{
			return t == 0;
		}
	};

	struct sqlParserStack {
		sqlStack<operatorSymbol*> opStack;
		sqlStack<token*> valueStack;

		sqlStack<META::TableMeta*> allocedTables;
		sqlStack<META::ColumnMeta*> allocedColumns;

		leveldb::Arena arena;
		inline void clear()
		{
			opStack.t = 0;
			valueStack.t = 0;
			arena.clear();
		}
	};
}