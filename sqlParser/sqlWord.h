#pragma once
#include <string>
#include <list>
#include "sqlParserHandle.h"
#include "sqlParserUtil.h"
#include "sqlValue.h"
namespace SQL_PARSER {
	#define NOT_MATCH_PTR  ((SQL_PARSER::SQLValue*)0xffffffffffffffffULL)
	#define MATCH  nullptr
	class SQLWord
	{
	public:
		bool m_optional;
		uint32_t m_refs;
		std::string m_comment;
		SQL_TYPE m_sqlType;
		bool m_forwardDeclare;
		enum SQLWordType
		{
			SQL_ARRAY, SQL_SIGNLE_WORD,SQL_FUNCTION,SQL_EXPRESSION
		};
		SQLWordType m_type;
		virtual SQLValue* match(handle* h, const char*& sql,bool needValue = false) = 0;
		void include()
		{
			m_refs++;
		}
		bool deInclude()
		{
			return --m_refs == 0;
		}
		SQLWord(SQLWordType t, bool optional = false) :
			m_optional(optional), m_refs(0), m_sqlType(UNSUPPORT), m_forwardDeclare(false), m_type(t)
		{
		}
		virtual ~SQLWord() {}
	};
	class SQLSingleWord : public SQLWord
	{
	public:
		enum sqlSingleWordType
		{
			S_CHAR, //single char 
			S_NAME,
			S_TABLE_NAME,
			S_COLUMN_NAME,
			S_ARRAY, //"xxxx" or 'xxxx'
			S_STRING,
			S_ANY_WORD,
			S_BRACKETS, //(xxxx)
			S_NUMBER,
			S_OPERATOR,
			S_VALUE_LIST,
			S_FUNC,
			S_EXPRESSION
		};
		sqlSingleWordType m_wtype;
		parserFuncType m_parser;
		SQLSingleWord(bool optional, sqlSingleWordType type) :
			SQLWord(SQL_SIGNLE_WORD, optional),m_wtype(type), m_parser(nullptr)
		{
		}
		virtual ~SQLSingleWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false) = 0;
		static SQLSingleWord* create(bool optional, const std::string& str);
	};

	class SQLCharWord : public SQLSingleWord
	{
	public:
		char m_word;
		SQLCharWord(bool optional, const std::string& word) :SQLSingleWord(optional, S_CHAR)
		{
			m_word = word.c_str()[0];
		}
		virtual ~SQLCharWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLNameWord :public SQLSingleWord
	{
	public:
		SQLNameWord(bool optional) :SQLSingleWord(optional, S_NAME)
		{
		}
		virtual ~SQLNameWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLTableNameWord :public SQLSingleWord
	{
	public:
		SQLTableNameWord(bool optional) :SQLSingleWord(optional, S_TABLE_NAME)
		{
		}
		virtual ~SQLTableNameWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLColumnNameWord :public SQLSingleWord {
	public:
		SQLColumnNameWord(bool optional) :SQLSingleWord(optional, S_COLUMN_NAME)
		{
		}
		virtual ~SQLColumnNameWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLArrayWord :public SQLSingleWord
	{
	public:
		SQLArrayWord(bool optional) :SQLSingleWord(optional, S_ARRAY)
		{
		}
		virtual ~SQLArrayWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLStringWord :public SQLSingleWord
	{
	public:
		std::string m_word;
		SQLStringWord(bool optional, const std::string& word) :SQLSingleWord(optional, S_STRING),m_word(word)
		{
		}
		virtual ~SQLStringWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLNumberWord :public SQLSingleWord
	{
	public:
		SQLNumberWord(bool optional) :SQLSingleWord(optional, S_NUMBER)
		{
		}
		virtual ~SQLNumberWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLAnyStringWord :public SQLSingleWord
	{
	public:
		SQLAnyStringWord(bool optional) :SQLSingleWord(optional, S_ANY_WORD)
		{
		}
		virtual ~SQLAnyStringWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLBracketsWord :public SQLSingleWord
	{
	public:
		SQLBracketsWord(bool optional) :SQLSingleWord(optional, S_ANY_WORD){}
		virtual ~SQLBracketsWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLOperatorWord :public SQLSingleWord
	{
	public:
		SQLOperatorWord(bool optional) :SQLSingleWord(optional, S_OPERATOR) {}
		virtual ~SQLOperatorWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};

	class SQLWordArray : public SQLWord
	{
	public:
		std::list<SQLWord*> m_words;
		bool m_or;
		bool m_loop;
		SQLWord* m_loopCondition;
		SQLWordArray(bool optional, bool _or, bool loop, SQLWord* loopCondition) :
			SQLWord(SQL_ARRAY, optional), m_or(_or), m_loop(loop), m_loopCondition(loopCondition)
		{
		}
		~SQLWordArray()
		{
			for (std::list<SQLWord*>::iterator iter = m_words.begin();
				iter != m_words.end(); iter++)
			{
				SQLWord* s = static_cast<SQLWord*>(*iter);
				if (s != nullptr)
				{
					if (s->deInclude())
						delete (*iter);
				}
			}
		}
		SQLWordArray(const SQLWordArray& s) :
			SQLWord(SQL_ARRAY, s.m_optional), m_or(s.m_or), m_loop(s.m_loop), m_loopCondition(s.m_loopCondition)
		{

		}
		void append(SQLWord* s)
		{
			m_words.push_back(s);
		}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};

	class SQLWordFunction;
	class SQLValueListWord;
	class SQLWordExpressions :public SQLSingleWord {
	private:
		SQLValue* matchValue(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList);
		SQLValue* matchOperation(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList);
		SQLValue* matchLBrac(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList);
		SQLValue* matchRBrac(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList);
	public:
		bool logicOrMath;
		SQLOperatorWord opWord;
		SQLNumberWord numberWord;
		SQLArrayWord strWord;
		SQLColumnNameWord nameWord;
		SQLWordFunction* func;
		SQLValueListWord* valueList;
		SQLWordExpressions(bool optional,bool logicOrMath);
		~SQLWordExpressions();
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLWordFunction :public SQLSingleWord {
	private:
		SQLNumberWord numberArgv;
		SQLArrayWord strWord;
		SQLColumnNameWord nameWord;
		SQLWordExpressions expressionWord;
		SQLAnyStringWord asWord;
	public:
		SQLWordFunction(bool optional) :SQLSingleWord(optional, S_FUNC),numberArgv(false), strWord(false), nameWord(false), expressionWord(false,false),asWord(false){}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
	class SQLValueListWord :public SQLSingleWord
	{
	public:
		SQLNumberWord numberArgv;
		SQLArrayWord strWord;
		SQLWordExpressions expressionWord;
		SQLWordFunction funcWord;
		SQLValueListWord(bool optional) :SQLSingleWord(optional, S_VALUE_LIST), numberArgv(false), strWord(false),expressionWord(false,false), funcWord(false) {}
		virtual ~SQLValueListWord() {}
		virtual SQLValue* match(handle* h, const char*& sql, bool needValue = false);
	};
}
