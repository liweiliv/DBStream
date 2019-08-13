#pragma once
#include <string>
#include <list>
#include "sqlParserHandle.h"
#include "sqlParserUtil.h"
namespace SQL_PARSER {
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
			SQL_ARRAY, SQL_SIGNLE_WORD
		};
		SQLWordType m_type;
		parseValue(*m_parser)(handle* h, const std::string& sql);
		virtual parseValue match(handle* h, const char*& sql) = 0;
		void include()
		{
			m_refs++;
		}
		bool deInclude()
		{
			return --m_refs == 0;
		}
		SQLWord(SQLWordType t, bool optional = false) :
			m_optional(optional), m_refs(0), m_sqlType(UNSUPPORT), m_forwardDeclare(false), m_type(t), m_parser(
				NULL)
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
			S_NAME, //dbname,tablename,column name
			S_ARRAY, //"xxxx" or 'xxxx'
			S_STRING,
			S_ANY_WORD,
			S_BRACKETS, //(xxxx)
			S_NUMBER
		};
		std::string m_word;
		sqlSingleWordType m_wtype;
		SQLSingleWord(bool optional, sqlSingleWordType type, const std::string& word) :
			SQLWord(SQL_SIGNLE_WORD, optional), m_word(word), m_wtype(type)
		{
		}
		virtual ~SQLSingleWord() {}
		virtual parseValue match(handle* h, const char*& sql) = 0;
		static SQLSingleWord* create(bool optional, const std::string& str);
	protected:
		void success(handle* h, const std::string& matchedWord);
	};

	class SQLCharWord : public SQLSingleWord
	{
	public:
		SQLCharWord(bool optional, const std::string& word) :SQLSingleWord(optional, S_CHAR, word)
		{
		}
		virtual ~SQLCharWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLNameWord :public SQLSingleWord
	{
	public:
		SQLNameWord(bool optional) :SQLSingleWord(optional, S_NAME, "")
		{
		}
		virtual ~SQLNameWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLArrayWord :public SQLSingleWord
	{
	public:
		SQLArrayWord(bool optional) :SQLSingleWord(optional, S_ARRAY, "")
		{
		}
		virtual ~SQLArrayWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLStringWord :public SQLSingleWord
	{
	public:
		SQLStringWord(bool optional, const std::string& word) :SQLSingleWord(optional, S_STRING, word)
		{
		}
		virtual ~SQLStringWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLNumberWord :public SQLSingleWord
	{
	public:
		SQLNumberWord(bool optional) :SQLSingleWord(optional, S_NUMBER, "")
		{
		}
		virtual ~SQLNumberWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLAnyStringWord :public SQLSingleWord
	{
	public:
		SQLAnyStringWord(bool optional) :SQLSingleWord(optional, S_ANY_WORD, "")
		{
		}
		virtual ~SQLAnyStringWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLBracketsWord :public SQLSingleWord
	{
	public:
		SQLBracketsWord(bool optional) :SQLSingleWord(optional, S_ANY_WORD, "")
		{
		}
		virtual ~SQLBracketsWord() {}
		virtual parseValue match(handle* h, const char*& sql);
	};
	class SQLWordArray : public SQLWord
	{
	public:
		std::list<SQLWord*> m_words;
		bool m_or;
		bool m_loop;
		SQLSingleWord* m_loopCondition;
		SQLWordArray(bool optional, bool _or, bool loop, SQLSingleWord* loopCondition) :
			SQLWord(SQL_ARRAY, optional), m_or(_or), m_loop(loop), m_loopCondition(loopCondition)
		{
		}
		~SQLWordArray()
		{
			for (std::list<SQLWord*>::iterator iter = m_words.begin();
				iter != m_words.end(); iter++)
			{
				SQLWord* s = static_cast<SQLWord*>(*iter);
				if (s != NULL)
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
		virtual parseValue match(handle* h, const char*& sql);
	};
}
