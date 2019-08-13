#include "sqlWord.h"
#include "sqlParserHandle.h"
#include "sqlParserUtil.h"
#include "util/winString.h"
#include "glog/logging.h"
namespace SQL_PARSER {
	void SQLSingleWord::success(handle* h, const std::string& matchedWord)
	{
		if (h != nullptr)
		{
			if (m_sqlType != UNSUPPORT)
				h->type = m_sqlType;
			if (m_parser != nullptr)
			{
				statusInfo* s = new statusInfo;
				s->sql = matchedWord;
				s->parserFunc = m_parser;
				if (h->head == nullptr)
					h->head = s;
				else
					h->end->next = s;
				h->end = s;
			}
		}
	}
	SQLSingleWord* SQLSingleWord::create(bool optional, const std::string& str)
	{
		if (str == "_A_")
			return new SQLArrayWord(optional);
		else if (str == "_AS_")
			return new SQLAnyStringWord(optional);
		else if (str == "_N_")
			return new SQLNameWord(optional);
		else if (str == "_B_")
			return new SQLBracketsWord(optional);
		else if (str == "_NUM_")
			return new SQLNumberWord(optional);
		else if (strncmp(str.c_str(), "_S_:", 4) == 0)
			return new SQLStringWord(optional, str.c_str() + 4);
		else if (strncmp(str.c_str(), "_C_:", 4) == 0)
			return new SQLCharWord(optional, str.c_str() + 4);
		else
			return nullptr;
	}
	parseValue SQLCharWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		parseValue rtv = OK;
		std::string matchedWord;
		char c = *p;
		if (c >= 'A' && c <= 'Z')
			c += 'a' - 'A';
		if (m_word[0] != c)
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		if (m_parser != nullptr)
			matchedWord = std::string(p, 1);
		LOG(ERROR) << m_comment << "match " << sql;
		sql = p + 1;
		success(h, matchedWord);
		return OK;
	}
	parseValue SQLNameWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		const char* nameStart, * nameEnd;
		uint16_t nameSize;
		if (!getName(p, nameStart, nameSize, nameEnd))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		if (*p != '\'' && *p != '`' && *p != '"')
		{
			if (isKeyWord(nameStart, nameSize))
			{
				LOG(ERROR) << m_comment << " do not match " << sql;
				return NOT_MATCH;
			}
		}
		if (m_parser != nullptr)
			matchedWord = std::string(nameStart, nameSize);
		LOG(ERROR) << m_comment << "match " << sql;
		sql = nameEnd;
		success(h, matchedWord);
		return OK;
	}
	parseValue SQLArrayWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		if (*p != '\'' && *p != '"')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}		
		char quote = *p;
		const char* end = p + 1;
		while (true)
		{
			if (*end == '\0')
				break;
			if (*end == quote)
			{
				if (*(end - 1) != '\\')
				{
					if (quote == '\'' && *(end + 1) == '\'')
					{
						end += 2;
						continue;
					}
					break;
				}
				else
				{
					end++;
					continue;
				}
			}
			else
				end++;
		}
		if (*end == '\0')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}		
		if (m_parser != nullptr)
			matchedWord = std::string(p + 1, end - p - 1);
		LOG(ERROR) << m_comment << "match " << sql;

		sql = end + 1;
		success(h, matchedWord);
		return OK;
	}
	parseValue SQLStringWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		if (strncasecmp(m_word.c_str(), p, m_word.size()) != 0
			|| (!isSpaceOrComment(p + m_word.size()) && p[m_word.size()] != '\0' && !isKeyChar(p[m_word.size()])))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = p + m_word.size();
		success(h, m_word);
		return OK;
	}
	parseValue SQLNumberWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		const char* n = p;
		bool hasDot = false;
		while (*n != '\0')
		{
			if (*n > '9' || *n < '0')
			{
				if (*n == '-')
				{
					if (n != p)
					{
						LOG(ERROR) << m_comment << " do not match " << sql;
						return NOT_MATCH;
					}
					n++;
					while (*n == ' ' || *n == '\t')
						n++;
					continue;
				}
				else if (*n == '.')
				{
					if (hasDot)
					{
						LOG(ERROR) << m_comment << " do not match " << sql;
						return NOT_MATCH;
					}
					else
						hasDot = true;
				}
				else
				{
					if (isKeyChar(*n))
					{
						if (n == p)
						{
							LOG(ERROR) << m_comment << " do not match " << sql;
							return NOT_MATCH;
						}
						else
							break;
					}
					else if (isSpaceOrComment(n))
						break;
					else
					{
						LOG(ERROR) << m_comment << " do not match " << sql;
						return NOT_MATCH;
					}
				}
			}
			n++;
		}
		if (m_parser != nullptr)
			matchedWord = std::string(p, n - p);
		LOG(ERROR) << m_comment << "match " << sql;

		sql = n;
		success(h, matchedWord);
		return OK;
	}
	parseValue SQLAnyStringWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		const char* end = endOfWord(p);
		if (end == nullptr)
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		if (isKeyWord(p, end - p))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		if (m_parser != nullptr)
			matchedWord = std::string(p, end - p);
		LOG(ERROR) << m_comment << "match " << sql;

		sql = end;
		success(h, matchedWord);
		return OK;
	}
	parseValue SQLBracketsWord::match(handle* h, const char*& sql)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		if (*p != '(')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		const char* end = p + 1;
		int32_t bracketCount = 1;
		while (*end != '\0')
		{
			if (*end == '(')
				bracketCount++;
			else if (*end == ')')
			{
				if (--bracketCount <= 0)
					break;
			}
			end++;
		}
		if (*end == '\0')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH;
		}
		if (m_parser != nullptr)
			matchedWord.assign(p, end - p);
		LOG(ERROR) << m_comment << "match " << sql;

		sql = end + 1;
		success(h, matchedWord);
		return OK;
	}

	parseValue SQLWordArray::match(handle* h, const char*& sql)
	{
		parseValue rtv = OK;
		bool matched = false;
		const char* savePoint = sql, * beforeLoop = nullptr;
		statusInfo* top = h->end;
		do
		{
			for (std::list<SQLWord*>::iterator iter = m_words.begin();
				iter != m_words.end(); iter++)
			{
				SQLWord* s = *iter;
				const char* str = nextWord(sql);
				if (s == nullptr)
					continue;
				if ((rtv = s->match(h, str)) != OK)
				{
					if (m_or)
						continue;
					if (s->m_optional)
					{
						rtv = OK;
						continue;
					}
					else
						break;
				}
				else
				{
					sql = str;
					if (m_or)
						break;
				}
			}
			if (rtv != OK)
			{
				if (m_loop && matched)
				{
					if (beforeLoop != nullptr)
						sql = beforeLoop;
					rtv = OK;
				}
				break;
			}
			else
				matched = true;
			if (m_loop)
			{
				if (m_loopCondition != nullptr)
				{
					beforeLoop = sql;
					const char* str = nextWord(sql);
					if (m_loopCondition->match(nullptr, str) == OK)
					{
						sql = str;
						continue;
					}
					else
						break;
				}
			}
			else
				break;
		} while (1);

		if (rtv != OK) //rollback
		{
			for (statusInfo* s = top ? top->next : nullptr; s != nullptr;)
			{
				statusInfo* tmp = s->next;
				delete s;
				s = tmp;
			}
			h->end = top;
			if (h->end != nullptr)
				h->end->next = nullptr;
			else
				h->head = nullptr;
			sql = savePoint;
			LOG(ERROR) << m_comment << " do not match " << sql;
		}
		else
		{
			LOG(ERROR) << m_comment << "  match " << savePoint;
			if (m_parser != nullptr)
			{
				statusInfo* s = new statusInfo;
				s->parserFunc = m_parser;
				if (h->head == nullptr)
					h->head = s;
				else
					h->end->next = s;
				h->end = s;
			}
			if (m_sqlType != UNSUPPORT)
				h->type = m_sqlType;
		}
		return rtv;
	}
}
