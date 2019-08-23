#include "sqlWord.h"
#include "sqlParserHandle.h"
#include "sqlParserUtil.h"
#include "util/winString.h"
#include "glog/logging.h"
namespace SQL_PARSER {
	SQLSingleWord* SQLSingleWord::create(bool optional, const std::string& str)
	{
		if (str == "_A_")
			return new SQLArrayWord(optional);
		else if (str == "_AS_")
			return new SQLAnyStringWord(optional);
		else if (str == "_N_")
			return new SQLNameWord(optional);
		else if (str == "_TABLE_")
			return new SQLTableNameWord(optional);
		else if (str == "_COLUMN_")
			return new SQLColumnNameWord(optional);
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
	SQLValue* SQLCharWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		parseValue rtv = OK;
		SQLStringValue* value = MATCH;
		char c = *p;
		if (c >= 'A' && c <= 'Z')
			c += 'a' - 'A';
		if (m_word[0] != c)
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(p, 1);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = p + 1;
		return value;
	}
	SQLValue* SQLNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		const char* nameStart, * nameEnd;
		uint16_t nameSize;
		SQLDBNameValue* value = MATCH;
		if (!getName(p, nameStart, nameSize, nameEnd))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLDBNameValue();
			value->database.assign(nameStart, nameSize);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = nameEnd;
		return value;
	}
	SQLValue* SQLTableNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* nameStart[2] = { 0 };
		const char* nameEnd;
		uint16_t nameSize[2] = {0};
		SQLTableNameValue* value = MATCH;
		if (!getName(p, nameStart[0], nameSize[0], nameEnd))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (*(nameEnd + 1) == '.')
		{
			p = nameEnd + 2;
			if (!getName(p, nameStart[1], nameSize[1], nameEnd))
			{
				LOG(ERROR) << m_comment << " do not match " << sql;
				return NOT_MATCH_PTR;
			}
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLTableNameValue();
			if (nameStart[1] != nullptr)
			{
				value->database.assign(nameStart[0], nameSize[0]);
				value->table.assign(nameStart[1], nameSize[1]);
			}
			else
			{
				value->table.assign(nameStart[0], nameSize[0]);
			}
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = nameEnd;
		return value;
	}
	SQLValue* SQLColumnNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* nameStart[3] = { 0 };
		const char* nameEnd;
		uint16_t nameSize[3] = { 0 };
		SQLColumnNameValue* value = MATCH;
		if (!getName(p, nameStart[0], nameSize[0], nameEnd))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (*(nameEnd + 1) == '.')
		{
			p = nameEnd + 2;
			if (!getName(p, nameStart[1], nameSize[1], nameEnd))
			{
				LOG(ERROR) << m_comment << " do not match " << sql;
				return NOT_MATCH_PTR;
			}
			if (*(nameEnd + 1) == '.')
			{
				p = nameEnd + 2;
				if (!getName(p, nameStart[2], nameSize[2], nameEnd))
				{
					LOG(ERROR) << m_comment << " do not match " << sql;
					return NOT_MATCH_PTR;
				}
			}
		}

		if (m_parser != nullptr || needValue)
		{
			value = new SQLColumnNameValue();
			if (nameStart[2] != nullptr)
			{
				value->database.assign(nameStart[0], nameSize[0]);
				value->table.assign(nameStart[1], nameSize[1]);
				value->columnName.assign(nameStart[2], nameSize[2]);
			}
			else if(nameStart[1] != nullptr)
			{
				value->table.assign(nameStart[0], nameSize[0]);
				value->columnName.assign(nameStart[1], nameSize[1]);
			}
			else
			{
				value->columnName.assign(nameStart[0], nameSize[0]);
			}
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = nameEnd;
		return value;
	}
	SQLValue* SQLArrayWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;
		if (*p != '\'' && *p != '"')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
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
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(p + 1, end - p - 1);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = end + 1;
		return value;
	}
	SQLValue* SQLStringWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;
		if (strncasecmp(m_word.c_str(), p, m_word.size()) != 0
			|| (!isSpaceOrComment(p + m_word.size()) && p[m_word.size()] != '\0' && !isKeyChar(p[m_word.size()])))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = p + m_word.size();

		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(m_word);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		return value;
	}
	SQLValue* SQLNumberWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;
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
						return NOT_MATCH_PTR;
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
						return NOT_MATCH_PTR;
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
							return NOT_MATCH_PTR;
						}
						else
							break;
					}
					else if (isSpaceOrComment(n))
						break;
					else
					{
						LOG(ERROR) << m_comment << " do not match " << sql;
						return NOT_MATCH_PTR;
					}
				}
			}
			n++;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(p, n - p);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = n;
		return value;
	}
	SQLValue* SQLAnyStringWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* end = endOfWord(p);
		SQLStringValue* value = MATCH;
		if (end == nullptr)
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (isKeyWord(p, end - p))
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(p, end - p);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = end;
		return value;
	}
	SQLValue* SQLBracketsWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;

		if (*p != '(')
		{
			LOG(ERROR) << m_comment << " do not match " << sql;
			return NOT_MATCH_PTR;
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
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValue::STRING_TYPE);
			value->value.assign(p, end - p);
			if (needValue)
				value->ref++;
			if (m_parser != nullptr)
			{
				value->ref++;
				statusInfo* s = new statusInfo();
				s->parserFunc = m_parser;
				s->value = value;
				h->addStatus(s);
			}
		}
		LOG(ERROR) << m_comment << "match " << sql;
		sql = end + 1;
		return value;
	}
	SQLValue* SQLWordArray::match(handle* h, const char*& sql, bool needValue)
	{
		SQLValue * rtv = nullptr;
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
				if ((rtv = s->match(h, str)) == NOT_MATCH_PTR)
				{
					if (m_or)
						continue;
					if (s->m_optional)
					{
						rtv = MATCH;
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
			if (rtv == NOT_MATCH_PTR)
			{
				if (m_loop && matched)
				{
					if (beforeLoop != nullptr)
						sql = beforeLoop;
					rtv = MATCH;
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
					if (m_loopCondition->match(nullptr, str) != NOT_MATCH_PTR)
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

		if (rtv == NOT_MATCH_PTR) //rollback
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
			if (m_sqlType != UNSUPPORT)
				h->type = m_sqlType;
		}
		return rtv;
	}
	SQLWordExpressions::SQLWordExpressions(bool optional) :SQLWord(SQL_EXPRESSION, optional), m_parserFunc(nullptr), numberArgv(false), strWord(false), nameWord(false), func(nullptr)
	{
		func = new SQLWordFunction(false);
	}
	SQLWordExpressions::~SQLWordExpressions()
	{
		delete func;
	}
	SQLValue* SQLWordExpressions::match(handle* h, const char*& sql, bool needValue)
	{
		std::list<const char*> brackets;
		SQLValue* value = MATCH;
		const char* pos = nextWord(sql);
		do {
			while (*pos == '(')
			{
				brackets.push_back(pos);
				pos = nextWord(pos + 1);
			}
			if ((value = match(nullptr, pos, needValue || m_parserFunc != nullptr)) != NOT_MATCH_PTR ||
				(value = func->match(nullptr, pos, needValue || m_parserFunc != nullptr)) != NOT_MATCH_PTR)
			{
				pos = nextWord(pos);
			}
		} while (*pos != '\0');
		return NOT_MATCH_PTR;
	}

	SQLValue* SQLWordFunction::match(handle* h, const char*& sql, bool needValue)
	{
		SQLValue * name = nullptr;
		const char* pos = nextWord(sql);
		if ((name = asWord.match(nullptr, pos, needValue || m_parserFunc != nullptr)) == NOT_MATCH_PTR)
			return NOT_MATCH_PTR;
		pos = nextWord(pos);
		if (*pos != '(')
			return NOT_MATCH_PTR;
		SQLFunctionValue* sfv = MATCH;

		if (needValue || m_parserFunc != nullptr)
		{
			if (name == nullptr)
				return NOT_MATCH_PTR;
			sfv = new SQLFunctionValue();
			sfv->funcName = static_cast<SQLStringValue*>(name)->value;
			delete name;
		}

		pos = nextWord(pos + 1);
		while (*pos != '\0')
		{
			if (*pos == ')')
			{
				sql = pos + 1;
				if (needValue)
					sfv->ref++;
				if (h != nullptr&&m_parserFunc != nullptr)
				{
					statusInfo* s = new statusInfo();
					sfv->ref++;
					s->parserFunc = m_parserFunc;
					s->value = sfv;
					h->addStatus(s);
				}
				return sfv;
			}
			SQLValue* value = MATCH;
			if ((value = expressionWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = numberArgv.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = nameWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = strWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = match(nullptr, pos, needValue)) != NOT_MATCH_PTR)
			{
				if (sfv != nullptr)
					sfv->argvs.push_back(value);
			}
			else
			{
				if (sfv != nullptr)
					delete sfv;
				return NOT_MATCH_PTR;
			}

			pos = nextWord(pos);
			if (*pos == ',')
			{
				pos = nextWord(pos + 1);
				if (*pos == ')')
				{
					if (sfv != nullptr)
						delete sfv;
					return NOT_MATCH_PTR;
				}
			}
			else
			{
				if (*pos != ')')
				{
					if (sfv != nullptr)
						delete sfv;
					return NOT_MATCH_PTR;
				}
			}
		}
		if (sfv != nullptr)
			delete sfv;
		return NOT_MATCH_PTR;
	}
}
