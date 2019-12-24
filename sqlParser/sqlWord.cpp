#include "sqlWord.h"
#include "sqlParserHandle.h"
#include "sqlParserUtil.h"
#include "util/winString.h"
#include "glog/logging.h"
#include "util/likely.h"
namespace SQL_PARSER {
	static SQLWordFunction globalFunc(false, '`');//todo
	static SQLWordExpressions globalMathExpression(false, false, '`');
	static SQLWordExpressions globalLogicExpression(false, true, '`');

	SQLSingleWord* SQLSingleWord::create(bool optional, const std::string& str, char qoute)
	{
		if (str == "_A_")
			return new SQLArrayWord(optional);
		else if (str == "_AS_")
			return new SQLAnyStringWord(optional);
		else if (str == "_N_")
			return new SQLNameWord(optional);
		else if (str == "_TABLE_")
			return new SQLTableNameWord(optional, false);
		else if (str == "_TABLE_A_")
			return new SQLTableNameWord(optional, true, qoute);
		else if (str == "_COLUMN_")
			return new SQLColumnNameWord(optional, qoute);
		else if (str == "_B_")
			return new SQLBracketsWord(optional);
		else if (str == "_INT_")
			return new SQLIntNumberWord(optional);
		else if (str == "_FLOAT_")
			return new SQLFloatNumberWord(optional);
		else if (strncmp(str.c_str(), "_S_:", 4) == 0)
			return new SQLStringWord(optional, str.c_str() + 4);
		else if (strncmp(str.c_str(), "_C_:", 4) == 0)
			return new SQLCharWord(optional, str.c_str() + 4);
		else if (strncmp(str.c_str(), "_E_:", 4) == 0)
		{
			if (strncasecmp(str.c_str() + 4, "LOGIC", 6) == 0)
				return new SQLWordExpressions(optional, true, qoute);
			else if (strncasecmp(str.c_str() + 4, "MATH", 5) == 0)
				return new SQLWordExpressions(optional, false, qoute);
			else
				return nullptr;
		}
		else if (str == "_F_")
			return new SQLWordFunction(optional, qoute);
		else if (str == "_VL_")
			return new SQLValueListWord(optional);
		else
			return nullptr;
	}
	DLL_EXPORT SQLValue* SQLCharWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLCharValue* value = MATCH;
		char c = *p;
		if (c >= 'A' && c <= 'Z')
			c += 'a' - 'A';
		if (m_word != c)
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLCharValue();
			value->value = m_word;
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = p + 1;
		return value;
	}
	DLL_EXPORT SQLValue* SQLNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		std::string matchedWord;
		const char* nameStart, * nameEnd;
		uint16_t nameSize;
		SQLNameValue* value = MATCH;
		if (!getName(p, nameStart, nameSize, nameEnd))
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLNameValue();
			value->name.assign(nameStart, nameSize);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = nameEnd;
		return value;
	}
	DLL_EXPORT SQLValue* SQLTableNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* nameStart[3] = { 0 };
		const char* nameEnd;
		uint16_t nameSize[3] = { 0 };
		SQLTableNameValue* value = MATCH;
		if (!getName(p, nameStart[0], nameSize[0], nameEnd, quote))
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (*nameEnd == '.')
		{
			p = nameEnd + 1;
			if (!getName(p, nameStart[1], nameSize[1], nameEnd, quote))
			{
#ifdef DEBUG
				LOG(ERROR) << m_comment << " do not match " << sql;
#endif
				return NOT_MATCH_PTR;
			}
		}
		if (hasAlias)
		{
			p = nextWord(nameEnd);
			if (strncasecmp(p, "AS", 2) == 0 && realEndOfWord(p) == p + 2)
			{
				p = nextWord(p + 2);
				if (*p == '\0' || !getName(p, nameStart[2], nameSize[2], nameEnd, quote))
				{
#ifdef DEBUG
					LOG(ERROR) << m_comment << " do not match for must have alias after AS in table name" << sql;
#endif
					return NOT_MATCH_PTR;
				}
			}
			else
				getName(p, nameStart[2], nameSize[2], nameEnd, quote);
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLTableNameValue();
			if (nameStart[2] != nullptr)
				value->alias.assign(nameStart[2], nameSize[2]);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = nameEnd;
		return value;
	}
	DLL_EXPORT SQLValue* SQLColumnNameWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* nameStart[3] = { 0 };
		const char* nameEnd;
		uint16_t nameSize[3] = { 0 };
		SQLColumnNameValue* value = MATCH;
		if (!getName(p, nameStart[0], nameSize[0], nameEnd,quote))
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (*nameEnd == '.')
		{
			p = nameEnd + 1;
			if (!getName(p, nameStart[1], nameSize[1], nameEnd, quote))
			{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
				return NOT_MATCH_PTR;
			}
			if (*nameEnd == '.')
			{
				p = nameEnd + 1;
				if (!getName(p, nameStart[2], nameSize[2], nameEnd, quote))
				{
#ifdef DEBUG
					LOG(ERROR) << m_comment << " do not match " << sql;
#endif
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
			else if (nameStart[1] != nullptr)
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = nameEnd;
		return value;
	}
	DLL_EXPORT SQLValue* SQLArrayWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;
		if (*p != '\'' && *p != '"')
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		char quote = *p;
		const char* end = p + 1;
		while (true)
		{
			if (*end == '\0')
			{
#ifdef DEBUG
				LOG(ERROR) << m_comment << " do not match " << sql;
#endif
				return NOT_MATCH_PTR;
			}
			if (*end == quote)
			{
				int escapeCount = 1;
				for (; *(end - escapeCount) == '\\'; escapeCount++);
				if (escapeCount & 0x01)
					break;
			}
			end++;
		}

		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValueType::STRING_TYPE);
			if (nullptr == (value->value = (char*)malloc(value->volumn = (end - p))))
			{
				LOG(ERROR) << m_comment << " do not match " << sql << " for alloc memory failed";
				delete value;
				return NOT_MATCH_PTR;
			}
			for (uint32_t soffset = 1; soffset < value->volumn; soffset++)
			{
				if (unlikely(p[soffset] == '\\'))
				{
					if (likely(soffset < value->volumn))
					{
						switch (p[soffset + 1])
						{
						case '\\':
							value->value[value->size++] = '\\';
							soffset++;
							break;
						case 't':
							value->value[value->size++] = '\t';
							soffset++;
							break;
						case '\'':
							value->value[value->size++] = '\'';
							soffset++;
							break;
						case '"':
							value->value[value->size++] = '"';
							soffset++;
							break;
						case 'r':
							value->value[value->size++] = '\r';
							soffset++;
							break;
						case 'n':
							value->value[value->size++] = '\n';
							soffset++;
							break;
						case '0':
							value->value[value->size++] = '\0';
							soffset++;
							break;
						default:
							value->value[value->size++] = '\\';
							soffset++;
							break;
						}
					}
					else
					{

#ifdef DEBUG
						LOG(ERROR) << m_comment << " do not match " << sql;
#endif
						delete value;
						return NOT_MATCH_PTR;
					}
				}
				else
				{
					value->value[value->size++] = p[soffset];
				}
			}
			value->value[value->size] = '\0';
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = end + 1;
		return value;
	}
	DLL_EXPORT SQLValue* SQLStringWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;
		if (strncasecmp(m_word.c_str(), p, m_word.size()) != 0
			|| (!isSpaceOrComment(p + m_word.size()) && p[m_word.size()] != '\0' && !isKeyChar(p[m_word.size()])))
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = p + m_word.size();

		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValueType::STRING_TYPE);
			value->value = (char*)m_word.c_str();
			value->quote = true;
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
	DLL_EXPORT SQLValue* SQLIntNumberWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLIntNumberValue* value = MATCH;
		const char* n = p;
		bool sign = true;
		int64_t v = 0;
		if (*n == '-' || *n == '+')
		{
			if (*n == '-')
				sign = false;
			n++;
			while (*n == ' ')
				n++;
		}

		while (*n <= '9' && *n >= '0')
		{
			int64_t tmp = v * 10 + (*n) - '0';
			if (v > tmp)//overflow
			{
				LOG(ERROR) << m_comment << " do not match for int type overflow" << sql;
				return NOT_MATCH_PTR;
			}
			v = tmp;
			n++;
		}
		if (!sign)
			v = -v;
		if (*n != '\0')
		{
			if (isKeyChar(*n))
			{
				if (n == p || *n == '.')
				{
#ifdef DEBUG
					LOG(ERROR) << m_comment << " do not match " << sql;
#endif
					return NOT_MATCH_PTR;
				}
			}
			else if (!isSpaceOrComment(n))
			{
#ifdef DEBUG
				LOG(ERROR) << m_comment << " do not match " << sql;
#endif
				return NOT_MATCH_PTR;
			}
		}
		else if (n == p)
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}

		if (m_parser != nullptr || needValue)
		{
			value = new SQLIntNumberValue(v);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = n;
		return value;
	}

	DLL_EXPORT SQLValue* SQLFloatNumberWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLFloatNumberValue* value = MATCH;
		const char* n = p;
		bool sign = true;
		int64_t intValue = 0, index = 0;
		double decmValue = 0;
		int8_t intValueSize = 0, decmSize = 0;
		double v = 0;
		if (*n == '-' || *n == '+')
		{
			if (*n == '-')
				sign = false;
			n++;
			while (*n == ' ')
				n++;
		}

		while (*n <= '9' && *n >= '0')
		{
			int64_t tmp = intValue * 10 + (*n) - '0';
			intValueSize++;
			if (intValue > tmp || intValueSize > 16)//overflow
			{
				LOG(ERROR) << m_comment << " do not match for int type overflow" << sql;
				return NOT_MATCH_PTR;
			}
			n++;
			intValue = tmp;
		}
		if (*n == '.')
		{
			n++;
			double pos = 0.1f;
			while (*n <= '9' && *n >= '0')
			{
				decmValue += pos * ((*n) - '0');
				pos /= 10;
				decmSize++;
				if ((decmSize + intValueSize) > 16)//overflow
				{
					LOG(ERROR) << m_comment << " do not match for decmValue overflow" << sql;
					return NOT_MATCH_PTR;
				}
				n++;
			}
			if (*(n - 1) == '.')
			{
				LOG(ERROR) << m_comment << " do not match for no number after [.] in float number" << sql;
				return NOT_MATCH_PTR;
			}
		}
		v = intValue + decmValue;
		if (*n == 'e' || *n == 'E')
		{
			bool indexSign = true;
			n++;
			if (*n == '-' || *n == '+')
			{
				if (*n == '-')
					indexSign = false;
				n++;
				while (*n == ' ')
					n++;
			}
			while (*n <= '9' && *n >= '0')
			{
				index = index * 10 + (*n) - '0';
				if (index > 4932)//overflow
				{
					LOG(ERROR) << m_comment << " do not match for index overflow " << sql;
					return NOT_MATCH_PTR;
				}
				n++;
			}
			if (indexSign)
			{
				for (int idx = 0; idx < index; idx++)
					v *= 10;
			}
			else
			{
				for (int idx = 0; idx < index; idx++)
					v /= 10;
			}
		}
		if (*n != '\0')
		{
			if (isKeyChar(*n))
			{
				if (n == p)
				{
#ifdef DEBUG
					LOG(ERROR) << m_comment << " do not match " << sql;
#endif
					return NOT_MATCH_PTR;
				}
			}
			else if (!isSpaceOrComment(n))
			{
#ifdef DEBUG
				LOG(ERROR) << m_comment << " do not match " << sql;
#endif
				return NOT_MATCH_PTR;
			}
		}
		else if (n == p)
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}


		if (m_parser != nullptr || needValue)
		{
			if (!sign)
				v = -v;
			value = new SQLFloatNumberValue(v);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = n;
		return value;
	}
	DLL_EXPORT SQLValue* SQLAnyStringWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		const char* end = endOfWord(p);
		SQLStringValue* value = MATCH;
		if (end == nullptr || end == p)
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (isKeyWord(p, end - p))
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValueType::STRING_TYPE);

			value->assign(p, end - p);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = end;
		return value;
	}
	DLL_EXPORT SQLValue* SQLOperatorWord::match(handle* h, const char*& sql, bool needValue)
	{
		OPERATOR op = parseOperation(sql);
		if (op == NOT_OPERATION)
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << operationInfos[op].signStr;
#endif
		return new SQLOperatorValue(op);
	}
	DLL_EXPORT SQLValue* SQLValueListWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* pos = nextWord(sql);
		if (*pos != '(')
			return NOT_MATCH_PTR;
		pos = nextWord(pos + 1);
		if (*pos == ')')//not allowed empty list
			return NOT_MATCH_PTR;
		SQLValueList* vlist = needValue ? new SQLValueList() : nullptr;
		SQLValue* v;
		SQLValueType valuesType = SQLValueType::MAX_TYPE;
		while (*pos != '\0')
		{
			if (NOT_MATCH_PTR != (v = globalMathExpression.match(nullptr, pos, needValue)))
			{
				if (needValue)
					vlist->values.push_back(v);
			}
			else if (NOT_MATCH_PTR != (v = globalFunc.match(nullptr, pos, needValue)) ||
				NOT_MATCH_PTR != (v = strWord.match(nullptr, pos, needValue)) ||
				NOT_MATCH_PTR != (v = intWord.match(nullptr, pos, needValue)) ||
				NOT_MATCH_PTR != (v = floatWord.match(nullptr, pos, needValue)))
			{
				if (needValue)
					vlist->values.push_back(v);
				if (valuesType == SQLValueType::MAX_TYPE)
					valuesType = v->type;
				else if (valuesType != v->type)
				{
					if (vlist != nullptr)
						delete vlist;
					return NOT_MATCH_PTR;
				}
			}
			else
			{
				if (vlist != nullptr)
					delete vlist;
				return NOT_MATCH_PTR;
			}
			pos = nextWord(pos);
			if (*pos == ',')
				pos = nextWord(pos + 1);
			else if (*pos == ')')
				break;
			else
			{
				if (vlist != nullptr)
					delete vlist;
				return NOT_MATCH_PTR;
			}
		}
		sql = pos + 1;
		return vlist;
	}
	DLL_EXPORT SQLValue* SQLBracketsWord::match(handle* h, const char*& sql, bool needValue)
	{
		const char* p = nextWord(sql);
		SQLStringValue* value = MATCH;

		if (*p != '(')
		{
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
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
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
			return NOT_MATCH_PTR;
		}
		if (m_parser != nullptr || needValue)
		{
			value = new SQLStringValue(SQLValueType::STRING_TYPE);
			value->assign(p + 1, end - p - 1);
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
#ifdef DEBUG
		LOG(ERROR) << m_comment << "match " << sql;
#endif
		sql = end + 1;
		return value;
	}
	DLL_EXPORT SQLValue* SQLWordArray::match(handle* h, const char*& sql, bool needValue)
	{
		SQLValue* rtv = nullptr;
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
			h->rollbackTo(top);
			sql = savePoint;
#ifdef DEBUG
			LOG(ERROR) << m_comment << " do not match " << sql;
#endif
		}
		else
		{
#ifdef DEBUG

			LOG(ERROR) << m_comment << "  match " << savePoint;
#endif

		}
		return rtv;
	}
	DLL_EXPORT SQLWordExpressions::SQLWordExpressions(bool optional, bool logicOrMath, char qoute) :SQLSingleWord(optional, S_EXPRESSION), logicOrMath(logicOrMath), opWord(false), intWord(false), floatWord(false), strWord(false), nameWord(false, qoute)
	{
		valueList = new SQLValueListWord(false);
	}
	DLL_EXPORT SQLWordExpressions::~SQLWordExpressions()
	{
		delete valueList;
	}
	SQLValue* SQLWordExpressions::matchValue(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList)
	{
		SQLValue* value;
		if (opStack.size()>0&& (opStack.top()->opera== IN_|| opStack.top()->opera == NOT_IN)&&(value = this->valueList->match(nullptr, sql, needValue)) != NOT_MATCH_PTR)
		{
			if (opStack.empty() || (opStack.top()->opera != IN_ && opStack.top()->opera != NOT_IN))
			{
				if (value != nullptr)
					delete value;
				return NOT_MATCH_PTR;
			}
			else
			{
				valueList.push_back(value);
				return value;
			}
		}
		/*search for string,number,function,column name*/
		else if ((value = globalFunc.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
			(value = intWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
			(value = floatWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
			(value = nameWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
			(value = strWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR)
		{
			valueList.push_back(value);
			return value;
		}
		/*search operator whtch only have left value ,like !,~.those operator and value after it will be look as one field*/
		else
		{
			SQLValue* opValue;
			const char* savePoint = sql;
			if ((opValue = opWord.match(nullptr, sql, true)) != NOT_MATCH_PTR)
			{
				if (!operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].hasLeftValues&&
					operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].hasRightValue)
				{
					if ((value = globalFunc.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
						(value = intWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
						(value = floatWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
						(value = nameWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR ||
						(value = strWord.match(nullptr, sql, needValue)) != NOT_MATCH_PTR)
					{
						while (!opStack.empty() && operationInfos[opStack.top()->opera].priority < operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].priority)
						{
							valueList.push_back(opStack.top());
							opStack.pop();
						}
						opStack.push(static_cast<SQLOperatorValue*>(opValue));
						valueList.push_back(value);
						return value;
					}
				}
				delete opValue;
				sql = savePoint;
				return NOT_MATCH_PTR;
			}
		}
		return NOT_MATCH_PTR;
	}
	SQLValue* SQLWordExpressions::matchOperation(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList)
	{
		SQLValue* opValue;
		const char* savePoint = sql;
		if ((opValue = opWord.match(nullptr, sql, true)) != NOT_MATCH_PTR)
		{
			if (operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].hasLeftValues&&
				operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].hasRightValue)
			{
				while (!opStack.empty() && operationInfos[opStack.top()->opera].type != LEFT_BRACKET&&operationInfos[opStack.top()->opera].priority <= operationInfos[static_cast<SQLOperatorValue*>(opValue)->opera].priority)
				{
					valueList.push_back(opStack.top());
					opStack.pop();
				}
				opStack.push(static_cast<SQLOperatorValue*>(opValue));
				return opValue;
			}
			else
			{
				delete opValue;
				sql = savePoint;
				return NOT_MATCH_PTR;
			}
		}
		else
			return NOT_MATCH_PTR;
	}
	SQLValue* SQLWordExpressions::matchLBrac(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList)
	{
		const char* ptr = nextWord(sql);
		if (*ptr == '(')
		{
			SQLOperatorValue* op = new SQLOperatorValue(LEFT_BRACKET);
			opStack.push(op);
			sql = ptr + 1;
			return op;
		}
		else
			return NOT_MATCH_PTR;
	}
	SQLValue* SQLWordExpressions::matchRBrac(const char*& sql, bool needValue, std::stack<SQLOperatorValue*>& opStack, std::list<SQLValue*>& valueList)
	{
		const char* ptr = nextWord(sql);
		if (*ptr == ')')
		{
			SQLOperatorValue* op = nullptr;
			while (!opStack.empty() && (op = opStack.top(), op->opera != LEFT_BRACKET))
			{
				opStack.pop();
				valueList.push_back(op);
			}
			if (op == nullptr || op->opera != LEFT_BRACKET)
				return NOT_MATCH_PTR;
			delete opStack.top();
			opStack.pop();
			sql = ptr + 1;
			return MATCH;
		}
		return NOT_MATCH_PTR;
	}	/*trans mid-prefix to back-prefix*/
	DLL_EXPORT SQLValue* SQLWordExpressions::match(handle* h, const char*& sql, bool needValue)
	{
		std::stack<SQLOperatorValue*> opStack;
		std::list<SQLValue*> valueList;
		SQLExpressionValue* value = nullptr;
		const char* pos = nextWord(sql);
		bool status = true;//true :search left value,false:search operator and right value
		int bracketCount = 0;
		do {
			if (status)
			{
				if (matchValue(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
					status = false;
				else if (matchLBrac(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
					bracketCount++;
				else
					goto NOT_MATCH;
			}
			else
			{
				if (matchOperation(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
				{
					pos = nextWord(pos);
					if (matchValue(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
					{
						pos = nextWord(pos);
						if (bracketCount > 0)
						{
							if (matchRBrac(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
								bracketCount--;
						}

					}
					else if (matchLBrac(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
					{
						bracketCount++;
						status = true;
					}
					else
						goto NOT_MATCH;
				}
				else if(*pos==')'&& bracketCount > 0)
				{
					if (matchRBrac(pos, needValue, opStack, valueList) != NOT_MATCH_PTR)
						bracketCount--;
					else 
						goto NOT_MATCH;
				}
				else
				{
					if (valueList.size() + opStack.size() <= 1)
						goto NOT_MATCH;
					else
						goto CHECK;
				}
			}
			pos = nextWord(pos);
		} while (*pos != '\0');

	CHECK:
		if (bracketCount != 0)
			goto NOT_MATCH;

		value = new SQLExpressionValue();
		value->valueStack = new SQLValue * [valueList.size() + opStack.size()];
		value->count = 0;
		for (std::list <SQLValue*>::iterator iter = valueList.begin(); iter != valueList.end(); iter++)
			value->valueStack[value->count++] = *iter;
		while (!opStack.empty())
		{
			value->valueStack[value->count++] = opStack.top();
			opStack.pop();
		}
		if (value->count == 0 || value->valueStack[value->count - 1]->type != SQLValueType::OPERATOR_TYPE ||
			((operationInfos[static_cast<SQLOperatorValue*>(value->valueStack[value->count - 1])->opera].optType == LOGIC) ^ logicOrMath)
			|| !value->check())
		{
			LOG(ERROR)<<"exp check failed";
			delete value;
			return NOT_MATCH_PTR;
		}
		sql = pos;
		if(needValue)
			value->ref++;
		if (m_parser != nullptr)
			value->ref++;
		return value;
	NOT_MATCH:
		for (std::list <SQLValue*>::iterator iter = valueList.begin(); iter != valueList.end(); iter++)
		{
			if (*iter != nullptr)
				delete* iter;
		}
		while (!opStack.empty())
		{
			delete opStack.top();
			opStack.pop();
		}
		return NOT_MATCH_PTR;
	}

	DLL_EXPORT SQLValue* SQLWordFunction::match(handle* h, const char*& sql, bool needValue)
	{
		SQLValue* name = nullptr;
		const char* pos = nextWord(sql);
		if ((name = asWord.match(nullptr, pos, needValue || m_parser != nullptr)) == NOT_MATCH_PTR)
			return NOT_MATCH_PTR;
		pos = nextWord(pos);
		if (*pos != '(')
		{
			delete name;
			return NOT_MATCH_PTR;
		}
		SQLFunctionValue* sfv = MATCH;

		if (needValue || m_parser != nullptr)
		{
			if (name == nullptr)
				return NOT_MATCH_PTR;
			sfv = new SQLFunctionValue();
			sfv->funcName = static_cast<SQLStringValue*>(name)->value;
		}
		delete name;

		pos = nextWord(pos + 1);
		while (*pos != '\0')
		{
			if (*pos == ')')
			{
				sql = pos + 1;
				if (needValue)
					sfv->ref++;
				if (h != nullptr && m_parser != nullptr)
				{
					statusInfo* s = new statusInfo();
					sfv->ref++;
					s->parserFunc = m_parser;
					s->value = sfv;
					h->addStatus(s);
				}
				return sfv;
			}
			SQLValue* value = MATCH;
			if ((value = globalMathExpression.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = intWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = floatWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = nameWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = strWord.match(h, pos, needValue)) != NOT_MATCH_PTR ||
				(value = match(nullptr, pos, needValue)) != NOT_MATCH_PTR)
			{
				if (sfv != nullptr)
					sfv->argvs.push_back(value);
				else if(value!=nullptr)
					delete value;
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
