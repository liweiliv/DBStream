#include "sqlWord.h"
#if 0
namespace SQL_PARSER {

	/*
	DLL_EXPORT void initKeyWords()
	{
		for (char i = 0; i < ' '; i++)
			KeyChar[i] = true;

		KeyChar[' '] = true;
		KeyChar['\t'] = true;
		KeyChar['\n'] = true;
		KeyChar['\r'] = true;
		KeyChar['.'] = true;
		KeyChar['`'] = true;
		KeyChar['\''] = true;
		KeyChar['"'] = true;
		KeyChar['+'] = true;
		KeyChar['-'] = true;
		KeyChar['*'] = true;
		KeyChar['/'] = true;
		KeyChar['%'] = true;
		KeyChar['&'] = true;
		KeyChar['|'] = true;
		KeyChar['~'] = true;
		KeyChar['^'] = true;
		KeyChar['('] = true;
		KeyChar[')'] = true;
		KeyChar['{'] = true;
		KeyChar['}'] = true;
		KeyChar['='] = true;
		KeyChar['>'] = true;
		KeyChar['<'] = true;
		KeyChar[';'] = true;
		KeyChar[','] = true;
		KeyChar[';'] = true;
		KeyChar['.'] = true;
		KeyChar['['] = true;
		KeyChar['?'] = true;
		KeyChar['^'] = true;
	}
	*/
#define jumpOverSpace() while (c == ' ' || c == '\n' || c == '\r' || c == '\t')\
		{\
			pos++;\
			if ((c = *pos) == '\0')\
			{\
				value = nullptr;\
				dsOk();\
			}\
		}
	DS grammarTree::nextWord(sqlHandle* handle, sqlParserStack* s, sqlValue*& value, const char*& pos)
	{
		PARSE_STATUS status = PARSE_STATUS::NEED_LEFT_VALUE;
		char c;
		operatorSqlValue* op;
		for (;;)
		{
			c = *pos;
			jumpOverSpace();
			if (c == '\'' || c == '"' || c == '`')
			{
				if (status == PARSE_STATUS::NEED_OPERATOR)
					dsReturn(endOfValue(handle, s, pos, value, status));
				dsReturnIfFailed(nextQuoteWord(handle, s, pos));
				status = PARSE_STATUS::NEED_OPERATOR;
			}
			else if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) //table name ,column name ,alias, variable name must start with letter
			{
				if (status == PARSE_STATUS::NEED_OPERATOR)
					dsReturn(endOfValue(handle, s, pos, value, status));
				dsReturnIfFailed(nextString(handle, s, pos));
				status = PARSE_STATUS::NEED_OPERATOR;
			}
			else if (c >= '0' && c <= '9')
			{
				if (status == PARSE_STATUS::NEED_OPERATOR)
					dsReturn(endOfValue(handle, s, pos, value, status));
				dsReturnIfFailed(nextNumber(handle, s, pos));
				status = PARSE_STATUS::NEED_OPERATOR;
			}
			else if (c == '-')
			{
				if (status == PARSE_STATUS::NEED_OPERATOR)//- means sub
				{
					op = m_opTress.match(pos);
					while (s->opStack.size() > 0 && s->opStack.top()->op->priority <= op->op->priority)
					{
						if (!s->valueStack.push(s->opStack.popAndGet()))
							dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
					}
					if (!s->opStack.push(op))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
					status = PARSE_STATUS::NEED_RIGHT_VALUE;
				}
				else if (status == PARSE_STATUS::NEED_LEFT_VALUE || status == PARSE_STATUS::NEED_RIGHT_VALUE)//- means  negative
				{
					dsReturnIfFailed(nextNumber(handle, s, pos));
					status = PARSE_STATUS::NEED_OPERATOR;
				}
				else
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			else if (c == ',')
			{
				if (status == PARSE_STATUS::FIELD_LIST_VALUE_SEP)
					status = PARSE_STATUS::FIELD_LIST_VALUE;
				else
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				pos++;
			}
			else if (c == '(')
			{
				dsReturnIfFailed(parseLBrac(handle, s, status, pos));
			}
			else if (c == ')')
			{
				dsReturnIfFailed(parseRBrac(handle, s, status, pos));
			}
			else if (c == '=')
			{
				if (handle->setOrWhere)//in set part '=' means set value 
				{
					if (status == PARSE_STATUS::NEED_OPERATOR)
						dsReturn(endOfValue(handle, s, pos, value, status));
					else if (status == PARSE_STATUS::NEED_LEFT_VALUE)
					{
						if (!s->valueStack.empty())
							dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
						value = (sqlValue*)s->arena.AllocateAligned(sizeof(sqlValue));
						value->pos = pos;
						value->length = 1;
						value->type = SQL_VALUE_TYPE::WORD;
						pos++;
						dsOk();
					}
					else
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else //in where part '=' means equal 
				{
					if (status != PARSE_STATUS::NEED_OPERATOR)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					op = m_opTress.match(pos);
					while (s->opStack.size() > 0 && s->opStack.top()->op->priority <= op->op->priority)
					{
						if (!s->valueStack.push(s->opStack.popAndGet()))
							dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
					}
					if (!s->opStack.push(op))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
					status = PARSE_STATUS::NEED_RIGHT_VALUE;
				}
			}
			else if ((op = m_opTress.match(pos)) != nullptr)
			{
				if (status == PARSE_STATUS::NEED_LEFT_VALUE || status == PARSE_STATUS::NEED_RIGHT_VALUE)
				{
					if (op->op->hasLeftValues)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else if (status == PARSE_STATUS::NEED_OPERATOR)
				{
					if (!op->op->hasLeftValues)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				while (s->opStack.size() > 0 && s->opStack.top()->op->priority <= op->op->priority)
				{
					if (!s->valueStack.push(s->opStack.popAndGet()))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				}
				if (!s->opStack.push(op))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				if (op->op->hasRightValue)
					status = PARSE_STATUS::NEED_RIGHT_VALUE;
			}
			else if (c == '.')
			{
				if (s->opStack.empty() && !s->valueStack.empty() && (s->valueStack.top()->type == SQL_VALUE_TYPE::WORD || s->valueStack.top()->type == m_tableNameQuoteType))
				{
					pos++;
					if ((c = *pos) == m_tableNameQuote)
						dsReturnIfFailed(nextQuoteWord(handle, s, pos));
					else if (((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')))
						dsReturnIfFailed(nextString(handle, s, pos));
					else
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					if (*pos == '.')
					{
						pos++;
						if ((c = *pos) == m_tableNameQuote)
							dsReturnIfFailed(nextQuoteWord(handle, s, pos));
						else if (((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')))
							dsReturnIfFailed(nextString(handle, s, pos));
						else
							dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
						fieldSqlValue* v = (fieldSqlValue*)s->arena.AllocateAligned(sizeof(fieldSqlValue));
						v->count = 3;
						v->third = s->valueStack.popAndGet();
						v->second = s->valueStack.popAndGet();
						v->first = s->valueStack.popAndGet();
						v->pos = v->first->pos;
						v->length = pos - v->pos;
						s->valueStack.push(v);
					}
					else
					{
						fieldSqlValue* v = (fieldSqlValue*)s->arena.AllocateAligned(sizeof(fieldSqlValue));
						v->count = 2;
						v->second = s->valueStack.popAndGet();
						v->first = s->valueStack.popAndGet();
						v->third = nullptr;
						v->pos = v->first->pos;
						v->length = pos - v->pos;
						s->valueStack.push(v);
					}
				}
				else
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			else
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}
		dsOk();
	}
	void grammarTree::initKeyWords()
	{
		m_keywords.insert(new sqlValue("ABSOLUTE"))
	}

}
#endif