#pragma once
#include <functional>
#include <glog/logging.h>
#include "thread/threadLocal.h"
#include "util/nameCompare.h"
#include "util/sparsepp/spp.h"
#include "util/nameCompare.h"
#include "token.h"
#include "util/arena.h"
#include "sqlStack.h"
#include "separator.h"
#include "keyWords.h"
#include "sqlHandle.h"
#include "operationInfo.h"
#include "errorCode.h"
#include "lex.h"
#include "literalTranslate.h"
namespace SQL_PARSER {

	struct grammarToken;
	typedef spp::sparse_hash_map<const str*, const grammarToken*, strCompare, strCompare> grammarHashMap;
	typedef spp::sparse_hash_map<const str*, keyWordInfo*, strCompare, strCompare> keyWordMap;

	enum grammarTokenMatchType {
		VALUE,
		KEY_WORD,
		IDENTIFIER,
		STRING,
		INT_NUMBER,
		FLOAT_NUMBER,
		SYMBOL
	};
	enum class SQL_TYPE {
		ORACLE,
		MYSQL,
		POSTGRESQL,
		SQL_SERVER,
		DB2
	};
	struct grammarToken {
		token word;
		bool matchAnyValueToekn;
		literalType type;
		parserFuncType func;
		const grammarToken* valueNext;//next is value, include literal,identifier,function,expression,some keyword
		const grammarToken* symbNext;
		const grammarToken* keyWordNext;
		const grammarToken* identiferNext;
		grammarHashMap nextKeyWordMap;
		bool leaf;
	};
#define NOT_MATHCH_RETURN if(matched){dsFailed(1, "grammar error @ "<< std::string(sqlPos, min(50, strlen(sqlPos))));}else{dsReturnCode(1);}
#define NOT_MATCH_CHECK_ANNOTATION_RETRY(i) if() 

#define tryProcessAnnotation(sqlPos) \
			if ((sqlPos)[0] == '-') \
			{\
				if ((sqlPos)[1] == '-')\
				{\
					if (m_sqlType == SQL_TYPE::MYSQL)\
					{\
						if ((sqlPos)[2] == ' ')\
						{\
							(sqlPos) += 3;\
							seekToNextLine(sqlPos);\
						}\
					}\
					else\
					{\
						(sqlPos) += 2;\
						seekToNextLine(sqlPos);\
					}\
				}\
			}\
			else if ((sqlPos)[0] == '#')\
			{\
				(sqlPos)++;\
				seekToNextLine(sqlPos);\
			}\
			else if ((sqlPos)[0] == '/' && (sqlPos)[1] == '*')\
			{\
				dsReturnIfFailed(processMultiLineAnnotation(sqlPos));\
			}\

#define NOT_MATCH_CHECK_ANNOTATION_RETRY(sqlPos, i) \
			if ((sqlPos)[0] == '-') \
			{\
				if ((sqlPos)[1] == '-')\
				{\
					if (m_sqlType == SQL_TYPE::MYSQL)\
					{\
						if ((sqlPos)[2] == ' ')\
						{\
							(sqlPos) += 3;\
							seekToNextLine(sqlPos);\
							goto MATCH_##i;\
						}\
					}\
					else\
					{\
						(sqlPos) += 2;\
						seekToNextLine(sqlPos);\
						goto MATCH_##i;\
					}\
				}\
			}\
			else if ((sqlPos)[0] == '#')\
			{\
				(sqlPos)++;\
				seekToNextLine(sqlPos);\
				goto MATCH_##i;\
			}\
			else if ((sqlPos)[0] == '/' && (sqlPos)[1] == '*')\
			{\
				dsReturnIfFailed(processMultiLineAnnotation(sqlPos));\
				goto MATCH_##i;\
			}\

	class sqlParser
	{

	private:
		keyWordMap m_keyWords;
		operationInfoTree m_opTress;
		char m_identifierQuote;
		bool m_doubleQuoteCanBeString;
		operatorSymbol* m_lbr;
		operatorSymbol* m_rbr;
		grammarHashMap m_grammarTree;
		threadLocal<sqlParserStack> m_stack;
		literalTranslate m_litTrans;
	protected:
		SQL_TYPE m_sqlType;
		int m_mysqlVersion;
	private:
		template<typename T>
		inline DS matchGeneralLiteral(sqlParserStack* stack, token*& t, char*& pos, T f, literalType type)
		{
			const char* start = pos + 2;
			while (f(*pos))
			{
				pos++;
				continue;
			}
			if (*pos != '\'')
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << start, ERROR);
			literal* l = (literal*)stack->arena.AllocateAligned(sizeof(literal));
			l->lType = type;
			l->value.assign(start, pos - start);
			l->type = tokenType::literal;
			t = l;
			pos++;
			dsOk();
		}
		inline DS tryMatchExpression(sqlParserStack* stack, token*& t, char*& pos)
		{
			nextWordPos(pos);
			char* p = pos;
			operatorSymbol* op = m_opTress.match(p);
			if (op != nullptr && (op->op->optType == OPERATION_TYPE::MATHS || op->op->optType == OPERATION_TYPE::LOGIC))
			{
				pos = p;
				dsReturn(matchExpression(stack, t, op, pos));
			}
			dsOk();
		}
		void initKeyWords()
		{
			for (int i = 0; i < sizeof(reserverdKeyWords) / sizeof(const char*); i++)
				m_keyWords.insert(std::pair<const str*, keyWordInfo*>(new str(reserverdKeyWords[i]), nullptr));
		}
	public:

		sqlParser()
		{
			initKeyWords();
			char lb[2] = { '(','\0'};
			char rb[2] = { ')','\0' };
			char* pos = lb;
			m_lbr = m_opTress.match(pos);
			pos = rb;
			m_rbr = m_opTress.match(pos);
		}

		void setIdentifierQuote(char symb)
		{
			if (symb != '\"' && symb != '`')
				return;
			m_identifierQuote = symb;
		}

		void setDoubleQuoteCanBeString(bool yes)
		{
			m_doubleQuoteCanBeString = yes;
		}
	protected:
		inline void seekToNextLine(char*& sqlPos)
		{
			char c;
			while ((c == *(sqlPos++)) != '\0')
			{
				if (c == '\n')
					return ;
			}
		}

		inline bool seekToEndOfMultiLineAnnotation(char*& sqlPos)
		{
			char c;
			while ((c = *(sqlPos++)) != '\0')
			{
				if (c == '*' && *sqlPos == '/')
				{
					sqlPos++;
					return true;
				}
			}
			return false;
		}

		DS processMultiLineAnnotation(char*& sqlPos)
		{
			sqlPos += 2;
			if (m_sqlType == SQL_TYPE::MYSQL && *sqlPos == '!')
			{
				char* pos = sqlPos + 1;
				nextWordPos(pos);
				if ((*pos >= '0' && *pos <= '9'))
				{
					int version = atoi(pos);
					if (version < m_mysqlVersion)
					{
						if (!seekToEndOfMultiLineAnnotation(pos))
							dsFailed(1, "can not find end of annotation @" << std::string(sqlPos, min(strlen(sqlPos), 50)));
						sqlPos = pos;
						dsOk();
					}
					else
					{
						while ((*pos >= '0' && *pos <= '9'))
							pos++;
						sqlPos = pos;
						if (!seekToEndOfMultiLineAnnotation(pos))
							dsFailed(1, "can not find end of annotation @" << std::string(sqlPos, min(strlen(sqlPos), 50)));
						*(pos - 1) = ' ';
						*(pos - 2) = ' ';
						dsOk();
					}
				}
				else
				{
					sqlPos = pos;
					if (!seekToEndOfMultiLineAnnotation(pos))
						dsFailed(1, "can not find end of annotation @" << std::string(sqlPos, min(strlen(sqlPos), 50)));
					*(pos - 1) = ' ';
					*(pos - 2) = ' ';
					dsOk();
				}
			}
			else
			{
				if (!seekToEndOfMultiLineAnnotation(sqlPos))
					dsFailed(1, "can not find end of annotation @" << std::string(sqlPos, min(strlen(sqlPos), 50)));
				dsOk();
			}
		}


	

		inline DS matchLiteralType(literalType type, token*& t)
		{
			if (unlikely(t->type != tokenType::literal))
				dsReturnCode(1);
			if (unlikely(static_cast<literal*>(t)->lType != type))
			{
				if (m_litTrans.canTrans(static_cast<literal*>(t)->lType, type))
					dsOk();
				else
					dsReturnCode(1);
			}
			dsOk();
		}

		inline DS matchLiteral(sqlParserStack* stack, char*& pos, sqlHandle* handle, literalType type, token*& t)
		{
			dsReturnIfFailed(matchToken(stack, t, pos, true, true));
			dsReturnCode(matchLiteralType(type, t));
		}

		inline DS matchAllLiteralToken(token* t)
		{
			if (unlikely(t->type != tokenType::literal))
				dsReturnCode(1);
			dsOk();
		}

		inline DS matchAllLiteral(sqlParserStack* stack, char*& pos, sqlHandle* handle, token*& t)
		{
			nextWordPos(pos);
			dsReturnIfFailed(matchToken(stack, t, pos, true, true));
			if (unlikely(t->type != tokenType::literal))
				dsReturnCode(1);
			dsOk();
		}

		inline DS matchAnyString(token* t)
		{
			if (unlikely(t->type != tokenType::literal && t->type != tokenType::keyword))
				dsReturnCode(1);
			dsOk();
		}

		inline DS matchAnyStringLiteral(sqlParserStack* stack, char*& pos, sqlHandle* handle, token*& t)
		{
			nextWordPos(pos);
			dsReturnIfFailed(matchToken(stack, t, pos, true, true));
			dsReturnCode(matchAnyString(t));
		}

	public:
		DS matchToken(sqlParserStack* stack, token*& t, char*& pos, bool needMatchExpression, bool needMatchValue)
		{
			char* start = pos;
			char c = *pos;
			t = nullptr;
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))//key word or identifier or function name
			{
				if (*(pos + 1) == '\'')
				{
					if (c == 'b' || c == 'B')
						dsReturnIfFailed(matchGeneralLiteral(stack, t, pos, [](char c) {return c == '0' || c == '1'; }, literalType::BIT_STRING));
					else if (c == 'x' || c == 'X')
						dsReturnIfFailed(matchGeneralLiteral(stack, t, pos, [](char c) {return ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')); }, literalType::HEX_STRING));
					else if (c == 'n' || c == 'N')
						dsReturnIfFailed(matchGeneralLiteral(stack, t, pos, [](char c) {return ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')); }, literalType::HEX_STRING));
					else
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else
				{
					while (!KEY_CHAR[*pos])
						pos++;
					str s(start, pos - start);
					keyWordMap::iterator iter = m_keyWords.find(&s);
					if (iter != m_keyWords.end())
					{
						keyWordInfo* k = iter->second;
						if (k == nullptr || !k->couldBeValue)
						{
							if (needMatchValue)
								dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
							t = (token*)stack->arena.AllocateAligned(sizeof(token));
							t->type = tokenType::keyword;
							t->value.assign(s);
							dsOk();
						}
						if (k->func != nullptr)
						{
							dsReturnIfFailed(k->func(stack, t, pos));
						}
						else
						{
							t = (token*)stack->arena.AllocateAligned(sizeof(token));
							t->type = tokenType::keyword;
							t->value.assign(s);
						}
					}
					else
					{
						nextWordPos(pos);
						if (*pos == '(')//function
						{
							t = (token*)stack->arena.AllocateAligned(sizeof(token));
							t->type = tokenType::identifier;
							t->value.assign(s);
							pos++;
							dsReturnIfFailed(matchFunctionArgvList(stack, t, pos));
						}
						else
						{
							t = (token*)stack->arena.AllocateAligned(sizeof(token));
							t->type = tokenType::identifier;
							t->value.assign(s);
						}
					}
				}
				if (needMatchExpression)
				{
					operatorSymbol* op = m_opTress.match(pos);
					if (op != nullptr)//expression
					{
						dsReturn(matchExpression(stack, t, op, pos));
					}
				}
				dsOk();
			}
			else if (c >= '0' && c <= '9') //number
			{
				dsReturnIfFailed(matchNumber(stack, t, pos));
				if (needMatchExpression)
					dsReturn(tryMatchExpression(stack, t, pos));
				else
					dsOk();
			}
			else if (c == '\'')//char string
			{
				dsReturnIfFailed(matchString(stack, t, pos));
				if (needMatchExpression)
					dsReturn(tryMatchExpression(stack, t, pos));
				else
					dsOk();
			}
			else if (c == '"' || c == '`')//delimited identifier
			{
				if (c == '"' && m_doubleQuoteCanBeString)
					dsReturnIfFailed(matchString(stack, t, pos));
				else
					dsReturnIfFailed(matchDelimitedIdentifier(stack, t, pos));
				if (needMatchExpression)
					dsReturn(tryMatchExpression(stack, t, pos));
				else
					dsOk();
			}
			else if (c == '+' || c == '-')//number
			{
				dsReturnIfFailed(matchNumber(stack, t, pos));
				if (needMatchExpression)
					dsReturn(tryMatchExpression(stack, t, pos));
				else
					dsOk();
			}
			else if (needMatchExpression)
			{
				char* p = pos;
				operatorSymbol* op = m_opTress.match(p);
				if (op != nullptr && (op->op->optType == OPERATION_TYPE::MATHS || op->op->optType == OPERATION_TYPE::LOGIC) && !op->op->hasLeftValues)//no left value expression
				{
					pos = p;
					dsReturn(matchExpression(stack, t, op, pos));
				}
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			else
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}
		inline void nextWordPos(char*& pos)
		{
			while (*pos != '\0' && isSeparator(pos))
				pos++;
		}
		DS matchExpression(sqlParserStack* stack, token*& t, operatorSymbol* op, char*& pos)
		{
			uint32_t vt = stack->valueStack.size();
			uint32_t ot = stack->opStack.size();
			bool matchRightValue = false;
			token* l = nullptr, * r = nullptr;
			if (t != nullptr)
			{
				if (unlikely(!stack->valueStack.push(t)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				l = t;
				matchRightValue = true;
				goto MATCH_OP;
			}
			if (op != nullptr)
			{
				if (unlikely(!stack->opStack.push(op)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				goto MATCH_RVALUE;
			}
		MATCH_LVAUE:
			nextWordPos(pos);
			while (*pos == '(')
			{
				if (unlikely(!stack->opStack.push(m_lbr)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				pos++;
				nextWordPos(pos);
			}
			op = m_opTress.match(pos);
			if (op != nullptr)
			{
				//no left value expression
				if (op->op->hasLeftValues)
					stack->opStack.push(op);
				else
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			dsReturnIfFailed(matchToken(stack, l, pos, false, true));
			if (unlikely(!stack->valueStack.push(l)))
				dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
		MATCH_OP:
			nextWordPos(pos);
			op = m_opTress.match(pos);
			if (op == nullptr)//end
				goto END;
			//after a value ,expect is ) or other math or logic operatorSymbol
			if (op == m_rbr)
			{
				op = nullptr;
				while (stack->opStack.size() > ot && (op = stack->opStack.top(), op != m_lbr))
				{
					stack->opStack.pop();
					if (unlikely(!stack->valueStack.push(op)))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				}
				if (op == nullptr || op->op->hasRightValue != LEFT_BRACKET)
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				stack->opStack.pop();
				//after ) ,we need to search next op
				goto MATCH_OP;
			}
			else if (op->op->hasLeftValues && op->op->hasRightValue && (op->op->optType == OPERATION_TYPE::LOGIC || op->op->optType == OPERATION_TYPE::MATHS))
			{
				while (stack->opStack.size() > ot && stack->opStack.top()->op->optType != LEFT_BRACKET && stack->opStack.top()->op->priority <= op->op->priority)
				{
					if (unlikely(!stack->valueStack.push(stack->opStack.popAndGet())))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				}
				if (unlikely(!stack->opStack.push(op)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
			}
			else
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		MATCH_RVALUE:
			nextWordPos(pos);
			//after operatorSymbol , expect is ( or value
			if (*pos == '(')
			{
				if (unlikely(!stack->opStack.push(m_lbr)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				pos++;
				//goto child expression in BRACKET
				goto MATCH_LVAUE;
			}
			dsReturnIfFailed(matchToken(stack, r, pos, false, true));
			if (unlikely(!stack->valueStack.push(r)))
				dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
			goto MATCH_OP;
		END:
			while (stack->opStack.size() > ot)
			{
				op = stack->opStack.popAndGet();
				if (op == m_lbr)
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				if (unlikely(!stack->valueStack.push(op)))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
			}
			expression* e = (expression*)stack->arena.AllocateAligned(sizeof(expression) + (stack->valueStack.size() - vt) * sizeof(token*));
			e->type = tokenType::literal;
			e->lType = literalType::EXPRESSION;
			e->value.assign(nullptr, 0);
			e->count = stack->valueStack.size() - vt;
			for (int i = e->count - 1; i >= 0; i--)
				e->valueList[i] = stack->valueStack.popAndGet();
			token* last = e->valueList[e->count - 1];
			if (last->type == tokenType::symbol)
			{
				if (static_cast<operatorSymbol*>(last)->op->optType == OPERATION_TYPE::LOGIC)
					e->booleanOrValue = true;
				else
					e->booleanOrValue = false;
			}
			else
				e->booleanOrValue = false;//value
			t = e;
			dsOk();
		}

		DS matchNumber(sqlParserStack* stack, token*& t, char*& pos)
		{
			bool isFloat = false;
			const char* start = pos;
			if (*pos == '+' || *pos == '-')
				pos++;
			if (*pos < '0' || *pos>'9')
				dsReturnCode(1);
			pos++;
			while (*pos >= '0' && *pos <= '9')
				pos++;
			if (*pos == '.')
			{
				isFloat = true;
				pos++;
				if (*pos < '0' || *pos  >'9')
					dsReturnCode(1);
				pos++;
				while (*pos >= '0' && *pos <= '9')
					pos++;
			}
			if (*pos == 'e' || *pos == 'E')
			{
				pos++;
				if (*pos == '+' || *pos == '-')
					pos++;
				if (*pos < '0' || *pos  >'9')
					dsReturnCode(1);
				pos++;
				while (*pos >= '0' && *pos <= '9')
					pos++;
			}
			if (!KEY_CHAR[*pos])
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			literal* l = (literal*)stack->arena.AllocateAligned(sizeof(literal));
			l->type = tokenType::literal;
			l->value.assign(start, pos - start);
			l->lType = isFloat ? literalType::FLOAT_NUMBER : literalType::INT_NUMBER;
			t = l;
			dsOk();
		}
		DS matchString(sqlParserStack* stack, token*& t, char*& pos)
		{
			char symb = *pos++;
			const char* start = pos;
			bool backslash = false;
			while (*pos != '\0')
			{
				if (unlikely(*pos == '\\'))
				{
					backslash = !backslash;
				}
				else if (*pos == symb)
				{
					if (likely(!backslash))
					{
						t = (token*)stack->arena.AllocateAligned(sizeof(literal));
						t->type = tokenType::literal;
						static_cast<literal*>(t)->lType = literalType::CHARACTER_STRING;
						t->value.assign(start, pos - start);
						pos++;
						dsOk();
					}
					else
						backslash = false;
				}
				pos++;
			}
			dsReturnCode(1);
		}
		DS matchFunctionArgvList(sqlParserStack* stack, token*& t, char*& pos)
		{
			int top = stack->valueStack.size();
			for (;;)
			{
				while (isSeparator(pos))
					pos++;
				if (*pos == ')')
					break;
				token* v;
				dsReturnIfFailed(matchToken(stack, v, pos, true, true));
				stack->valueStack.push(v);
				if (*pos == ',')
				{
					pos++;
					continue;
				}
				else if (*pos == ')')
				{
					break;
				}
				else
					dsReturnCode(1);
			}
			pos++;
			int size = stack->valueStack.size() - top;
			function* f = (function*)stack->arena.AllocateAligned(sizeof(function) + (size > 1 ? (sizeof(token*) * (size - 1)) : 0));
			f->count = size;
			while (size > 0)
				f->argv[--size] = stack->valueStack.popAndGet();
			f->lType = literalType::FUNCTION;
			f->name.assign(t->value);
			f->value.assign(t->value.pos, pos - t->value.pos);
			t = f;
			dsOk();
		}
		DS matchFunction(sqlParserStack* stack, token*& t, char*& pos)
		{
			dsReturnIfFailed(matchNonDelimitedIdentifier(stack, t, pos, true));
			while (isSeparator(pos))
				pos++;
			if (*pos != '(')
				dsReturnCode(1);
			dsReturn(matchFunctionArgvList(stack, t, pos));
		}
		DS matchNonDelimitedIdentifier(sqlParserStack* stack, token*& t, char*& pos, bool funcName)
		{
			const char* start = pos;
			char c = *pos;
			//first char must be a-z or U+0080 .. U+FFFF
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) && c < 0x80)
				dsReturnCode(1);
			//a-z A-Z 0-9 $#_
			while ((c = *pos) && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '1' && c <= '9') || c == '$' || c == '#' || c == '_' || c > 0x80))
				pos++;
			if (funcName)
			{
				t = (token*)stack->arena.AllocateAligned(sizeof(token));
				t->value.assign(start, pos - start);
			}
			else
			{
				if (t == nullptr)
				{
					t = (identifier*)stack->arena.AllocateAligned(sizeof(identifier));
					static_cast<identifier*>(t)->init(start, pos - start);
				}
				else
				{
					static_cast<identifier*>(t)->add(start, pos - start);
				}
			}
			dsOk();
		}

		DS matchDelimitedIdentifier(sqlParserStack* stack, token*& t, char*& pos)
		{
			if (*pos != m_identifierQuote)
				dsFailed(1, "must be delimited identifier  @" << std::string(pos, min(strlen(pos), 50)));
			pos++;
			const char* start = pos;
			bool backslash = false;
			while (*pos != '\0')
			{
				if (unlikely(*pos == '\\'))
					backslash = !backslash;
				else if (*pos == m_identifierQuote)
				{
					if (likely(!backslash))
					{
						if (t == nullptr)
						{
							t = (identifier*)stack->arena.AllocateAligned(sizeof(identifier));
							static_cast<identifier*>(t)->init(start, pos - start);
						}
						else
						{
							static_cast<identifier*>(t)->add(start, pos - start);
						}
						pos++;
						dsOk();
					}
					else
						backslash = false;
				}
				pos++;
			}
			dsFailed(1, "unexpect unfinished delimited identifier @" << std::string(pos, min(strlen(pos), 50)));
		}

		DS matchIdentifier(sqlParserStack* stack, token*& t, char*& pos)
		{
			t = nullptr;
			for (char i = 0; i < 3; i++)
			{
				DS s;
				if (*pos == m_identifierQuote)
					dsReturnIfNotOk(matchDelimitedIdentifier(stack, t, pos));
				else
					dsReturnIfNotOk(matchNonDelimitedIdentifier(stack, t, pos, false));
				if (*pos != '.')
					break;
			}
			dsOk();
		}

		DLL_EXPORT virtual DS parseOneSentence(sqlHandle* handle, char*& sqlStr, sql* s);
	public:
		DLL_EXPORT DS parse(sqlHandle* handle, char* sqlStr, DS(*handleFunc)(sqlHandle*))
		{
			char* pos = sqlStr;
			nextWordPos(pos);
			for (;;)
			{
				sql s;
				dsReturnIfFailed(parseOneSentence(handle, pos, &s));
				if(handleFunc != nullptr)
					dsReturnIfFailed(handleFunc(handle));
				nextWordPos(pos);
				if (*pos == ';')
					pos++;
				else if (*pos == '\0')
					dsOk();
				else
					dsFailed(1, "unexpect sql @" << std::string(pos, min(strlen(pos), 50)));
			}
		}
	};
}