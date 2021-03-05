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
namespace SQL_PARSER {
	struct sqlParserStack {
		sqlStack<operatorSymbol*> opStack;
		sqlStack<token*> valueStack;
		leveldb::Arena arena;
		inline void clear()
		{
			opStack.t = 0;
			valueStack.t = 0;
			arena.clear();
		}
	};
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

		template<typename T>
		inline dsStatus& matchGeneralLiteral(sqlParserStack* stack, token*& t, const char*& pos, T f, literalType type)
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
		inline dsStatus& tryMatchExpression(sqlParserStack* stack, token*& t, const char*& pos)
		{
			nextWordPos(pos);
			const char* p = pos;
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
			const char* pos = "(";
			m_lbr = m_opTress.match(pos);
			pos = ")";
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

		dsStatus& matchToken(sqlParserStack* stack, token*& t, const char*& pos, bool needMatchExpression, bool needMatchValue)
		{
			const char* start = pos;
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
				const char* p = pos;
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
		inline void nextWordPos(const char*& pos)
		{
			while (isSeparator(pos))
				pos++;
		}
		dsStatus& matchExpression(sqlParserStack* stack, token*& t, operatorSymbol* op, const char*& pos)
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

		dsStatus& matchNumber(sqlParserStack* stack, token*& t, const char*& pos)
		{
			bool isFloat = false;
			const char* start = pos;
			if (*pos == '+' || *pos == '-')
				pos++;
			if (*pos < '0' || *pos>'9')
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			pos++;
			while (*pos >= '0' && *pos <= '9')
				pos++;
			if (*pos == '.')
			{
				isFloat = true;
				pos++;
				if (*pos < '0' || *pos  >'9')
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
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
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
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
		dsStatus& matchString(sqlParserStack* stack, token*& t, const char*& pos)
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
						t = (token*)stack->arena.AllocateAligned(sizeof(token));
						t->type = tokenType::identifier;
						t->value.assign(start, pos - start);
						pos++;
						dsOk();
					}
					else
						backslash = false;
				}
				pos++;
			}
			dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}
		dsStatus& matchFunctionArgvList(sqlParserStack* stack, token*& t, const char*& pos)
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
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
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
		dsStatus& matchFunction(sqlParserStack* stack, token*& t, const char*& pos)
		{
			dsReturnIfFailed(matchNonDelimitedIdentifier(stack, t, pos, true));
			while (isSeparator(pos))
				pos++;
			if (*pos != '(')
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			dsReturn(matchFunctionArgvList(stack, t, pos));
		}
		dsStatus& matchNonDelimitedIdentifier(sqlParserStack* stack, token*& t, const char*& pos, bool funcName)
		{
			const char* start = pos;
			char c = *pos;
			//first char must be a-z or U+0080 .. U+FFFF
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) && c < 0x80)
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
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
		dsStatus& matchDelimitedIdentifier(sqlParserStack* stack, token*& t, const char*& pos)
		{
			if (*pos != m_identifierQuote)
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
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
			dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}

		dsStatus& matchIdentifier(sqlParserStack* stack, token*& t, const char*& pos)
		{
			for (char i = 0; i < 3; i++)
			{
				if (*pos == m_identifierQuote)
				{
					dsReturnIfFailed(matchDelimitedIdentifier(stack, t, pos));
				}
				else
				{
					dsReturnIfFailed(matchNonDelimitedIdentifier(stack, t, pos, false));
				}
				if (*pos != '.')
					break;
			}
			dsOk();
		}
		inline sqlParserStack* getStack()
		{
			sqlParserStack* s = m_stack.get();
			if (unlikely(s == nullptr))
				m_stack.set(s = new sqlParserStack());
			return s;
		}
		dsStatus& parseOneSentence(sqlHandle* handle, const char*& sqlStr, sql* s);
		dsStatus& parse(sqlHandle* handle, const char* sqlStr, dsStatus& (*handleFunc)(sqlHandle*))
		{
			const char* pos = sqlStr;
			for (;;)
			{
				nextWordPos(pos);
				if (*pos == ';')
				{
					pos++;
					continue;
				}
				else if (*pos == '\0')
					dsOk();
				sql s;
				dsReturnIfFailed(parseOneSentence(handle, pos, &s));
				dsReturnIfFailed(s.semanticAnalysis(handle));
				dsReturnIfFailed(handleFunc(handle));
			}
		}
	};
}