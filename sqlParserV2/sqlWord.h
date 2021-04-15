#pragma once
#if 0
#include <string>
#include <stdint.h>
#include "glog/logging.h"
#include "util/winString.h"
#include "util/status.h"
#include "util/nameCompare.h"
#include "util/sparsepp/spp.h"
#include "operationInfo.h"
#include "errorCode.h"
#include "thread/threadLocal.h"
#include "memory/objectCache.h"
#include "sqlValue.h"
#include "util/arena.h"
#include "sql.h"
namespace SQL_PARSER {
	struct grammarTreeNode;
	struct grammarBean {
		bool isStaticWord;
		stringWord* staticWord;
		SQL_VALUE_TYPE type;
		parserFuncType func;
		grammarBean* next;
		bool optional;
		inline bool match(const sqlValue * value)
		{
			if (isStaticWord)
				return staticWord->match(value);
			return SQL_TYPE_MATCH_MATRIX[static_cast<char>(type)][static_cast<char>(this->type)];
		}

	};
	struct grammarBlock {
		parserFuncType beginFunc;
		parserFuncType endFunc;
		grammarBean* head;
		bool optional;
		bool leaf;
		bool loop;
		stringWord* loopSeparatorWord;
		//if next is a loop block ,use nextBlock, if next is branch,use child, at least  one of nextBlock and child must be null
		grammarBlock * nextBlock;
		grammarTreeNode* child;
	};
	typedef spp::sparse_hash_map<const sqlValue*, grammarBlock*, wordCompare, wordCompare> grammarBlockHashMap;
	typedef spp::sparse_hash_map<const sqlValue*, grammarTreeNode*, wordCompare, wordCompare> grammarHashMap;
	typedef spp::sparse_hash_set<const sqlValue*, wordCompare, wordCompare> grammarHashSet;


	struct grammarTreeNode {
		sqlWord* start;
		parserFuncType func;
		grammarBlockHashMap staticWordMap;
		grammarBlock* fieldNameBlock;
		grammarBlock* numberBlock;
		grammarBlock* stringBlock;
		grammarBlock* functionBlock;
		grammarBlock* logicExpBlock;
		grammarBlock* valueExpBlock;
		bool leaf;
		bool empty;
	};
	enum class PARSE_STATUS {
		NEED_LEFT_VALUE,
		NEED_OPERATOR,
		NEED_RIGHT_VALUE,
		FIELD_LIST_VALUE,
		FIELD_LIST_VALUE_SEP
	};
	class grammarTree {
	private:
		template<typename T>
		struct stack {
			T s[MAX_EXPRESSION_OPERATION_COUNT];
			uint32_t t;
			stack()
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
			stack<operatorSqlValue*> opStack;
			stack<sqlValue*> valueStack;
			stack<uint32_t> childOpPosStack;
			stack<uint32_t> childValuePosStack;
			stack<PARSE_STATUS> statusStack;
			leveldb::Arena arena;
		};
		grammarHashMap m_tree;
		grammarHashSet m_keywords;
		operationInfoTree m_opTress;
		threadLocal<sqlParserStack> m_stack;
		/*
		objectCache<sqlValue> m_sqlValueCache;
		objectCache<fieldSqlValue> m_fieldSqlValueCache;
		objectCache<expressionSqlValue> m_expSqlValueCache;
		objectCache<functionSqlValue> m_funcSqlValueCache;
		objectCache<sqlValueFuncPair> m_valueFuncPairCache;
		objectCache<sql> m_sqlCache;
		*/
		operatorSqlValue* m_lBrac;
		operatorSqlValue* m_rBrac;
		char m_tableNameQuote;
		SQL_VALUE_TYPE m_tableNameQuoteType;
		bool m_doubleQuoteCanBeString;
	private:
		void initKeyWords();
		inline DS nextQuoteWord(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			char quote = *pos;
			const char* end = pos + 1;
			bool hasEscape = false;
			while (true)
			{
				if (*end == '\\')
					hasEscape = true;
				if (*end == quote)
				{
					int escapeCount = 1;
					for (; *(end - escapeCount) == '\\'; escapeCount++);
					if (escapeCount & 0x01)
						break;
				}
				end++;
			}

			sqlValue* value = (sqlValue*)s->arena.AllocateAligned(sizeof(sqlValue*));
			value->type = quote == '\'' ? SQL_VALUE_TYPE::SINGLE_QUOTE_WORD : (quote == '`' ? SQL_VALUE_TYPE::BACK_QUOTE_WORD : SQL_VALUE_TYPE::DOUBLE_QUOTE_WORD);
			if (!hasEscape)
			{
				value->pos = pos + 1;
				value->length = end - pos - 1;
				if (!s->valueStack.push(value))
					dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
				pos = end;
				dsOk();
			}
			else
			{
				int32_t length = end - pos - 1;
				char* str = s->arena.AllocateAligned(length);
				uint32_t realSize = 0;
				for (int32_t soffset = 1; soffset < length; soffset++)
				{
					if (unlikely(pos[soffset] == '\\'))
					{
						if (likely(soffset < length))
						{
							switch (pos[soffset + 1])
							{
							case '\\':
								str[realSize++] = '\\';
								soffset++;
								break;
							case 't':
								str[realSize++] = '\t';
								soffset++;
								break;
							case '\'':
								str[realSize++] = '\'';
								soffset++;
								break;
							case '"':
								str[realSize++] = '"';
								soffset++;
								break;
							case 'r':
								str[realSize++] = '\r';
								soffset++;
								break;
							case 'n':
								str[realSize++] = '\n';
								soffset++;
								break;
							case '0':
								str[realSize++] = '\0';
								soffset++;
								break;
							default:
								str[realSize++] = pos[soffset + 1];
								soffset++;
								break;
							}
						}
						else
							dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					}
					else
					{
						str[realSize++] = pos[soffset];
					}
					value->pos = str;
					value->length = end - pos - 1;
					if (!s->valueStack.push(value))
						dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
					pos = end;
					dsOk();
				}
			}
		}
		inline DS nextString(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			const char* start = pos;
			while (!KEY_CHAR[*pos])
				pos++;
			sqlValue* v = (sqlValue*)s->arena.AllocateAligned(sizeof(sqlValue*));
			v->pos = start;
			v->length = pos - start;
			v->type = SQL_VALUE_TYPE::WORD;
			if (!s->valueStack.push(v))
				dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
			dsOk();
		}

		inline bool ull(const char*& pos)
		{
			char c = *pos;
			if (c == 'U' || c == 'u')
			{
				pos++;
				if (*pos == 'L' || *pos == 'l')
				{
					pos++;
					if (*pos == 'L' || *pos == 'l')
						pos++;
				}
				return true;
			}
			else if (c == 'L' || c == 'l')
			{
				pos++;
				if (*pos == 'L' || *pos == 'l')
					pos++;
				return true;
			}
			return false;
		}
		inline DS numberEnd(const char*& pos)
		{
			char c = *pos;
			if (c == '\0' || c == ' ' || c == '\t' || c == '\n' || c == '\r')
				dsOk();
			else if (m_opTress.match(pos) != nullptr)
				dsOk();
			else
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}

		inline DS nextHexNumber(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			const char* start = pos - 2;
			char c;
			for (; (c = *pos) != '\0'; pos++)
			{
				if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || c >= 'a' && c <= 'f')
					continue;
				ull(pos);
				break;
			}
			dsReturn(numberEnd(pos));
		}
		inline DS nextBinaryNumber(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			const char* start = pos - 2;
			char c;
			for (; (c = *pos) != '\0'; pos++)
			{
				if (c == '0' || c == '1')
					continue;
				ull(pos);
				break;
			}
			dsReturn(numberEnd(pos));
		}
		inline DS nextOctalNumber(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			const char* start = pos - 2;
			char c;
			for (; (c = *pos) != '\0'; pos++)
			{
				if (c >= '0' && c <= '7')
					continue;
				ull(pos);
				break;
			}
			dsReturn(numberEnd(pos));
		}
		inline DS nextNumber(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{
			bool isFloat = false;
			int exponent = -1;
			const char* start = pos;
			if (*pos == '-')
				pos++;
			char c;
			for (; (c = *pos) != '\0'; pos++)
			{
				if (c >= '0' && c <= '9')
					continue;
				else if (c == '.')
				{
					if (isFloat || exponent > 0)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					isFloat = true;
				}
				else if (c == 'X' || c == 'x')//hex
				{
					if (*start == '0' && pos == start + 1)
					{
						dsReturnIfFailed(nextHexNumber(h, s, pos));
						break;
					}
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else if (c == 'B' || c == 'b')//binary
				{
					if (*start == '0' && pos == start + 1)
					{
						dsReturnIfFailed(nextBinaryNumber(h, s, pos));
						break;
					}
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else if (c == 'O' || c == 'o')//octal
				{
					if (*start == '0' && pos == start + 1)
					{
						dsReturnIfFailed(nextOctalNumber(h, s, pos));
						break;
					}
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				}
				else if (c == 'e' || c == 'E')//exponent
				{
					if (exponent > 0)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					if (0 == (exponent = pos - start))
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					isFloat = true;
				}
				else if (c == 'f' || c == 'F')
				{
					isFloat = true;
					dsReturnIfFailed(numberEnd(++pos));
					break;
				}
				else if (ull(pos))
				{
					if (isFloat)
						dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
					dsReturnIfFailed(numberEnd(pos));
					break;
				}
				else
				{
					dsReturnIfFailed(numberEnd(pos));
					break;
				}
			}
			if (exponent > 0 && pos - start <= exponent)
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			if (isFloat && *(pos - 1) == '.')
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			sqlValue* value = (sqlValue*)s->arena.AllocateAligned(sizeof(sqlValue*));
			value->pos = start;
			value->length = pos - start;
			value->type = isFloat ? SQL_VALUE_TYPE::FLOAT_NUMBER : SQL_VALUE_TYPE::INTEGER_NUMBER;
			if (!s->valueStack.push(value))
				dsFailedAndLogIt(errorCode::OVER_LIMIT, "expression operation count over limit :" << pos, ERROR);
		}
		inline DS nextOperator(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{

		}
		inline DS parseChildField(sqlHandle* h, sqlParserStack* s, const char*& pos)
		{

		}
		inline DS parseLBrac(sqlHandle* h, sqlParserStack* s, PARSE_STATUS& status, const char*& pos)
		{
			if (status == PARSE_STATUS::NEED_LEFT_VALUE)
			{
				s->opStack.push(m_lBrac);
			}
			else if (status == PARSE_STATUS::NEED_RIGHT_VALUE)
			{
				s->statusStack.push(status);
				status = PARSE_STATUS::NEED_LEFT_VALUE;
				s->opStack.push(m_lBrac);
			}
			else if (status == PARSE_STATUS::NEED_OPERATOR)
			{
				if (s->valueStack.top()->type == SQL_VALUE_TYPE::WORD)
				{
					sqlValue* v = s->valueStack.top();
					functionSqlValue* f = (functionSqlValue*)s->arena.AllocateAligned(sizeof(functionSqlValue*));
					f->pos = v->pos;
					f->length = v->length;
					s->statusStack.push(status);
					s->childOpPosStack.push(s->opStack.size());
					s->childValuePosStack.push(s->valueStack.size());
					s->valueStack.push(f);
					status = PARSE_STATUS::FIELD_LIST_VALUE;
				}
			}
			else if (status == PARSE_STATUS::FIELD_LIST_VALUE)
			{
				s->opStack.push(m_lBrac);
				s->statusStack.push(status);
				status = PARSE_STATUS::NEED_LEFT_VALUE;
			}
			else
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}

		inline DS parseRBrac(sqlHandle* h, sqlParserStack* s, PARSE_STATUS& status, const char*& pos)
		{
			if (status == PARSE_STATUS::FIELD_LIST_VALUE)
			{
				sqlValue* v = s->valueStack.top();
				if (v->type != SQL_VALUE_TYPE::FUNCTION)
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			else
			{
				operatorSqlValue* op = nullptr;
				while (!s->opStack.empty() && (op = s->opStack.top(), op->op->optType != LEFT_BRACKET))
				{
					s->opStack.pop();
					s->valueStack.push(op);
				}
				if (op == nullptr || op->op->optType != LEFT_BRACKET)
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}
			status = s->statusStack.empty() ? PARSE_STATUS::NEED_LEFT_VALUE : s->statusStack.popAndGet();
			dsOk();
		}

		inline sqlParserStack* getStack()
		{
			sqlParserStack* s = m_stack.get();
			if (unlikely(s == nullptr))
				m_stack.set(s = new sqlParserStack());
			return s;
		}

		inline DS endOfValue(sqlHandle* handle, sqlParserStack* s, const char* pos, sqlValue*& value, PARSE_STATUS status)
		{
			if (s->valueStack.size() == 1 && s->opStack.empty())
			{
				value = s->valueStack.popAndGet();
				dsOk();
			}
			//end of word, stack must be empty, for example [i>(a + b(1,c))  and ],[and] must be out of expression
			if (!s->statusStack.empty() || !s->childValuePosStack.empty() || !s->childOpPosStack.empty())
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);

			expressionSqlValue* exp = (expressionSqlValue*)s->arena.AllocateAligned(sizeof(expressionSqlValue));
			exp->valueStack = (sqlValue**)s->arena.AllocateAligned(sizeof(sqlValue*) * (s->valueStack.size() + s->opStack.size()));
			memcpy(exp->valueStack, s->valueStack.s, sizeof(sqlValue*) * s->valueStack.size());
			s->valueStack.t = 0;
			while (!s->opStack.empty())
				exp->valueStack[exp->count++] = s->opStack.popAndGet();
			value = exp;
			dsOk();
		}

		DS nextWord(sqlHandle* handle, sqlParserStack* s, sqlValue*& value, const char*& pos);
		void cleanHandle(struct sqlHandle* h)
		{
			h->tid = getThreadId();
			getStack()->arena.clear();
			h->sqlList = nullptr;
			h->tail = 0;
			h->sqlCount = 0;
		}
		inline bool macthBlock(sqlValue * value,grammarTreeNode * node, grammarBlock *& block)
		{
			switch (value->type)
			{
			case SQL_VALUE_TYPE::WORD:
			{
				grammarBlockHashMap::iterator i = node->staticWordMap.find(value);
				if (i == node->staticWordMap.end())
				{
					if (node->fieldNameBlock != nullptr)
					{
						block = node->fieldNameBlock;
						return true;
					}
					else
						return false;
				}
				else
				{
					block = i->second;
					return true;
				}
			}
			case SQL_VALUE_TYPE::BACK_QUOTE_WORD:
				if (m_tableNameQuoteType == SQL_VALUE_TYPE::BACK_QUOTE_WORD && node->fieldNameBlock != nullptr)
				{
					block = node->fieldNameBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::DOUBLE_QUOTE_WORD:
				if (m_tableNameQuoteType == SQL_VALUE_TYPE::DOUBLE_QUOTE_WORD && node->fieldNameBlock != nullptr)
				{
					block = node->fieldNameBlock;
					return true;
				}
				else if (m_doubleQuoteCanBeString && node->stringBlock != nullptr)
				{
					block = node->stringBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::SINGLE_QUOTE_WORD:
				if (m_tableNameQuoteType == SQL_VALUE_TYPE::SINGLE_QUOTE_WORD && node->fieldNameBlock != nullptr)
				{
					block = node->fieldNameBlock;
					return true;
				}
				else if (node->stringBlock != nullptr)
				{
					block = node->stringBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::INTEGER_NUMBER:
			case SQL_VALUE_TYPE::FLOAT_NUMBER:
				if (node->numberBlock != nullptr)
				{
					block = node->numberBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::FIELD_NAME_WORD:
				if (node->fieldNameBlock != nullptr)
				{
					block = node->fieldNameBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::FUNCTION:
				if (node->numberBlock != nullptr)
				{
					block = node->numberBlock;
					return true;
				}
				else if (node->stringBlock != nullptr)
				{
					block = node->stringBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::VALUE_EXPRESSION:
				if (node->valueExpBlock != nullptr)
				{
					block = node->valueExpBlock;
					return true;
				}
				else
					return false;
			case SQL_VALUE_TYPE::LOGIC_EXPRESSION:
				if (node->valueExpBlock != nullptr)
				{
					block = node->valueExpBlock;
					return true;
				}
				else if (node->logicExpBlock != nullptr)
				{
					block = node->logicExpBlock;
					return true;
				}
				else
					return false;
			default:
				return false;
			}
			return false;
		}
		DS parse(struct sqlHandle* h, const char* sqlString)
		{
			sqlParserStack* s = getStack();
			h->tid = getThreadId();
			const char* pos = sqlString;
			sqlValue* value = nullptr;
			dsReturnIfFailed(nextWord(h, s, value, pos));
			if (value == nullptr)
				dsOk();
			if (value->type != SQL_VALUE_TYPE::WORD)
				dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);

			h->sqlList = (sql*)s->arena.AllocateAligned(sizeof(sql));
			h->tail = h->sqlList;
			h->tail->init(h->sqlCount, h->currentDatabase.empty() ? nullptr : h->currentDatabase.c_str());
			h->tail->head = (sqlValueFuncPair*)s->arena.AllocateAligned(sizeof(sqlValueFuncPair));
			h->tail->tail = h->tail->head;

			for (;;)
			{
				grammarHashMap::iterator iter = m_tree.find(value);
				if (iter == m_tree.end())
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
				grammarTreeNode* node = iter->second;
				grammarBlock* block;
				const grammarBean* bean;
				if (node->func != nullptr)
				{
					sqlValueFuncPair *pair = (sqlValueFuncPair*)s->arena.AllocateAligned(sizeof(sqlValueFuncPair));
					pair->init(value, node->func);
					h->tail->add(pair);
				}
				for (;;)
				{
					if (node->empty)
						goto FINISH;
					if(node->functionBlock == nullptr)
					dsReturnIfFailed(nextWord(h, s, value, pos));
					if (!macthBlock(value, node, block))
					{
						if (node->leaf)
							goto FINISH;
						else
							dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << value->pos, ERROR);
					}
					if (block->beginFunc != nullptr)
					{
						sqlValueFuncPair* pair = (sqlValueFuncPair*)s->arena.AllocateAligned(sizeof(sqlValueFuncPair));
						pair->init(value, block->beginFunc);
						h->tail->add(pair);
					}
					for(;;)
					{
						dsReturnIfFailed(nextWord(h, s, value, pos));
						for (bean = block->head; bean != nullptr; bean = bean->next)
						{
							if (value == nullptr)//end
							{
								for (;;)
								{
									if (bean->optional)
									{
										if (bean->next == nullptr)
										{
											if (block->leaf)
												goto FINISH;
											else
												dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
										}
										bean = bean->next;
										continue;
									}
									else
										dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
								}
							}
							if ((bean->isStaticWord && bean->staticWord->match(value)) || SQL_TYPE_MATCH_MATRIX[static_cast<char>(bean->type)][static_cast<char>(value->type)])
							{
								if (bean->func != nullptr)
								{
									sqlValueFuncPair* pair = (sqlValueFuncPair*)s->arena.AllocateAligned(sizeof(sqlValueFuncPair));
									pair->init(value, bean->func);
									h->tail->add(pair);
								}
								dsReturnIfFailed(nextWord(h, s, value, pos));
							}
							else if (bean->optional)
								continue;
							else
								dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
						}
						if (block->loop)
						{
							if (block->loopSeparatorWord != nullptr)
							{

							}
						}
					}

					if (block->endFunc != nullptr)
					{
						sqlValueFuncPair* pair = (sqlValueFuncPair*)s->arena.AllocateAligned(sizeof(sqlValueFuncPair));
						pair->init(nullptr, block->endFunc);
						h->tail->add(pair);
					}
				}
			FINISH:

			}
			
		}
	};
}
#endif
