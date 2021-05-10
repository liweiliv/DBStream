#pragma once
#include <map>
#include <functional>
#include <string>
#include "util/json.h"
#include "util/status.h"
#include "errorCode.h"
#include "token.h"
namespace SQL_PARSER
{
	class lex {
	private:
		struct nodeInfo
		{
			bool loop;
			bool loopSeparatorIsOptional;
			const char* loopSeparator;
			bool optional;
			bool branch;
			bool pureIdentifier;
			const jsonArray* childJson;
			nodeInfo** child;
			int childCount;
			token* nodeToken;
			std::string funcName;
			std::string include;
			int ref;
			uint64_t nodeScanVersion;
			bool inOptimize;
			int funcId;
			const char* name;
			nodeInfo() :loop(false), loopSeparatorIsOptional(false), loopSeparator(nullptr), optional(false), branch(false), pureIdentifier(false),
				childJson(nullptr), child(nullptr), childCount(0), nodeToken(nullptr), ref(1), nodeScanVersion(0), inOptimize(false), funcId(0), name(nullptr)
			{
			}
			~nodeInfo()
			{
				if (nodeToken != nullptr)
					delete nodeToken;
				if (child != nullptr)
				{
					/*
					for (int i = 0; i < childCount; i++)
					{
						if (child[i] != nullptr && --child[i]->ref <= 0)
							delete child[i];
					}
					*/
					delete[] child;
				}
			}
			static bool compareLoop(const nodeInfo& first, const nodeInfo& second)
			{
				if (first.loop != second.loop)
					return false;
				if (first.loopSeparator != nullptr && second.loopSeparator != nullptr)
				{
					if (first.loopSeparatorIsOptional != second.loopSeparatorIsOptional
						|| strcmp(first.loopSeparator, second.loopSeparator) != 0)
						return false;
				}
				else if (first.loopSeparator != nullptr || second.loopSeparator != nullptr)
					return false;
				return true;
			}
			static bool compareNodeAndToken(const nodeInfo& first, const nodeInfo& second)
			{
				if (second.childCount != 1)
					return false;
				if (second.child[0]->loop && second.loop)
					return false;
				if (second.child[0]->loop)
				{
					if (!compareLoop(first, second))
						return false;
				}
				else if (second.child[0]->loop)
				{
					if (!compareLoop(first, *second.child[0]))
						return false;
				}
				if (!first.optional && (second.optional || second.child[0]->optional))
					return false;
				if (first.optional && !second.optional && !second.child[0]->optional)
					return false;
				if (second.child[0]->nodeToken == nullptr)
					return false;
				if (!first.nodeToken->compare(*second.child[0]->nodeToken))
					return false;
				if (first.funcName.compare(second.child[0]->funcName) != 0)
					return false;
				return true;
			}
			bool compare(const nodeInfo& n) const
			{
				if (optional != n.optional || branch != n.branch || loop != n.loop)
					return false;

				if (nodeToken != nullptr && n.nodeToken != nullptr)
				{
					if (!nodeToken->compare(*n.nodeToken))
						return false;
					if (funcName.compare(n.funcName) != 0)
						return false;
					if (!compareLoop(*this, n))
						return false;
				}
				else if (nodeToken != nullptr && n.child != nullptr)
				{
					return compareNodeAndToken(*this, n);
				}
				else if (child != nullptr && n.nodeToken != nullptr)
				{
					return compareNodeAndToken(n, *this);
				}
				else
				{
					if (!compareLoop(*this, n))
						return false;
					if (childCount != n.childCount)
						return false;
					for (int i = 0; i < childCount; i++)
					{
						if (!child[i]->compare(*n.child[i]))
							return false;
					}
				}
				return true;
			}
		};
		typedef std::pair<string, std::function<DS(const jsonValue*, nodeInfo*)> > parseNodeFuncPair;
		typedef std::map<string, std::function<DS(const jsonValue*, nodeInfo*)> > parseNodeFuncTree;
		parseNodeFuncTree m_parseNodeFuncs;
		std::map<std::string, nodeInfo*> m_allNodes;
		std::map<std::string, nodeInfo*> m_headNodes;
		nodeInfo m_root;
		std::list<nodeInfo*> m_containIncludeNodes;
		jsonObject* m_grammarTreeInJson;
		std::map<int, String> m_codes;
		std::map<std::string, int> m_staticTokens;

		uint64_t m_currentNodeScanVersion;
	public:
		lex() :m_grammarTreeInJson(nullptr), m_currentNodeScanVersion(0)
		{
			initParseNodeFuncs();
		}
	private:
		static inline DS getliteralType(const jsonValue* v, nodeInfo* n, literalType type)
		{
			if (v != nullptr)
			{
				if (v->t != JSON_TYPE::J_STRING)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				n->funcName = static_cast<const jsonString*>(v)->m_value;
			}
			literal* l;
			if (type == literalType::EXPRESSION)
				l = new expression();
			else if (type == literalType::FUNCTION)
				l = new function();
			else
				l = new literal();
			l->type = tokenType::literal;
			l->lType = type;
			n->nodeToken = l;
			dsOk();
		}
		void initParseNodeFuncs()
		{
			m_parseNodeFuncs.insert(parseNodeFuncPair("loop", [](const jsonValue* v, nodeInfo* n) ->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					if (strncasecmp(static_cast<const jsonString*>(v)->m_value.c_str(), "optinal ", 8) == 0)
					{
						n->loopSeparator = static_cast<const jsonString*>(v)->m_value.c_str() + 8;
						n->loopSeparatorIsOptional = true;
					}
					else
						n->loopSeparator = static_cast<const jsonString*>(v)->m_value.c_str();
				}
				n->loop = true;
				dsOk();
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("optional", [](const jsonValue* v, nodeInfo* n)->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_BOOL)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->optional = static_cast<const jsonBool*>(v)->m_value;
					dsOk();
				}
				else
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("or", [](const jsonValue* v, nodeInfo* n)->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_BOOL)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->branch = static_cast<const jsonBool*>(v)->m_value;
					dsOk();
				}
				else
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("child", [](const jsonValue* v, nodeInfo* n) ->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_ARRAY)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->childJson = static_cast<const jsonArray*>(v);
					dsOk();
				}
				else
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("include", [](const jsonValue* v, nodeInfo* n) ->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for include type is not string", ERROR);
					n->include = static_cast<const jsonString*>(v)->m_value;
					dsOk();
				}
				else
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for include is null", ERROR);
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("is head", [](const jsonValue* v, nodeInfo* n) ->DS {dsOk(); }));

			m_parseNodeFuncs.insert(parseNodeFuncPair("string value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::CHARACTER_STRING)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("int value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::INT_NUMBER)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("float value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::FLOAT_NUMBER)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("number value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::NUMBER_VALUE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("expression value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::EXPRESSION)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("bool expression value", [](const jsonValue* v, nodeInfo* n) ->DS {
				dsReturnIfFailed(getliteralType(v, n, literalType::EXPRESSION));
				static_cast<expression*>(n->nodeToken)->booleanOrValue = true;
				dsOk(); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("function value", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::FUNCTION)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("date value", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::DATE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("time value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::TIME)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("timestamp value", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::TIMESTAMP)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("bool value", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::BOOLEAN)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("any string", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::ANY_STRING)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("all values", [](const jsonValue* v, nodeInfo* n)->DS {dsReturn(getliteralType(v, n, literalType::ALL_VALUE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("identifier word", [](const jsonValue* v, nodeInfo* n)->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->funcName = static_cast<const jsonString*>(v)->m_value;
				}
				identifier* i = new identifier();
				i->type = tokenType::identifier;
				n->nodeToken = i;
				dsOk();
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("pure identifier word", [](const jsonValue* v, nodeInfo* n)->DS {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->funcName = static_cast<const jsonString*>(v)->m_value;
				}
				identifier* i = new identifier();
				i->type = tokenType::identifier;
				n->nodeToken = i;
				n->pureIdentifier = true;
				dsOk();
				}));

			//m_parseNodeFuncs.insert(parseNodeFuncPair("value", [](const jsonValue* v, nodeInfo* n) ->DS {dsReturn(getliteralType(v, n, literalType::UNKNOWN)); }));
		}

		//a-z,space,tab,_ allowed
		inline bool isLowCaseString(const char* v)
		{
			const char* pos = v;
			char c;
			while ((c = *pos++) != '\0')
			{
				if (c <= 'z' && c >= 'a')
					continue;
				if (c == ' ' || c == '\t' || c == '_')
					continue;
				return false;
			}
			return true;
		}

		//A-Z,_ allowed
		inline bool isKeyWord(const char* v)
		{
			const char* pos = v;
			char c;
			while ((c = *pos++) != '\0')
			{
				if (c <= 'Z' && c >= 'A')
					continue;
				if (c == '_')
					continue;
				if (c >= '0' && c <= '9')
					continue;
				return false;
			}
			return true;
		}

		DS parseToken(nodeInfo* n, const std::string& key, jsonValue* value)
		{
			if (isLowCaseString(key.c_str()))
			{
				parseNodeFuncTree::iterator piter = m_parseNodeFuncs.find(key);
				if (piter == m_parseNodeFuncs.end())
				{
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:" << key, ERROR);
				}
				dsReturnIfFailedWithOp(piter->second(value, n), delete n);
			}
			else if (isKeyWord(key.c_str()) || (key.size() == 1 && KEY_CHAR[(uint8_t)key.c_str()[0]]))
			{
				if (n->nodeToken != nullptr)
				{
					delete n;
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}
				if (value != nullptr)
				{
					if (value->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->funcName = static_cast<const jsonString*>(value)->m_value;
				}
				n->nodeToken = new token();
				n->nodeToken->type = tokenType::keyword;
				n->nodeToken->value.assign(key.c_str());
			}
			else
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "key must be a lowercase stringbranch key word, but actually is:" << key, ERROR);
			dsOk();
		}

		DS mergeSingleChild(const jsonObject* grammarMap, nodeInfo* n)
		{
			jsonValue* v = n->childJson->m_values.front();
			if (v->t == JSON_TYPE::J_STRING)
			{
				dsReturnIfFailed(parseToken(n, static_cast<jsonString*>(v)->m_value, nullptr));
			}
			else if (v->t == JSON_TYPE::J_OBJECT)
			{
				nodeInfo* child = nullptr;
				bool isInclude = static_cast<const jsonObject*>(v)->get("include") != nullptr;
				if (isInclude)
					dsReturnIfFailed(parseInclude(grammarMap, static_cast<const jsonObject*>(v), child));
				else
					dsReturnIfFailed(parseNode(grammarMap, nullptr, static_cast<const jsonObject*>(v), child));
				if (isInclude || (n->loop && child->loop && (n->loopSeparator != nullptr && child->loopSeparator != nullptr)))
				{
					n->child = new nodeInfo * [1];
					n->child[0] = child;
					n->childCount = 1;
					//both parent and child have loopSeparator ,can not merge
				}
				else
				{
					if (n->loop || child->loop)
						n->loop = true;
					n->nodeToken = child->nodeToken;
					n->funcName = child->funcName;
					if (n->optional || child->optional)
						n->optional = true;
					n->branch = child->branch;
					n->child = child->child;
					n->childCount = child->childCount;
					child->nodeToken = nullptr;
					child->child = nullptr;
					delete child;
				}
			}
			dsOk();
		}

		DS parseInclude(const jsonObject* grammarMap, const jsonObject* grammar, nodeInfo*& result)
		{
			const jsonValue* include = grammar->get("include");
			if (include == nullptr)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed, expect include", ERROR);
			if (include->t != JSON_TYPE::J_STRING)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed, expect include value is string type", ERROR);

			nodeInfo* includeNode = nullptr;
			if (m_allNodes.find(static_cast<const jsonString*>(include)->m_value) != m_allNodes.end())
			{
				includeNode = m_allNodes.find(static_cast<const jsonString*>(include)->m_value)->second;
				includeNode->ref++;
			}
			else
			{
				const jsonValue* includeJson;
				if ((includeJson = grammarMap->get(static_cast<const jsonString*>(include)->m_value)) == nullptr)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for inlcude node:" << static_cast<const jsonString*>(include)->m_value << " not exist", ERROR);
				if (includeJson->t != JSON_TYPE::J_OBJECT)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				includeNode = new nodeInfo();
				m_allNodes.insert(std::pair<std::string, nodeInfo*>(static_cast<const jsonString*>(include)->m_value, includeNode));
				if (!dsCheck(parseNode(grammarMap, static_cast<const jsonString*>(include)->m_value.c_str(), static_cast<const jsonObject*>(includeJson), includeNode)))
				{
					m_allNodes.erase(static_cast<const jsonString*>(include)->m_value);
					delete includeNode;
					dsFailedAndReturn();
				}
			}

			if (grammar->m_values.size() == 1)
			{
				result = includeNode;
			}
			else
			{
				nodeInfo* self = new nodeInfo();
				for (jsonObjectMap::const_iterator iter = grammar->m_values.begin(); iter != grammar->m_values.end(); iter++)
				{
					if (iter->first.compare("include") == 0)
						continue;
					dsReturnIfFailedWithOp(parseToken(self, iter->first, iter->second), delete self);
				}
				if (self->loop == false && self->optional == false)
				{
					delete self;
					result = includeNode;
				}
				else
				{
					self->child = new nodeInfo * [1];
					self->childCount = 1;
					self->child[0] = includeNode;
					result = self;
				}
			}
			/*
			if (self->optional)
				includeNode->optional = true;
			if (self->loop)
			{
				if (includeNode->loop)
				{
					delete self;
					delete includeNode;
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}
				includeNode->loop = true;
				includeNode->loopSeparatorIsOptional = self->loopSeparatorIsOptional;
				includeNode->loopSeparator = self->loopSeparator;
			}
			delete self;
			result = includeNode;
			*/
			dsOk();
		}
		DS checkIfNodeIsHead(const char* nodeName, const jsonObject* grammar, nodeInfo* n)
		{
			const jsonValue* isHead = grammar->get("is head");
			if (isHead != nullptr)
			{
				if (isHead->t != JSON_TYPE::J_BOOL)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				if(nodeName == nullptr)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				if (static_cast<const jsonBool*>(isHead)->m_value)
					m_headNodes.insert(std::pair<std::string, nodeInfo*>(nodeName, n));
			}
			dsOk();
		}


		DS parseNode(const jsonObject* grammarMap, const char* nodeName, const jsonObject* grammar, nodeInfo*& result)
		{
			nodeInfo* n = result == nullptr ? new nodeInfo() : result;
			n->name = nodeName;
			for (jsonObjectValueList::const_iterator iter = grammar->m_valueList.begin(); iter != grammar->m_valueList.end(); iter++)
			{
				if (isLowCaseString(iter->first.c_str()))
				{
					parseNodeFuncTree::iterator fiter = m_parseNodeFuncs.find(iter->first);
					if (fiter == m_parseNodeFuncs.end())
					{
						delete n;
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:" << iter->first, ERROR);
					}
					dsReturnIfFailedWithOp(fiter->second(iter->second, n), delete n);
				}
				else
				{
					if (n->nodeToken == nullptr)
						dsReturnIfFailedWithOp(parseToken(n, iter->first, iter->second), delete n);
				}
			}
			// must have one of nodeToken, child, include, and at most have one
			if ((n->nodeToken != nullptr ? 1 : 0) + (n->childJson != nullptr ? 1 : 0) + (n->include.empty() ? 0 : 1) != 1)
			{
				delete n;
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
			}
			else if (n->childJson != nullptr)
			{
				if (!n->funcName.empty())
				{
					delete n;
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}
				if (n->childJson->m_values.size() == 1)
				{
					if (!dsCheck(mergeSingleChild(grammarMap, n)))
					{
						delete n;
						dsFailedAndReturn();
					}
					dsReturnIfFailedWithOp(checkIfNodeIsHead(nodeName, grammar, n), delete n);
					result = n;
					dsOk();
				}
				n->childCount = n->childJson->m_values.size();
				n->child = new nodeInfo * [n->childCount];
				memset(n->child, 0, sizeof(nodeInfo*) * n->childCount);
				int count = 0;
				for (std::list<jsonValue*>::const_iterator iter = n->childJson->m_values.begin(); iter != n->childJson->m_values.end(); iter++)
				{
					const jsonValue* v = *iter;
					if (v->t == JSON_TYPE::J_STRING)
					{
						const std::string& str = static_cast<const jsonString*>(v)->m_value;
						nodeInfo* c = new nodeInfo();
						if (isLowCaseString(str.c_str()))
						{
							parseNodeFuncTree::iterator fiter = m_parseNodeFuncs.find(str);
							if (fiter == m_parseNodeFuncs.end())
							{
								delete n;
								delete c;
								dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:" << str, ERROR);
							}
							if (!dsCheck(fiter->second(nullptr, c)))
							{
								delete n;
								delete c;
								dsFailedAndReturn();
							}
						}
						else
						{
							if (!dsCheck(parseToken(c, str, nullptr)))
							{
								delete n;
								delete c;
								dsFailedAndReturn();
							}
						}
						n->child[count] = c;
					}
					else if (v->t == JSON_TYPE::J_OBJECT)
					{
						nodeInfo* c = nullptr;
						if (static_cast<const jsonObject*>(v)->get("include") != nullptr)
						{
							if (!dsCheck(parseInclude(grammarMap, static_cast<const jsonObject*>(v), c)))
							{
								delete n;
								dsFailedAndReturn();
							}
						}
						else
						{
							if (!dsCheck(parseNode(grammarMap, nullptr, static_cast<const jsonObject*>(v), c)))
							{
								delete n;
								dsFailedAndReturn();
							}
						}
						n->child[count] = c;
					}
					else
					{
						delete n;
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					}
					count++;
				}
			}
			else if (!n->include.empty())
			{
				nodeInfo* c = nullptr;
				if (!dsCheck(parseInclude(grammarMap, grammar, c)))
				{
					delete n;
					dsFailedAndReturn();
				}
				n->child = new nodeInfo * [1];
				n->childCount = 1;
				n->child[0] = c;
			}
			dsReturnIfFailedWithOp(checkIfNodeIsHead(nodeName, grammar, n), delete n);
			result = n;
			dsOk();
		}

		DS tokenConflictCheck(token* src, token* dest)
		{
			if (src->type == dest->type)
			{
				switch (src->type)
				{
				case tokenType::identifier:
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				case tokenType::keyword:
				case tokenType::specialCharacter:
				case tokenType::symbol:
					if (src->value.compare(dest->value) == 0)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					break;
				case tokenType::literal:
				{
					if (static_cast<literal*>(src)->lType == static_cast<literal*>(dest)->lType)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					else if (static_cast<literal*>(src)->lType == literalType::UNKNOWN || static_cast<literal*>(dest)->lType == literalType::UNKNOWN)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					break;
				}
				default:
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}
			}
			dsOk();
		}

		DS nodeAndTokenConflictCheck(token* src, nodeInfo* dest)
		{
			m_currentNodeScanVersion++;
			dsReturn(_nodeAndTokenConflictCheck(src, dest));
		}

		DS _nodeAndTokenConflictCheck(token* src, nodeInfo* dest)
		{
			if (dest->nodeScanVersion >= m_currentNodeScanVersion)
				dsFailedAndLogIt(1, "not support node chain direct inlcude it self", ERROR);
			dest->nodeScanVersion = m_currentNodeScanVersion;
			for (int i = 0; i < dest->childCount; i++)
			{
				nodeInfo* child = dest->child[i];
				if (child->child != nullptr)
				{
					dsReturnIfFailed(_nodeAndTokenConflictCheck(src, child));
				}
				else
				{
					dsReturnIfFailed(tokenConflictCheck(src, child->nodeToken));
				}
				if (!dest->branch && !child->optional)
					break;
			}
			dsOk();
		}


		DS nodeAndNodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			m_currentNodeScanVersion++;
			dsReturn(_nodeAndNodeConflictCheck(src, dest));
		}

		DS _nodeAndNodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			if (dest->nodeScanVersion >= m_currentNodeScanVersion)
				dsFailedAndLogIt(1, "not support node chain direct inlcude it self", ERROR);
			if (src->branch && dest->branch)
			{
				for (int i = 0; i < src->childCount; i++)
				{
					for (int j = 0; j < dest->childCount; j++)
					{
						dsReturnIfFailed(nodeConflictCheck(src->child[i], dest->child[j]));
					}
				}
			}
			else if (src->branch && !dest->branch)
			{
				for (int i = 0; i < src->childCount; i++)
				{
					for (int j = 0; j < dest->childCount; j++)
					{
						dsReturnIfFailed(nodeConflictCheck(src->child[i], dest->child[j]));
						if (!dest->child[j]->optional)
							break;
					}
				}
			}
			else if (!src->branch && dest->branch)
			{
				for (int i = 0; i < dest->childCount; i++)
				{
					for (int j = 0; j < src->childCount; j++)
					{
						dsReturnIfFailed(nodeConflictCheck(dest->child[i], src->child[j]));
						if (!src->child[j]->optional)
							break;
					}
				}
			}
			else
			{
				for (int i = 0; i < src->childCount; i++)
				{
					for (int j = 0; j < dest->childCount; j++)
					{
						dsReturnIfFailed(nodeConflictCheck(src->child[i], dest->child[j]));
						if (!dest->child[j]->optional)
							break;
					}
					if (!src->child[i]->optional)
						break;
				}
			}
			dsOk();
		}

		DS nodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			m_currentNodeScanVersion++;
			dsReturn(_nodeConflictCheck(src, dest));
		}

		DS _nodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			if (src->nodeToken != nullptr && dest->nodeToken != nullptr)
			{
				dsReturnIfFailed(tokenConflictCheck(src->nodeToken, dest->nodeToken));
			}
			else if (src->nodeToken != nullptr && dest->child != nullptr)
			{
				dsReturnIfFailed(_nodeAndTokenConflictCheck(src->nodeToken, dest));
			}
			else if (src->child != nullptr && dest->nodeToken != nullptr)
			{
				dsReturnIfFailed(_nodeAndTokenConflictCheck(dest->nodeToken, src));
			}
			else
			{
				dsReturnIfFailed(_nodeAndNodeConflictCheck(src, dest));
			}
			dsOk();
		}

		DS checkAndTryMergeTokenAndNotOptNode(nodeInfo*& first, nodeInfo* second)
		{
			token* t = first->nodeToken;
			if (second->ref > 1)
				dsReturnIfFailed((nodeAndTokenConflictCheck(t, second)));
			for (int i = 0; i < second->childCount; i++)
			{
				nodeInfo* c = second->child[i];
				if (c->optional)
				{
					dsReturnIfFailed((nodeAndTokenConflictCheck(t, c)));
					continue;
				}
				else
				{
					if (c->nodeToken != nullptr)
					{
						if (c->nodeToken->compare(*t))
						{
							if (first->loop)
							{
								if (c->loop)
								{
									if (!nodeInfo::compareLoop(*first, *c))
										dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
								}
							}
							if (i == second->childCount - 2)
								second->child[i + 1]->optional = true;
							else if (i < second->childCount - 2)
							{
								nodeInfo** nodes = new nodeInfo * [i + 2];
								memcpy(nodes, second->child, sizeof(nodeInfo*) * (i + 1));
								nodeInfo* newNode = new nodeInfo();
								newNode->optional = true;
								newNode->child = new nodeInfo * [second->childCount - i - 1];
								memcpy(newNode->child, &second->child[i + 1], sizeof(nodeInfo*) * (second->childCount - i - 1));
								newNode->childCount = second->childCount - i - 1;
								nodes[i + 1] = newNode;
								delete[]second->child;
								second->child = nodes;
								second->childCount = i + 2;
							}
							delete first;
							first = nullptr;
							dsOk();
						}
					}
					else
						dsReturnIfFailed((nodeAndTokenConflictCheck(t, c)));
					break;
				}
			}
			dsOk();
		}

		DS checkAndTryMergeTokenAndNode(nodeInfo*& first, nodeInfo* second)
		{
			token* t = first->nodeToken;
			if (second->ref > 1)
				dsReturnIfFailed((nodeAndTokenConflictCheck(t, second)));
			if (second->branch)
			{
				for (int i = 0; i < second->childCount; i++)
				{
					nodeInfo* c = second->child[i];
					if (c->nodeToken != nullptr)
					{
						if (t->compare(*c->nodeToken))
						{
							if (first->funcName.compare(c->funcName) != 0)
								dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);

							if (first->loop)
							{
								if (c->loop)
								{
									if (nodeInfo::compareLoop(*first, *c))
										dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
								}
								else if (second->loop)
								{
									if (nodeInfo::compareLoop(*first, *second))
										dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
								}
								else
									dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
							}
							if (first->optional)
								c->optional = true;
							first = nullptr;
							dsOk();
						}
						else
						{
							dsReturnIfFailed(tokenConflictCheck(t, c->nodeToken));
						}
					}
					else
					{
						if (c->branch)
						{
							if (!c->loop)
								dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
							if (!dsCheck(nodeAndTokenConflictCheck(t, c)))
								dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
							continue;
						}
						else
						{
							dsReturnIfFailed(checkAndTryMergeTokenAndNotOptNode(first, c));
							if (first == nullptr)
								dsOk();
						}
					}
				}
			}
			else
			{
				dsReturn(checkAndTryMergeTokenAndNotOptNode(first, second));
			}
			dsOk();
		}

		void mergeNodesWhenFirstContainSecond(nodeInfo*& first, nodeInfo*& second)
		{
			if (second->childCount + 1 == first->childCount)
			{
				delete second;
				second = nullptr;
				first->child[first->childCount - 1]->optional = true;
			}
			else
			{
				nodeInfo** nodes = new nodeInfo * [first->childCount + 1];
				memcpy(nodes, second->child, sizeof(nodeInfo*) * first->childCount);
				nodes[first->childCount] = new nodeInfo();
				nodes[first->childCount]->child = new nodeInfo * [second->childCount - first->childCount];
				memcpy(nodes[first->childCount]->child, &second->child[first->childCount], sizeof(nodeInfo*) * (second->childCount - first->childCount));
				nodes[first->childCount]->childCount = second->childCount - first->childCount;
				nodes[first->childCount]->optional = true;
				second->childCount = first->childCount + 1;
				delete first;
				delete[] second->child;
				second->child = nodes;
			}
		}

		DS checkAndTryMerge(nodeInfo*& first, nodeInfo*& second)
		{
			if (first == nullptr || second == nullptr)
				dsOk();
			if (first->ref > 1 || second->ref > 1)
				dsReturn(nodeConflictCheck(first, second));
			if (first->compare(*second))
			{
				second = nullptr;
				dsOk();
			}
			if (first->loop || second->loop)//can not merge loop node
				dsReturn(nodeConflictCheck(first, second));

			if (first->optional != second->optional)
				dsReturn(nodeConflictCheck(first, second));
			else if (first->nodeToken != nullptr && second->nodeToken != nullptr)
			{
				if (first->nodeToken->compare(*second->nodeToken))
				{
					if (first->funcName.compare(second->funcName) == 0)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					second = nullptr;
				}
				dsOk();
			}
			else if (first->nodeToken != nullptr || second->nodeToken != nullptr)
			{
				if (first->nodeToken != nullptr)
					dsReturn(checkAndTryMergeTokenAndNode(first, second));
				else
					dsReturn(checkAndTryMergeTokenAndNode(second, first));
			}
			else //both hava child
			{
				//or is true ,means loop is true
				if (first->branch && second->branch)
				{
					if (nodeInfo::compareLoop(*first, *second))
					{
						nodeInfo** nodes = new nodeInfo * [first->childCount + second->childCount];
						for (int i = 0; i < first->childCount; i++)
							nodes[i] = first->child[i];
						for (int i = 0; i < second->childCount; i++)
							nodes[i + second->childCount] = second->child[i];
						delete[]second->child;
						delete[]first->child;
						first->child = nodes;
						first->childCount = first->childCount + second->childCount;
						second->child = nullptr;
						second->childCount = 0;
						delete second;
						second = nullptr;
						dsReturn(optimizeChild(first));
					}
					else
						dsReturn(nodeConflictCheck(first, second));
				}
				else if (first->branch && !second->branch)
				{
					if (nodeInfo::compareLoop(*first, *second))
					{
						for (int i = 0; i < first->childCount; i++)
						{
							if (first->child[i]->compare(*second))
							{
								delete second;
								second = nullptr;
								dsOk();
							}
							else
								dsReturn(nodeConflictCheck(first->child[i], second));
						}
					}
					else
						dsReturn(nodeConflictCheck(first, second));
				}
				else if (!first->branch && second->branch)
				{
					if (nodeInfo::compareLoop(*first, *second))
					{
						for (int i = 0; i < second->childCount; i++)
						{
							if (second->child[i]->compare(*first))
							{
								delete first;
								first = nullptr;
								dsOk();
							}
							else
								dsReturn(nodeConflictCheck(second->child[i], first));
						}
					}
					else
						dsReturn(nodeConflictCheck(first, second));
				}
				else
				{
					int idx = 0;
					for (; idx < std::min<int>(first->childCount, second->childCount); idx++)
					{
						if (first->child[idx]->compare(*second->child[idx]))
							continue;
						else
							break;
					}
					if (idx == 0)
						dsOk();
					if (idx == std::min<int>(first->childCount, second->childCount))
					{
						if (first->childCount == second->childCount)
						{
							delete second;
							second = nullptr;
						}
						else if (first->childCount < second->childCount)
							mergeNodesWhenFirstContainSecond(first, second);
						else
							mergeNodesWhenFirstContainSecond(second, first);
					}
					else
					{
						nodeInfo* p = new nodeInfo;
						p->child = new nodeInfo * [idx + 1];
						p->childCount = idx + 1;
						p->branch = false;
						p->optional = first->optional;
						memcpy(p->child, first->child, idx * sizeof(nodeInfo*));
						p->child[idx] = new nodeInfo();
						p->child[idx]->child = new nodeInfo * [2];
						p->child[idx]->child[0] = first;
						p->child[idx]->child[1] = second;
						p->child[idx]->childCount = 2;
						p->child[idx]->branch = true;
						for (int i = 0; i < idx; i++)
						{
							delete second->child[i];
							second->child[i] = nullptr;
						}
						if (first->childCount == idx + 1)
						{
							p->child[idx]->child[0] = first->child[idx];
							delete[]first->child;
							first->child = nullptr;
							delete first;
							first = nullptr;
						}
						else
						{
							nodeInfo** fc = new nodeInfo * [first->childCount - 1];
							memcpy(fc, &first->child[idx], sizeof(nodeInfo*) * (first->childCount - idx));
							delete[] first->child;
							first->child = fc;
							first->childCount = first->childCount - idx;
						}
						if (second->childCount == idx + 1)
						{
							p->child[idx]->child[1] = second->child[idx];
							delete[]second->child;
							second->child = nullptr;
							delete second;
							second = nullptr;
						}
						else
						{
							nodeInfo** fc = new nodeInfo * [second->childCount - 1];
							memcpy(fc, &second->child[idx], sizeof(nodeInfo*) * (second->childCount - idx));
							delete[] second->child;
							second->child = fc;
							second->childCount = second->childCount - idx;
						}
						second = nullptr;
						first = p;
						dsReturn(optimizeChild(p));
					}
				}
			}
			dsOk();
		}
		DS optimizeChild(nodeInfo* node)
		{
			if (node->child == nullptr || node->inOptimize)
				dsOk();
			node->inOptimize = true;
			for (int i = 0; i < node->childCount; i++)
			{
				if (node->child[i]->child != nullptr)
					dsReturnIfFailed(optimizeChild(node->child[i]));
			}
			if (node->branch)
			{
				for (int i = 0; i < node->childCount; i++)
				{
					nodeInfo* c = node->child[i];
					if (c->branch)
					{
						if (c->loop || c->ref > 1)
							continue;
						if (c->optional)
							dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "child node in [or] node can not be optional", ERROR);
						int mergedCount = 0;
						for (int m = 0; m < c->childCount; m++)
						{
							for (int j = 0; j < node->childCount; j++)
							{
								if (i == j)
									continue;
								dsReturnIfFailed(checkAndTryMerge(c->child[m], node->child[j]));
								if (c->child[m] == nullptr)
								{
									mergedCount++;
									break;
								}
								else if (node->child[j] == nullptr)
								{
									for (int n = j; n < node->childCount - 1; n++)
										node->child[n] = node->child[n + 1];
									node->childCount--;
									break;
								}
								dsReturnIfFailed(nodeConflictCheck(c->child[m], node->child[j]));
							}
						}
						nodeInfo** tmp = new nodeInfo * [node->childCount - 1 + c->childCount - mergedCount];
						for (int m = 0; m < i; m++)
							tmp[m] = node->child[m];
						for (int m = i; m < node->childCount - 1; m++)
							tmp[m] = node->child[m + 1];
						int idx = node->childCount - 1;
						for (int m = 0; m < c->childCount; m++)
						{
							if (c->child[m] != nullptr)
								tmp[idx++] = c->child[m];
						}
						delete[]node->child;
						node->child = tmp;
						node->childCount = node->childCount - 1 + c->childCount - mergedCount;
						memset(c->child, 0, sizeof(nodeInfo*) * c->childCount);
						delete c;
						i--;
					}
				}
				for (int i = 0; i < node->childCount; i++)
				{
					for (int j = i + 1; j < node->childCount; j++)
					{
						dsReturnIfFailed(checkAndTryMerge(node->child[i], node->child[j]));
					}
				}
				int newCount = 0;
				for (int i = 0; i < node->childCount; i++)
				{
					if (node->child[i] != nullptr)
						newCount++;
				}

				if (newCount == 1)
				{
					nodeInfo* c = nullptr;
					for (int i = 0; i < node->childCount; i++)
					{
						if (node->child[i] != nullptr)
						{
							c = node->child[i];
							break;
						}
					}
					if ((node->loop && c->loop) || c->ref > 1)
					{
						nodeInfo** nodes = new nodeInfo * [1];
						nodes[0] = c;
						delete[]node->child;
						node->child = nodes;
						node->childCount = 1;
					}
					else
					{
						if (c->loop)
						{
							node->loop = c->loop;
							node->loopSeparator = c->loopSeparator;
						}
						if (c->optional)
							node->optional = true;
						node->nodeToken = c->nodeToken;
						c->nodeToken = nullptr;
						node->child = c->child;
						c->child = nullptr;
						node->childCount = c->childCount;
						c->childCount = 0;
						node->funcName = c->funcName;
						node->branch = c->branch;
						delete c;
					}
					dsOk();
				}
				if (newCount == node->childCount)
					dsOk();
				nodeInfo** nodes = new nodeInfo * [newCount];
				int idx = 0;
				for (int i = 0; i < node->childCount; i++)
				{
					if (node->child[i] != nullptr)
						nodes[idx++] = node->child[i];
				}
				delete[]node->child;
				node->child = nodes;
				node->childCount = newCount;
			}
			dsOk();
		}
	public:
		DS loadFromFile(const char* filePath)
		{
			fileHandle handle = openFile(filePath, true, false, false);
			if (!fileHandleValid(handle))
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "open file :" << filePath << " failed for " << errno << "," << strerror(errno), ERROR);
			int size = getFileSize(handle);
			if (size <= 0)
			{
				closeFile(handle);
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "open file :" << filePath << " failed for file size is " << size, ERROR);
			}
			char* buf = new char[size + 1];
			if (size != readFile(handle, buf, size))
			{
				delete buf;
				closeFile(handle);
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "open file :" << filePath << " failed for file only read part data from file", ERROR);
			}
			buf[size] = '\0';
			closeFile(handle);
			dsReturnWithOp(load(buf), delete[]buf);
		}
		DS load(const char* grammar)
		{
			int size = 0;
			jsonValue* json = nullptr;
			if (!dsCheck(jsonValue::parse(json, grammar, size)))
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for" << getLocalStatus().toString(), ERROR);
			if (json == nullptr || json->t != JSON_TYPE::J_OBJECT)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
			m_grammarTreeInJson = static_cast<jsonObject*>(json);

			for (jsonObjectMap::iterator iter = m_grammarTreeInJson->m_values.begin(); iter != m_grammarTreeInJson->m_values.end(); iter++)
			{
				if (iter->second->t != JSON_TYPE::J_OBJECT)
				{
					delete m_grammarTreeInJson;
					m_grammarTreeInJson = nullptr;
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				}
				if (m_allNodes.find(iter->first) != m_allNodes.end())
					continue;
				nodeInfo* c = nullptr;
				if (!dsCheck(parseNode(m_grammarTreeInJson, iter->first.c_str(), static_cast<const jsonObject*>(iter->second), c)))
				{
					delete m_grammarTreeInJson;
					m_grammarTreeInJson = nullptr;
					dsFailedAndReturn();
				}
				m_allNodes.insert(std::pair<std::string, nodeInfo*>(iter->first, c));
			}
			//delete grammarTree;
			dsOk();
		}
		DS optimize()
		{
			m_root.branch = true;
			m_root.childCount = m_headNodes.size();
			m_root.child = new nodeInfo * [m_root.childCount];
			int idx = 0;
			for (std::map<std::string, nodeInfo*>::iterator iter = m_headNodes.begin(); iter != m_headNodes.end(); iter++)
				m_root.child[idx++] = iter->second;
			dsReturn(optimizeChild(&m_root));
		}
		bool compare(const lex& lr) const
		{
			return m_root.compare(lr.m_root);
		}

		struct nodeIterator {
			std::list<std::pair<nodeInfo*, int>> stack;
		};
		typedef std::list < std::pair<nodeInfo*, int>> nodeStack;

		inline void addSpace(char* space)
		{
			int length = strlen(space);
			space[length] = '\t';
			space[length + 1] = '\0';
		}
		inline void decSpace(char* space)
		{
			int length = strlen(space);
			if (length == 0)
				return;
			space[length - 1] = '\0';
		}

		DS generateliteralCode(String& code, char* space, nodeInfo* node)
		{
			if (node->nodeToken->type != tokenType::literal)
				dsFailedAndLogIt(1, "unexpect tokenType:" << static_cast<int>(node->nodeToken->type), ERROR);
			code.append(space).append("pos = sqlPos;\n");
			code.append(space).append("dsReturnIfFailed(matchToken(handle->stack, t, pos, true, false ));\n");
			dsReturn(generateliteralWithTokenCode(code, space, node));
		}

		DS generateliteralWithTokenCode(String& code, char* space, nodeInfo* node)
		{
			if (node->nodeToken->type != tokenType::literal)
				dsFailedAndLogIt(1, "unexpect tokenType:" << static_cast<int>(node->nodeToken->type), ERROR);
			switch (static_cast<literal*>(node->nodeToken)->lType)
			{
			case literalType::INT_NUMBER:
				code.append(space).append("if(matchLiteralType(literalType::INT_NUMBER, t) == 0)\n");
				break;
			case literalType::FLOAT_NUMBER:
				code.append(space).append("if(matchLiteralType(literalType::FLOAT_NUMBER, t) == 0)\n");
				break;
			case literalType::NUMBER_VALUE:
				code.append(space).append("if(unlikely(static_cast<literal*>(t)->lType == literalType::INT_NUMBER || static_cast<literal*>(t)->lType == literalType::FLOAT_NUMBER))\n");
				break;
			case literalType::BIT_STRING:
				code.append(space).append("if(matchLiteralType(literalType::BIT_STRING, t) == 0)\n");
				break;
			case literalType::HEX_STRING:
				code.append(space).append("if(matchLiteralType(literalType::HEX_STRING, t) == 0)\n");
				break;
			case literalType::BOOLEAN:
				code.append(space).append("if(matchLiteralType(literalType::BOOLEAN, t) == 0)\n");
				break;
			case literalType::DATE:
				code.append(space).append("if(matchLiteralType(literalType::DATE, t) == 0)\n");
				break;
			case literalType::TIME:
				code.append(space).append("if(matchLiteralType(literalType::TIME, t) == 0)\n");
				break;
			case literalType::INTERVAL:
				code.append(space).append("if(matchLiteralType(literalType::INTERVAL, t) == 0)\n");
				break;
			case literalType::CHARACTER_STRING:
			case literalType::NATIONAL_CHARACTER_STRING:
				code.append(space).append("if(unlikely(static_cast<literal*>(t)->lType == literalType::CHARACTER_STRING \n");
				addSpace(space);
				code.append(space).append(" || static_cast<literal*>(t)->lType == literalType::HEX_STRING\n");
				code.append(space).append(" || static_cast<literal*>(t)->lType == literalType::BIT_STRING))\n");
				decSpace(space);
				break;
			case literalType::FUNCTION:
				code.append(space).append("if(matchLiteralType(literalType::FUNCTION, t) == 0)\n");
				break;
			case literalType::EXPRESSION:
				code.append(space).append("if(matchLiteralType(literalType::EXPRESSION, t) == 0)\n");
				break;
			case literalType::ANY_STRING:
				code.append(space).append("if(matchAnyString(t) == 0)\n");
				break;
			case literalType::ALL_VALUE:
				code.append(space).append("if(matchAllLiteralToken(t) == 0)\n");
				break;
			default:
				dsFailedAndLogIt(1, "unsupport literalType:" << static_cast<int>(static_cast<literal*>(node->nodeToken)->lType), ERROR);
			}
			code.append(space).append("{\n");
			addSpace(space);
			code.append(space).append("sqlPos = pos;\n");
			dsOk();
		}

		bool hasAlphabet(const char* str)
		{
			char c;
			while ((c = *(str++)) != '\0')
			{
				if ((c <= 'z' && c >= 'a') || (c <= 'Z' && c >= 'A'))
					return true;
			}
			return false;
		}

		void generateCodeForIdentifer(char* space, String& code, bool pureIdentifier)
		{
			code.append(space).append("pos = sqlPos;\n");
			code.append(space).append("dsReturnIfFailed(s = matchIdentifier(handle->stack, t, pos, ").append(pureIdentifier?"false":"true").append("));\n");
			code.append(space).append("if(s == 0)\n");
			code.append(space).append("{\n");
			addSpace(space);
			code.append(space).append("sqlPos = pos;\n");
		}

		void generateCodeForIdentiferWithToken(char* space, String& code, const char* notMatchCode)
		{
			code.append(space).append("if(t->type == tokenType::identifier)\n");
			code.append(space).append("{\n");
			addSpace(space);
			code.append(space).append("sqlPos = pos;\n");
		}

		void appendTokenTailCode(char* space, const char* funcName, const char* funcTokenArgv, String& code,
			const std::list<std::string>& matchedCodes, const std::list<std::string>& notMatchCodes)
		{
			if (funcName != nullptr)
				addFuncCode(space, funcName, funcTokenArgv == nullptr ? "" : funcTokenArgv, code);
			if (!matchedCodes.empty())
			{
				for (std::list<std::string>::const_iterator iter = matchedCodes.begin(); iter != matchedCodes.end(); iter++)
					code.append(space).append(*iter).append("\n");
			}
			decSpace(space);
			code.append(space).append("}\n");
			if (!notMatchCodes.empty())
			{
				code.append(space).append("else\n");
				code.append(space).append("{\n");
				addSpace(space);
				for (std::list<std::string>::const_iterator iter = notMatchCodes.begin(); iter != notMatchCodes.end(); iter++)
					code.append(space).append(*iter).append("\n");
				decSpace(space);
				code.append(space).append("}\n");
			}
		}

		void generateCodeForMatchStaticWords(String& code, char* space, const char* word)
		{
			if (strlen(word) == 1)
			{
				if (hasAlphabet(word))
				{
					char s[2] = { 0 };
					if ((word[0] <= 'z' && word[0] >= 'a'))
						s[0] = word[0] + ('A' - 'a');
					else
						s[0] = word[0] - ('A' - 'a');
					code.append(space).append("if(*sqlPos == '").append(word).append("' || *sqlPos == '").append(s).append("')\n");
				}
				else
				{
					code.append(space).append("if(*sqlPos == '").append(word).append("')\n");
				}
			}
			else
			{
				if (hasAlphabet(word))
				{
					code.append(space).append("if(strncasecmp(\"").append(word)
						.append("\", sqlPos, ").append(strlen(word)).append(") == 0 && isSeparator(sqlPos + ").append(strlen(word)).append(")) \n");
				}
				else
				{
					code.append(space).append("if(memcmp(\"").append(word)
						.append("\", sqlPos, ").append(strlen(word)).append(") == 0 && isSeparator(sqlPos + ").append(strlen(word)).append(")) \n");
				}
			}
			code.append(space).append("{\n");
			addSpace(space);
			code.append(space).append("sqlPos += ").append(strlen(word)).append(";\n");
		}

		void addFuncCode(char* space, const std::string& funcName, const std::string& keyWord, String& code)
		{
			code.append(space).append("dsReturnIfFailed(").append(funcName).append("(handle, ");
			if (keyWord.empty())
			{
				code.append("t");
			}
			else
			{
				int id = 0;
				std::map<std::string, int>::const_iterator iter = m_staticTokens.find(keyWord);
				if (iter == m_staticTokens.end())
					id = m_staticTokens.insert(std::pair<std::string, int>(keyWord, m_staticTokens.size() + 1)).first->second;
				else
					id = iter->second;
				code.append("m_staticToken_").append(id);
			}
			code.append(", currentSql));\n");
		}

		DS generateCodeForNodeToken(nodeInfo* node, String& code, char* space)
		{
			std::list<std::string> matchedCodes;
			std::list<std::string> notMatchCodes;
			if (node->nodeToken != nullptr)
			{
				if (node->loop && node->optional)
					dsFailedAndLogIt(1, "loop token node can be optional", ERROR);
				if (node->loop)
				{
					if (node->loopSeparator == nullptr)
					{
						notMatchCodes.push_back("dsReturnCode(count == 0);");
					}
					else if (node->loopSeparatorIsOptional)
					{
						//need check has matched loopSeparatorIsOptional
						notMatchCodes.push_back("NOT_MATHCH_RETURN(count == 0)");
					}
					else
					{
						//if count == 0, not match, count >0 means has matched loopSeparator,grammar error
						notMatchCodes.push_back("if (count > 0)");
						notMatchCodes.push_back("\tdsFailed(1, \"grammar error @ \"<< std::string(sqlPos, std::min<size_t>(50, strlen(sqlPos))));");
						notMatchCodes.push_back("else");
						notMatchCodes.push_back("\tdsReturn(1);");
					}
				}
				else
				{
					notMatchCodes.push_back("dsReturnCode(1);");
				}
				code.append(space).append("nextWordPos(sqlPos);\n");
				if (node->nodeToken->type == tokenType::keyword)
				{
					generateCodeForMatchStaticWords(code, space, node->nodeToken->value.pos);
				}
				else if (node->nodeToken->type == tokenType::literal)
				{
					dsReturnIfFailed(generateliteralCode(code, space, node));
				}
				else if (node->nodeToken->type == tokenType::identifier)
				{
					generateCodeForIdentifer(space, code, node->pureIdentifier);
				}
				else
					dsFailedAndLogIt(1, "not support token type" << (int)(node->nodeToken->type), ERROR);

				appendTokenTailCode(space, node->funcName.empty() ? nullptr : node->funcName.c_str()
					, node->nodeToken->type == tokenType::keyword ? node->nodeToken->value.toString().c_str() : nullptr
					, code, matchedCodes, notMatchCodes);
				dsOk();
			}
			else
				dsFailedAndLogIt(1, "node must have nodeToken", ERROR);
		}

		DS createChildFunc(nodeInfo* c, String& code, char* space, int& idx, const std::list<std::string>& matchedCodes, const std::list<std::string>& notMatchCodes)
		{
			if (c->funcId == 0)
			{
				c->funcId = ++idx;
				dsReturnIfFailed(generateCodeForNode(c, idx));
			}
			if (!notMatchCodes.empty() || !matchedCodes.empty())
			{
				code.append(space).append("dsReturnIfFailed(s = parse_").append(c->funcId).append("(handle, sqlPos, currentSql));\n");
				if (!matchedCodes.empty() && notMatchCodes.empty())
					code.append(space).append("if (s == 0)\n");
				else
					code.append(space).append("if (s != 0)\n");
				if (!notMatchCodes.empty())
				{
					code.append(space).append("{\n");
					addSpace(space);
					for (std::list<std::string>::const_iterator iter = notMatchCodes.begin(); iter != notMatchCodes.end(); iter++)
						code.append(space).append(*iter).append("\n");
					decSpace(space);
					code.append(space).append("}\n");
				}
				if (!matchedCodes.empty())
				{
					if (!notMatchCodes.empty())
						code.append(space).append("else\n");
					code.append(space).append("{\n");
					addSpace(space);
					if (!matchedCodes.empty())
					{
						for (std::list<std::string>::const_iterator iter = matchedCodes.begin(); iter != matchedCodes.end(); iter++)
							code.append(space).append(*iter).append("\n");
					}
					decSpace(space);
					code.append(space).append("}\n");
				}
			}
			else
			{
				code.append(space).append("dsReturnIfFailed(parse_").append(c->funcId).append("(handle, sqlPos, currentSql));\n");
			}
			dsOk();
		}

		bool isNeedMatchedPartOfTokensCode(nodeInfo* node)
		{
			if (node->loop && node->loopSeparator != nullptr)
			{
				if (node->nodeToken != nullptr && !node->loopSeparatorIsOptional)
					return false;
				else
					return true;
			}
			else if (node->childCount > 1 && !node->branch)
				return true;
			return false;
		}

		DS generateMatchedCode(nodeInfo* node, int i, std::list<std::string>& matchedCodes, std::list<std::string>& notMatchCodes)
		{
			nodeInfo* c = node->child[i];
			if (node->branch)
			{
				if (c->optional)
					dsFailedAndLogIt(1, "branch token node can not be optional", ERROR);
				if (node->loop)
				{
					if (i != node->childCount)
					{
						if (node->loopSeparator == nullptr)
							matchedCodes.push_back("continue;");
						else
							matchedCodes.push_back("goto MATCH;");
					}
				}
				else
				{
					matchedCodes.push_back("dsOk();");
				}
			}
			else
			{
				bool hasFetchToFirstNotOptional = false;
				for (int n = 0; n < i; n++)
				{
					if (!node->child[n]->optional)
					{
						hasFetchToFirstNotOptional = true;
						break;
					}
				}
				String notMatchTmp = "NOT_MATCH_CHECK_ANNOTATION_RETRY(sqlPos, ";
				notMatchTmp.append(i).append(");");
				notMatchCodes.push_back(notMatchTmp);
				if (!c->optional)
				{
					//check if this is the first not optional node
					if (!hasFetchToFirstNotOptional)
					{
						if (node->loop)
						{
							if (node->loopSeparator != nullptr)
							{
								//node->loopSeparator is not null, so need to check node->loopSeparator has matched
								notMatchCodes.push_back("NOT_MATHCH_RETURN(count == 0)");
							}
							else //node->loopSeparator is null,not need to check node->loopSeparator has matched
							{
								if (i == 0) // if this is the first node, count == 0 return 1,count > 0 ,return ok
									notMatchCodes.push_back("dsReturnCode(count == 0);");
								else // if this is not the first node,need to check if prev optional node has matched
									notMatchCodes.push_back("NOT_MATHCH_RETURN(count == 0)");
							}
						}
						else
						{
							if (i == 0)
							{
								//first node is not optional, directly return 1
								notMatchCodes.push_back("dsReturnCode(1);");
							}
							else
							{
								//not first node, need to check prev optional node has matched
								notMatchCodes.push_back("NOT_MATHCH_RETURN(1)");
							}
						}
					}
					else
					{
						if (node->loop)
							notMatchCodes.push_back("NOT_MATHCH_RETURN(count == 0)");
						else
							notMatchCodes.push_back("dsReturnCode(1);");
					}
				}
				if (isNeedMatchedPartOfTokensCode(node))
				{
					if (i == node->childCount - 1)
					{
						//if all child node is optional, set matchedPartOfTokens, and in end of loop check if has matched any node
						//otherwise ,we do not need to set matchedPartOfTokens when this is the last child node
						if (node->loop)
							matchedCodes.push_back("matchedPartOfTokens = false;");
					}
					else
					{
						if (!hasFetchToFirstNotOptional)
							matchedCodes.push_back("matchedPartOfTokens = true;");
					}
				}
			}
			dsOk();
		}

		DS generateCodeForBranchNode(String & code,char * space,nodeInfo* node, int& idx)
		{
			int keyWordCount = 0;
			int literalCount = 0;
			int identiferCount = 0;
			for (int i = 0; i < node->childCount; i++)
			{
				nodeInfo* c = node->child[i];
				if (c->nodeToken != nullptr && !c->loop)
				{
					if (c->nodeToken->type == tokenType::keyword)
						keyWordCount++;
					else if (c->nodeToken->type == tokenType::literal)
						literalCount++;
					else if (c->nodeToken->type == tokenType::identifier)
						identiferCount++;
				}
			}
			if (identiferCount > 1)
				dsFailedAndLogIt(1, "identifer count must be 0 or 1", ERROR);
			code.append(space).append("nextWordPos(sqlPos);\n");
			code.append(space).append("tryProcessAnnotation(sqlPos);\n");
			if (keyWordCount > 0)
			{
				for (int i = 0; i < node->childCount; i++)
				{
					nodeInfo* c = node->child[i];
					if (c->nodeToken != nullptr&& !c->loop && c->nodeToken->type == tokenType::keyword)
					{
						std::list < std::string >  matchedCodes;
						std::list < std::string >  notMatchCodes;
						dsReturnIfFailed(generateMatchedCode(node, i, matchedCodes, notMatchCodes));
						generateCodeForMatchStaticWords(code, space, c->nodeToken->value.pos);
						appendTokenTailCode(space, c->funcName.empty() ? nullptr : c->funcName.c_str()
							, c->nodeToken->type == tokenType::keyword ? c->nodeToken->value.toString().c_str() : nullptr
							, code, matchedCodes, notMatchCodes);
					}
				}
			}
			if (identiferCount + literalCount > 0 && !(identiferCount>0&& literalCount ==0))
			{
				code.append(space).append("pos = sqlPos;\n");
				code.append(space).append("dsReturnIfFailed(matchToken(handle->stack, t, pos, true, false ));\n");
				for (int i = 0; i < node->childCount; i++)
				{
					nodeInfo* c = node->child[i];
					if (c->nodeToken != nullptr && !c->loop)
					{
						std::list < std::string >  matchedCodes;
						std::list < std::string >  notMatchCodes;
						dsReturnIfFailed(generateMatchedCode(node, i, matchedCodes, notMatchCodes));
						if (c->nodeToken->type == tokenType::literal)
							dsReturnIfFailed(generateliteralWithTokenCode(code, space, c));
						else if (c->nodeToken->type == tokenType::identifier)
						{
							code.append(space).append("if (unlikely(t->type != tokenType::literal))\n");
							code.append(space).append("{\n");
							addSpace(space);
							code.append(space).append("sqlPos = pos;\n");
						}
						appendTokenTailCode(space, c->funcName.empty() ? nullptr : c->funcName.c_str()
							, c->nodeToken->type == tokenType::keyword ? c->nodeToken->value.toString().c_str() : nullptr
							, code, matchedCodes, notMatchCodes);
					}
				}
			}
			else if (identiferCount == 1 && literalCount == 0)
			{
				for (int i = 0; i < node->childCount; i++)
				{
					nodeInfo* c = node->child[i];
					if (c->nodeToken != nullptr && !c->loop && c->nodeToken->type == tokenType::identifier)
					{
						std::list < std::string >  matchedCodes;
						std::list < std::string >  notMatchCodes;
						dsReturnIfFailed(generateMatchedCode(node, i, matchedCodes, notMatchCodes));
						generateCodeForIdentifer(space, code, c->pureIdentifier);
						appendTokenTailCode(space, c->funcName.empty() ? nullptr : c->funcName.c_str()
							, c->nodeToken->type == tokenType::keyword ? c->nodeToken->value.toString().c_str() : nullptr
							, code, matchedCodes, notMatchCodes);
						break;
					}
				}
			}

			for (int i = 0; i < node->childCount; i++)
			{
				nodeInfo* c = node->child[i];
				if (c->nodeToken == nullptr || c->loop)
				{
					std::list < std::string >  matchedCodes;
					std::list < std::string >  notMatchCodes;
					dsReturnIfFailed(generateMatchedCode(node, i, matchedCodes, notMatchCodes));
					dsReturnIfFailed(createChildFunc(c, code, space, idx, matchedCodes, notMatchCodes));
				}
			}
			if (node->loop)
			{
				if (node->loopSeparator != nullptr && !node->loopSeparatorIsOptional)
				{
					//if count == 0 , not match, otherwise we must have matched loopSeparator, so this is grammar error
					code.append(space).append("if(count == 0)");
					addSpace(space);
					code.append(space).append("dsReturnCode(1);\n");
					decSpace(space);
					code.append(space).append("else\n");
					addSpace(space);
					code.append(space).append("dsFailed(1, \"grammar error @ \" << std::string(sqlPos, std::min<size_t>(50, strlen(sqlPos))));\n");
					decSpace(space);
				}
				else
				{
					code.append(space).append("dsReturnCode(count == 0);");
				}
			}
			else
				code.append(space).append("dsReturnCode(1);\n");
			dsOk();
		}

		DS generateCodeForNotBranchNode(String& code, char* space, nodeInfo* node, int& idx)
		{
			for (int i = 0; i < node->childCount; i++)
			{
				nodeInfo* c = node->child[i];
				decSpace(space);
				code.append(space).append("MATCH_").append(i).append(":\n");
				addSpace(space);
				code.append(space).append("nextWordPos(sqlPos);\n");
				std::list < std::string >  matchedCodes;
				std::list < std::string >  notMatchCodes;
				dsReturnIfFailed(generateMatchedCode(node, i, matchedCodes, notMatchCodes));
				if (c->nodeToken == nullptr || c->loop)
				{
					dsReturnIfFailed(createChildFunc(c, code, space, idx, matchedCodes, notMatchCodes));
				}
				else
				{
					if (c->nodeToken->type == tokenType::keyword)
						generateCodeForMatchStaticWords(code, space, c->nodeToken->value.pos);
					else if (c->nodeToken->type == tokenType::literal)
						dsReturnIfFailed(generateliteralCode(code, space, c));
					else if (c->nodeToken->type == tokenType::identifier)
						generateCodeForIdentifer(space, code, c->pureIdentifier);
					else
						dsFailedAndLogIt(1, "not support token type" << (int)(c->nodeToken->type), ERROR);
					appendTokenTailCode(space, c->funcName.empty() ? nullptr : c->funcName.c_str()
						, c->nodeToken->type == tokenType::keyword ? c->nodeToken->value.toString().c_str() : nullptr
						, code, matchedCodes, notMatchCodes);
				}
			}
			dsOk();
		}

		DS generateCodeForNode(nodeInfo* node, int& idx)
		{
			String code;
			int id = idx;
			char space[256] = { '\t','\t',0 };
			if(node->name != nullptr)
				code.append(space).append("//").append(node->name).append("\n");
			code.append(space).append("inline DS parse_").append(idx).append("(sqlHandle * handle, char *& sqlPos, sql*& currentSql)\n");
			code.append(space).append("{\n");
			addSpace(space);

			bool needMatchedPartOfTokensCode = isNeedMatchedPartOfTokensCode(node);
			if (needMatchedPartOfTokensCode)
				code.append(space).append("bool matchedPartOfTokens = false;\n");

			if (node->loop)
			{
				code.append(space).append("for(uint32_t count = 0; ; count++)\n");
				code.append(space).append("{\n");
				addSpace(space);
			}

			if (node->nodeToken == nullptr || node->nodeToken->type != tokenType::keyword)
			{
				code.append(space).append("DS s;\n");
				if (node->nodeToken != nullptr && (node->nodeToken->type == tokenType::literal || node->nodeToken->type == tokenType::identifier))
					code.append(space).append("char * pos;\n");
				else if (node->child != nullptr)
				{
					for (int i = 0; i < node->childCount; i++)
					{
						if (node->child[i]->nodeToken != nullptr
							&& (node->child[i]->nodeToken->type == tokenType::literal || node->child[i]->nodeToken->type == tokenType::identifier))
						{
							code.append(space).append("char * pos;\n");
							break;
						}
					}
				}
				code.append(space).append("token * t = nullptr;\n");
			}
			if (node->nodeToken != nullptr)
			{
				dsReturnIfFailed(generateCodeForNodeToken(node, code, space));
			}
			else
			{
				if (node->branch)
					dsReturnIfFailed(generateCodeForBranchNode(code, space, node, idx));
				else
					dsReturnIfFailed(generateCodeForNotBranchNode(code, space, node, idx));

				bool hasMatchedLable = node->childCount > 1 && (node->branch && node->loopSeparator != nullptr);
				if (node->branch && hasMatchedLable)
				{
					decSpace(space);
					code.append(space).append("MATCH:\n");
					addSpace(space);
				}
			}
			if (node->loop)
			{
				if (node->loopSeparator != nullptr)
				{
					generateCodeForMatchStaticWords(code, space, node->loopSeparator);
					if (needMatchedPartOfTokensCode)
						code.append(space).append("matchedPartOfTokens = true;\n");
					decSpace(space);
					code.append(space).append("}\n");
					code.append(space).append("else\n");
					code.append(space).append("{\n");
					addSpace(space);
					if (node->loopSeparatorIsOptional)
					{
						code.append(space).append("matchedPartOfTokens = false;\n");
						code.append(space).append("continue;\n");
					}
					else
					{
						code.append(space).append("if(count > 0)\n");
						addSpace(space);
						code.append(space).append("break;\n");
						decSpace(space);
						code.append(space).append("else\n");
						addSpace(space);
						code.append(space).append("dsReturnCode(1);\n");
						decSpace(space);
					}
					code.append(space).append("}\n");
				}
				else
				{
					if (needMatchedPartOfTokensCode)
						code.append(space).append("matchedPartOfTokens = false;\n");
				}
				decSpace(space);
				code.append(space).append("}\n");
			}
			if(!(node->branch && !node->loop))
				code.append(space).append("dsOk();\n");
			decSpace(space);
			code.append(space).append("}\n");
			m_codes.insert(std::pair<int, String>(id, code));
			dsOk();
		}
		void generateStaticTokenCode(String& code)
		{
			char space[20] = { '\t',0 };
			code.append("\tprivate:\n");
			for (size_t i = 1; i < m_staticTokens.size(); i++)
				code.append("\t\ttoken * m_staticToken_").append(i).append(";\n");
			code.append("\n");
			code.append(space).append("void initStaticTokens()\n");
			code.append(space).append("{\n");
			addSpace(space);
			for (int i = 1; i < (int)m_staticTokens.size(); i++)
			{
				for (std::map<std::string, int>::iterator iter = m_staticTokens.begin(); iter != m_staticTokens.end(); iter++)
				{
					if (iter->second != i)
						continue;
					code.append(space).append("m_staticToken_").append(iter->second).append(" = new token();\n");
					code.append(space).append("m_staticToken_").append(iter->second).append("->type = tokenType::keyword;\n");
					code.append(space).append("m_staticToken_").append(iter->second).append("->value.assign(\"").append(iter->first).append("\", ").append(iter->first.size()).append(");\n");
					break;
				}
			}
			decSpace(space);
			code.append(space).append("}\n");
		}

		DS generateCode(const char* parseFuncHeadFile, const char* className, const char* sourceFile)
		{
			int idx = 0;
			const char* includeCode =
				"#include <string.h>\n"
				"#include <string>\n"
				"#include <stdlib.h>\n"
				"#include <stdint.h>\n"
				"#include <glog/logging.h>\n"
				"#include \"util/status.h\"\n"
				"#include \"sqlParser.h\"\n";
			dsReturnIfFailed(generateCodeForNode(&m_root, idx));
			if (checkFileExist(sourceFile, F_OK) == 0)
			{
				LOG(WARNING) << "out put source file:" << sourceFile << " exist";
				if (0 != remove(sourceFile))
					dsFailedAndLogIt(1, "remove source file:" << sourceFile << " failed for " << errno << " " << strerror(errno), ERROR);
				if (checkFileExist(sourceFile, F_OK) == 0)
					dsFailedAndLogIt(1, "remove source file:" << sourceFile << " failed for " << errno << " " << strerror(errno), ERROR);
			}
			fileHandle handle = openFile(sourceFile, true, true, true);
			if (!fileHandleValid(handle))
				dsFailedAndLogIt(1, "open out put source file:" << sourceFile << " failed for " << errno << " " << strerror(errno), ERROR);
			int writeSize = 0;
			String code;
			if ((writeSize = writeFile(handle, includeCode, strlen(includeCode))) != (int)(strlen(includeCode)))
				goto FAILED;
			if (parseFuncHeadFile != nullptr)
			{
				code.clear();
				code.append("#include \"").append(parseFuncHeadFile).append("\"\n");
				if ((writeSize = writeFile(handle, code.c_str(), code.size())) != (int)(code.size()))
					goto FAILED;
			}
			code.assign("namespace SQL_PARSER{\n"
				"	class ").append(className).append(": public sqlParser{ \n");
			if ((writeSize = writeFile(handle, code.c_str(), code.size())) != (int)code.size())
				goto FAILED;
			if (!m_staticTokens.empty())
			{
				code.clear();
				generateStaticTokenCode(code);
				if ((writeSize = writeFile(handle, code.c_str(), code.size())) != (int)code.size())
					goto FAILED;
			}
			code.clear();
			code.append("public:\n");
			code.append("\t\t").append(className).append("()\n");
			code.append("\t\t").append("{\n");
			if (!m_staticTokens.empty())
				code.append("\t\t\t").append("initStaticTokens();\n");
			code.append("\t\t").append("}\n");
			code.append("\t\t~").append(className).append("()\n");
			code.append("\t\t").append("{\n");
			if (!m_staticTokens.empty())
			{
				for (int i = 1; i < (int)m_staticTokens.size(); i++)
					code.append("\t\t\t").append("delete m_staticToken_").append(i).append(";\n");
			}
			code.append("\t\t").append("}\n");
			code.append("private:\n");
			code.append("\t\tvirtual DS parseOneSentence(sqlHandle * handle, char *&sqlPos, sql*& s)\n"
				"		{\n"
				"			char * pos = sqlPos;\n"
				"			dsReturnIfFailed(parse_0(handle, pos, s));\n"
				"			sqlPos = pos;\n"
				"			dsOk();\n"
				"		}\n");
			if ((writeSize = writeFile(handle, code.c_str(), code.size())) != (int)code.size())
				goto FAILED;

			for (std::map<int, String>::iterator iter = m_codes.begin(); iter != m_codes.end(); iter++)
			{
				if (1 != writeFile(handle, "\n", 1))
					goto FAILED;
				if ((writeSize = writeFile(handle, iter->second.c_str(), iter->second.size())) != (int)iter->second.size())
					goto FAILED;
			}
			code.assign("\t};\n}");
			if ((writeSize = writeFile(handle, code.c_str(), code.size())) != (int)code.size())
				goto FAILED;
			closeFile(handle);
			dsOk();
		FAILED:
			int err = errno;
			closeFile(handle);
			dsFailedAndLogIt(1, "write code to source file:" << sourceFile << " failed for " << err << " " << strerror(err), ERROR);
		}
	};
}
