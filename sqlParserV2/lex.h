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
			const char* loopSeparator;
			bool optional;
			bool branch ;
			const jsonArray* childJson;
			nodeInfo** child;
			int childCount;
			token* nodeToken;
			std::string funcName;
			std::string include;
			int ref;
			nodeInfo() :loop(false), loopSeparator(nullptr), optional(false),branch (false), childJson(nullptr), child(nullptr), childCount(0), nodeToken(nullptr), ref(1)
			{
			}
			~nodeInfo()
			{
				if (nodeToken != nullptr)
					delete nodeToken;
				if (child != nullptr)
				{
					for (int i = 0; i < childCount; i++)
					{
						if (child[i] != nullptr && --child[i]->ref <= 0)
							delete child[i];
					}
					delete[] child;
				}
			}
			static bool compareLoop(const nodeInfo& first, const nodeInfo& second)
			{
				if (first.loop != second.loop)
					return false;
				if (first.loopSeparator != nullptr && second.loopSeparator != nullptr)
				{
					if (strcmp(first.loopSeparator, second.loopSeparator) != 0)
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
				if (optional != n.optional ||branch!=n.branch ||loop != loop)
					return false;

				if (nodeToken != nullptr && n.nodeToken != nullptr)
				{
					if (!nodeToken->compare(*n.nodeToken))
						return false;
					if (funcName.compare(n.funcName) != 0)
						return false;
					if (loop != n.loop)
						return false;
					if (loopSeparator != nullptr && n.loopSeparator != nullptr)
					{
						if (strcmp(loopSeparator, n.loopSeparator) != 0)
							return false;
					}
					else if (loopSeparator != nullptr || n.loopSeparator != nullptr)
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
		typedef std::pair<string, std::function<dsStatus& (const jsonValue*, nodeInfo*)> > parseNodeFuncPair;
		typedef std::map<string, std::function<dsStatus& (const jsonValue*, nodeInfo*)> > parseNodeFuncTree;
		parseNodeFuncTree m_parseNodeFuncs;
		std::map<std::string, nodeInfo*> m_allNodes;
		std::map<std::string, nodeInfo*> m_headNodes;
		nodeInfo m_root;
		std::list<nodeInfo*> m_containIncludeNodes;
		jsonObject* m_grammarTreeInJson;

	public:
		lex():m_grammarTreeInJson(nullptr)
		{
			initParseNodeFuncs();
		}
	private:
		static inline dsStatus& getliteralType(const jsonValue* v, nodeInfo* n, literalType type)
		{
			if (v != nullptr)
			{
				if (v->t != JSON_TYPE::J_STRING)
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
				n->funcName = static_cast<const jsonString*>(v)->m_value;
			}
			literal* l = new literal();
			l->type = tokenType::literal;
			l->lType = type;
			n->nodeToken = l;
			dsOk();
		}
		void initParseNodeFuncs()
		{
			m_parseNodeFuncs.insert(parseNodeFuncPair("loop", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {
				if (v != nullptr)
				{
					if (v->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					n->loopSeparator = static_cast<const jsonString*>(v)->m_value.c_str();
				}
				n->loop = true;
				dsOk();
				}));
			m_parseNodeFuncs.insert(parseNodeFuncPair("optional", [](const jsonValue* v, nodeInfo* n)->dsStatus& {
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
			m_parseNodeFuncs.insert(parseNodeFuncPair("or", [](const jsonValue* v, nodeInfo* n)->dsStatus& {
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
			m_parseNodeFuncs.insert(parseNodeFuncPair("child", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {
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
			m_parseNodeFuncs.insert(parseNodeFuncPair("is head", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsOk(); }));

			m_parseNodeFuncs.insert(parseNodeFuncPair("string value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::CHARACTER_STRING)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("int value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::INT_NUMBER)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("float value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::FLOAT_NUMBER)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("number value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::NUMBER_VALUE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("expression value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::EXPRESSION)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("bool expression value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {
				dsReturnIfFailed(getliteralType(v, n, literalType::EXPRESSION)); 
				static_cast<expression*>(n->nodeToken)->booleanOrValue = true;
				dsOk(); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("function value", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::FUNCTION)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("date value", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::DATE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("time value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::TIME)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("timestamp value", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::TIMESTAMP)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("bool value", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::BOOLEAN)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("any string", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::ANY_STRING)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("all values", [](const jsonValue* v, nodeInfo* n)->dsStatus& {dsReturn(getliteralType(v, n, literalType::ALL_VALUE)); }));
			m_parseNodeFuncs.insert(parseNodeFuncPair("identifier word", [](const jsonValue* v, nodeInfo* n)->dsStatus& {
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

			//m_parseNodeFuncs.insert(parseNodeFuncPair("value", [](const jsonValue* v, nodeInfo* n) ->dsStatus& {dsReturn(getliteralType(v, n, literalType::UNKNOWN)); }));
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

		dsStatus& parseToken(nodeInfo* n, const std::string& key, jsonValue* value)
		{
			if (isLowCaseString(key.c_str()))
			{
				parseNodeFuncTree::iterator piter = m_parseNodeFuncs.find(key);
				if (piter == m_parseNodeFuncs.end())
				{
					dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:" << key, ERROR);
				}
				if (!dsCheck(piter->second(value, n)))
				{
					delete n;
					dsReturn(getLocalStatus());
				}
			}
			else if (isKeyWord(key.c_str()) || (key.size() == 1 && KEY_CHAR[key.c_str()[0]]))
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

		dsStatus& mergeSingleChild(const jsonObject* grammarMap, nodeInfo* n)
		{
			jsonValue* v = n->childJson->m_values.front();
			if (v->t == JSON_TYPE::J_STRING)
			{
				dsReturnIfFailed(parseToken(n, static_cast<jsonString*>(v)->m_value, nullptr));
			}
			else if (v->t == JSON_TYPE::J_OBJECT)
			{
				nodeInfo* child = nullptr;
				if (static_cast<const jsonObject*>(v)->get("include") != nullptr)
					dsReturnIfFailed(parseInclude(grammarMap, static_cast<const jsonObject*>(v), child));
				else
					dsReturnIfFailed(parseNode(grammarMap, static_cast<const jsonObject*>(v), child));
				if (n->loop && child->loop && (n->loopSeparator != nullptr && child->loopSeparator != nullptr))
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
					n->branch = child->branch ;
					n->child = child->child;
					n->childCount = child->childCount;
					child->nodeToken = nullptr;
					child->child = nullptr;
					delete child;
				}
			}
			dsOk();
		}

		dsStatus& parseInclude(const jsonObject* grammarMap, const jsonObject* grammar, nodeInfo*& result)
		{
			const jsonValue* include = grammar->get("include");
			if (include == nullptr)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
			const jsonValue* includeJson;
			if (include->t != JSON_TYPE::J_STRING)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
			if ((includeJson = grammarMap->get(static_cast<const jsonString*>(include)->m_value)) == nullptr)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for inlcude node:" << static_cast<const jsonString*>(include)->m_value << " not exist", ERROR);
			if (includeJson->t != JSON_TYPE::J_OBJECT)
				dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
			nodeInfo* self = new nodeInfo();
			for (jsonObjectMap::const_iterator iter = grammar->m_values.begin(); iter != grammar->m_values.end(); iter++)
			{
				if (iter->first.compare("include") == 0)
					continue;
				if (!dsCheck(parseToken(self, iter->first, iter->second)))
				{
					delete self;
					dsReturn(getLocalStatus());
				}
			}
			nodeInfo* includeNode = new nodeInfo();
			if (!dsCheck(parseNode(grammarMap, static_cast<const jsonObject*>(includeJson), includeNode)))
			{
				delete self;
				delete includeNode;
				dsReturn(getLocalStatus());
			}
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
				includeNode->loopSeparator = self->loopSeparator;
			}
			delete self;
			result = includeNode;
			dsOk();
		}

		dsStatus& parseNode(const jsonObject* grammarMap, const jsonObject* grammar, nodeInfo*& result)
		{
			nodeInfo* n = new nodeInfo();
			for (jsonObjectMap::const_iterator iter = grammar->m_values.begin(); iter != grammar->m_values.end(); iter++)
			{
				if (isLowCaseString(iter->first.c_str()))
				{
					parseNodeFuncTree::iterator fiter = m_parseNodeFuncs.find(iter->first);
					if (fiter == m_parseNodeFuncs.end())
					{
						delete n;
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:" << iter->first, ERROR);
					}
					if (!dsCheck(fiter->second(iter->second, n)))
					{
						delete n;
						dsReturn(getLocalStatus());
					}
				}
				else
				{
					if (n->nodeToken != nullptr || !dsCheck(parseToken(n, iter->first, iter->second)))
					{
						delete n;
						dsReturn(getLocalStatus());
					}
				}

			}
			// must have one of t, child,include,and at most have one
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
						dsReturn(getLocalStatus());
					}
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
								dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed for can not find parse func for key:"<<str, ERROR);
							}
							if (!dsCheck(fiter->second(static_cast<const jsonString*>(v), c)))
							{
								delete n;
								delete c;
								dsReturn(getLocalStatus());
							}
						}
						else
						{
							if (!dsCheck(parseToken(c, str, nullptr)))
							{
								delete n;
								delete c;
								dsReturn(getLocalStatus());
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
								dsReturn(getLocalStatus());
							}
						}
						else
						{
							if (!dsCheck(parseNode(grammarMap, static_cast<const jsonObject*>(v), c)))
							{
								delete n;
								dsReturn(getLocalStatus());
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
			result = n;
			dsOk();
		}

		dsStatus& tokenConflictCheck(token* src, token* dest)
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
		dsStatus& nodeAndTokenConflictCheck(token* src, nodeInfo* dest)
		{
			for (int i = 0; i < dest->childCount; i++)
			{
				nodeInfo* child = dest->child[i];
				if (child->child != nullptr)
				{
					dsReturnIfFailed(nodeAndTokenConflictCheck(src, child));
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
		dsStatus& nodeAndNodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			if (src->branch && dest->branch )
			{
				for (int i = 0; i < src->childCount; i++)
				{
					for (int j = 0; j < dest->childCount; j++)
					{
						dsReturnIfFailed(nodeConflictCheck(src->child[i], dest->child[j]));
					}
				}
			}
			else if (src->branch && !dest->branch )
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
			else if (!src->branch && dest->branch )
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

		dsStatus& nodeConflictCheck(nodeInfo* src, nodeInfo* dest)
		{
			if (src->nodeToken != nullptr && dest->nodeToken != nullptr)
			{
				dsReturnIfFailed(tokenConflictCheck(src->nodeToken, dest->nodeToken));
			}
			else if (src->nodeToken != nullptr && dest->child != nullptr)
			{
				dsReturnIfFailed(nodeAndTokenConflictCheck(src->nodeToken, dest));
			}
			else if (src->child != nullptr && dest->nodeToken != nullptr)
			{
				dsReturnIfFailed(nodeAndTokenConflictCheck(dest->nodeToken, src));
			}
			else
			{
				dsReturnIfFailed(nodeAndNodeConflictCheck(src, dest));
			}
			dsOk();
		}

		dsStatus& checkAndTryMergeTokenAndNotOptNode(nodeInfo*& first, nodeInfo* second)
		{
			token* t = first->nodeToken;
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
									if (first->loopSeparator != nullptr && c->loopSeparator != nullptr)
									{
										if (strcasecmp(first->loopSeparator, c->loopSeparator) != 0)
										{
											dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
										}
									}
									else if (first->loopSeparator != nullptr || c->loopSeparator != nullptr)
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

		dsStatus& checkAndTryMergeTokenAndNode(nodeInfo*& first, nodeInfo* second)
		{
			token* t = first->nodeToken;
			if (second->branch )
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
									if (first->loopSeparator != nullptr && c->loopSeparator != nullptr)
									{
										if (strcasecmp(first->loopSeparator, c->loopSeparator) != 0)
										{
											dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
										}
									}
									else if (first->loopSeparator != nullptr || c->loopSeparator != nullptr)
										dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
								}
								else if (second->loop)
								{
									if (first->loopSeparator != nullptr && second->loopSeparator != nullptr)
									{
										if (strcasecmp(first->loopSeparator, second->loopSeparator) != 0)
										{
											dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
										}
									}
									else if (first->loopSeparator != nullptr || second->loopSeparator != nullptr)
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
						if (c->branch )
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

		dsStatus& checkAndTryMerge(nodeInfo*& first, nodeInfo*& second)
		{
			if (first == nullptr || second == nullptr)
				dsOk();
			if (first->compare(*second))
			{
				second = nullptr;
				dsOk();
			}
			if (first->loop || second->loop)//can not merge loop node
				dsReturn(nodeConflictCheck(first, second));

			if (first->optional || second->optional)
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
			else //both has child
			{
				//or is true ,means loop is true
				if (first->branch && second->branch )
				{
					if (nodeInfo::compareLoop(*first, *second))
					{
						nodeInfo** nodes = new nodeInfo * [first->childCount + second->childCount];
						for (int i = 0; i < first->childCount; i++)
							nodes[i] = first->child[i];
						for (int i = 0; i < second->childCount; i++)
							nodes[i+ second->childCount] = second->child[i];
						delete []second->child;
						delete []first->child;
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
				else if (first->branch && !second->branch )
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
				else if (!first->branch && second->branch )
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
					for (; idx < min(first->childCount, second->childCount); idx++)
					{
						if (first->child[idx]->compare(*second->child[idx]))
							continue;
						else
							break;
					}
					if (idx == 0)
						dsOk();
					if (idx == min(first->childCount, second->childCount))
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
		dsStatus& optimizeChild(nodeInfo* node)
		{
			if (node->child == nullptr)
				dsOk();
			for (int i = 0; i < node->childCount; i++)
			{
				if (node->child[i]->child != nullptr)
					dsReturnIfFailed(optimizeChild(node->child[i]));
			}
			if (node->branch )
			{
				for (int i = 0; i < node->childCount; i++)
				{
					nodeInfo* c = node->child[i];
					if (c->branch )
					{
						if (c->loop)
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
								dsReturnIfFailed(nodeConflictCheck(c->child[m], node->child[j]));
							}
						}
						nodeInfo** tmp = new nodeInfo * [node->childCount - 1 + c->childCount - mergedCount];
						for (int m = 0; m < i; m++)
							tmp[m] = node->child[m];
						for (int m = i; m < node->childCount - 1; m++)
							tmp[m] = node->child[m + i];
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
					if (node->loop && c->loop)
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
		dsStatus& loadFromFile(const char* filePath)
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
			if (!dsCheck(load(buf)))
			{
				delete buf;
				dsReturn(getLocalStatus());
			}
			else
			{
				delete buf;
				dsOk();
			}
		}
		dsStatus& load(const char* grammar)
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
				nodeInfo* c = nullptr;
				if (!dsCheck(parseNode(m_grammarTreeInJson, static_cast<const jsonObject*>(iter->second), c)))
				{
					delete m_grammarTreeInJson;
					m_grammarTreeInJson = nullptr;
					dsReturn(getLocalStatus());
				}
				const jsonValue* isHead = static_cast<jsonObject*>(iter->second)->get("is head");
				if (isHead != nullptr)
				{
					if (isHead->t != JSON_TYPE::J_BOOL)
					{
						delete m_grammarTreeInJson;
						m_grammarTreeInJson = nullptr;
						delete c;
						dsFailedAndLogIt(LOAD_GRAMMAR_FAILED, "parse grammar failed", ERROR);
					}
					if (static_cast<const jsonBool*>(isHead)->m_value)
						m_headNodes.insert(std::pair<std::string, nodeInfo*>(iter->first, c));
				}
				m_allNodes.insert(std::pair<std::string, nodeInfo*>(iter->first, c));
			}
			//delete grammarTree;
		}
		dsStatus& optimize()
		{
			m_root.branch = true;
			m_root.childCount = m_headNodes.size();
			m_root.child = new nodeInfo * [m_root.childCount];
			int idx = 0;
			for (std::map<std::string, nodeInfo*>::iterator iter = m_headNodes.begin(); iter != m_headNodes.end(); iter++)
				m_root.child[idx++] = iter->second;
			dsReturn(optimizeChild(&m_root));
		}
		bool compare(const lex & lr) const
		{
			return m_root.compare(lr.m_root);
		}
	};
}
