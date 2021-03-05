#include "sqlParser.h"
namespace SQL_PARSER {
	dsStatus& sqlParser::parseOneSentence(sqlHandle* handle, const char*& sqlStr, sql* s)
	{
		sqlParserStack* stack = getStack();
		stack->clear();
		const char* pos = sqlStr;
		nextWordPos(pos);
		const char* start = pos;
		while (!KEY_CHAR[*pos])
			pos++;
		str firstWord(start, pos - start);
		grammarHashMap::const_iterator iter = m_grammarTree.find(&firstWord);
		if (iter == m_grammarTree.end())
			dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		const grammarToken* gt = iter->second;
		if (gt->func != nullptr)
		{
			sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
			pair->init(nullptr, gt->func);
			s->add(pair);
		}
		for (;;)
		{
			nextWordPos(pos);
			if (*pos == '\0' || *pos == ';')
			{
				if (gt->leaf)
					dsOk();
				else
					dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
			}

			if (gt->symbNext != nullptr)
			{
				if (memcmp(gt->symbNext->word.value.pos, pos, gt->symbNext->word.value.size) == 0)
				{
					pos += gt->symbNext->word.value.size;
					gt = gt->symbNext;
					if (gt->func != nullptr)
					{
						sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
						pair->init(nullptr, gt->func);
						s->add(pair);
					}
					continue;
				}
			}

			if (gt->keyWordNext != nullptr)
			{
				if (strncmp(gt->symbNext->word.value.pos, pos, gt->symbNext->word.value.size) == 0)
				{
					pos += gt->keyWordNext->word.value.size;
					gt = gt->keyWordNext;
					if (gt->func != nullptr)
					{
						sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
						pair->init(nullptr, gt->func);
						s->add(pair);
					}
					continue;
				}
			}
			else if (!gt->nextKeyWordMap.empty())
			{
				const char* tmp = pos;
				while (!KEY_CHAR[*tmp])
					tmp++;
				if (tmp != pos)
				{
					str keyWord(pos, tmp - pos);
					grammarHashMap::iterator iter = gt->nextKeyWordMap.find(&keyWord);
					if (iter != gt->nextKeyWordMap.end())
					{
						gt = iter->second;
						if (gt->func != nullptr)
						{
							sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
							pair->init(nullptr, gt->func);
							s->add(pair);
						}
						pos = tmp;
					}
				}
				continue;
			}

			if (gt->identiferNext != nullptr)
			{
				token* next = nullptr;
				dsReturnIfFailed(matchIdentifier(stack, next, pos));
				gt = gt->identiferNext;
				if (gt->func != nullptr)
				{
					sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
					pair->init(next, gt->func);
					s->add(pair);
				}
				continue;
			}

			if (gt->valueNext != nullptr)
			{
				token* next = nullptr;
				const char* tmp = pos;
				dsReturnIfFailed(matchToken(stack, next, tmp, true, true));
				if (gt->valueNext->matchAnyValueToekn || next->type == tokenType::literal && gt->valueNext->type == static_cast<literal*>(next)->lType)
				{
					gt = gt->identiferNext;
					if (gt->func != nullptr)
					{
						sqlValueFuncPair* pair = (sqlValueFuncPair*)stack->arena.AllocateAligned(sizeof(sqlValueFuncPair));
						pair->init(next, gt->func);
						s->add(pair);
					}
					pos = tmp;
					continue;
				}
			}
			dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << pos, ERROR);
		}
	}
}