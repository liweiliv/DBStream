#pragma once
#include "util/winString.h"
#include "sqlParserUtil.h"
namespace SQL_PARSER {
	enum OPERATOR {
		LEFT_BRACKET,//(
		RIGHT_BRACKET,//)
		LEFT_SQUARE_BRACKET,//[
		RIGHT_SQUARE_BRACKET,//]
		LEFT_BRACE,//{
		RIGHT_BRACE,//}
		DIVISION_EQUAL,// /=
		MULTIPLE_EQUAL,// *=
		REMAINDER_EQUAL,// %=
		PLUS_EQUAL, //+=
		SUBTRACT_EQUAL,//-=
		LEFT_SHIFT_EQUAL, // <<=
		RIGHT_SHIFT_EQUAL, // >>=
		BIT_AND_EQUAL,// &=
		BIT_OR_EQUAL,// |=
		XOR_EQUAL, //^=
		POINT,//.
		PLUS_PLUS,//++
		SUBTRACT_SUBTRACT,//--
		GREATER_EQUAL,//>=
		LESS_EQUAL,//<=
		EQUAL_EQUAL,//==
		NOT_EQUAL,//!=
		AND,//&&
		OR,//||
		EXCLAMATION_MARK,//!
		TILDE,//~
		MULTIPLE,//*
		DIVISION,// /
		REMAINDER,// %
		PLUS,//+
		SUBTRACT,//-
		LEFT_SHIFT,// <<
		RIGHT_SHIFT,// >>
		GREATER_THAN,//>
		LESS_THAN,//<
		BIT_AND,//&
		XOR,//^
		BIT_OR,//|
		QUESTION,//?:
		EQUAL,//=
		AND_W,// AND
		OR_W,// OR
		IN_,//IN
		NOT_IN,//NOT IN
		NOT_OPERATION
	};
	enum OPERATION_TYPE
	{
		POSITION,
		MATHS,
		CHANGE_VALUE,
		LOGIC
	};
	struct operationInfo
	{
		OPERATOR type;
		uint8_t priority;
		OPERATION_TYPE optType;
		bool hasRightValue;
		bool hasLeftValues;
		char signStr[7];
	};
	constexpr static operationInfo operationInfos[] = {
		{LEFT_BRACKET,1,POSITION,false,false,"("},
		{RIGHT_BRACKET,1,POSITION,false,false,")"},
		{LEFT_SQUARE_BRACKET,1,POSITION,false,"["},
		{RIGHT_SQUARE_BRACKET,1,POSITION,false,"]"},
		{LEFT_BRACE,1,POSITION,false,false,"{"},
		{RIGHT_BRACE,1,POSITION,false ,false,"}"},
		{DIVISION_EQUAL,14,CHANGE_VALUE,true,true,"/="},
		{MULTIPLE_EQUAL,14,CHANGE_VALUE,true,true,"*="},
		{REMAINDER_EQUAL,14,CHANGE_VALUE,true,true,"%="},
		{PLUS_EQUAL,14,CHANGE_VALUE,true,true,"+="},
		{SUBTRACT_EQUAL,14,CHANGE_VALUE,true,true,"-="},
		{LEFT_SHIFT_EQUAL,14,CHANGE_VALUE,true,true,"<<="},
		{RIGHT_SHIFT_EQUAL,14,CHANGE_VALUE,true,true,">>="},
		{BIT_AND_EQUAL,14,CHANGE_VALUE,true,true,"&="},
		{BIT_OR_EQUAL,14,CHANGE_VALUE,true,true,"|="},
		{XOR_EQUAL,14,CHANGE_VALUE,true,true,"^="},
		{POINT,1,POSITION,true,true,"."},
		{PLUS_PLUS,2,CHANGE_VALUE,true,false,"++"},
		{SUBTRACT_SUBTRACT,2,CHANGE_VALUE,true,false,"--"},
		{GREATER_EQUAL,6,LOGIC,true,true,">="},
		{LESS_EQUAL,6,LOGIC,true,true,"<="},
		{EQUAL_EQUAL,7,LOGIC,true,true,"=="},
		{NOT_EQUAL,7,LOGIC,true,true,"!="},
		{AND,11,LOGIC,true,true,"&&"},
		{OR,12,LOGIC,true,true,"||"},
		{EXCLAMATION_MARK,2,LOGIC,false,true,"!"},
		{TILDE,2,MATHS,false,true,"~"},
		{MULTIPLE,3,MATHS,true,true,"*"},
		{DIVISION,3,MATHS,true,true,"/"},
		{REMAINDER,3,MATHS,true,true,"%"},
		{PLUS,4,MATHS,true,true,"+"},
		{SUBTRACT,4,MATHS,true,true,"-"},
		{LEFT_SHIFT,5,MATHS,true,true,"<<"},
		{RIGHT_SHIFT,5,MATHS,true,true,">>"},
		{GREATER_THAN,6,LOGIC,true,true,">"},
		{LESS_THAN,6,LOGIC,true,true,"<"},
		{BIT_AND,8,MATHS,true,true,"&"},
		{XOR,9,MATHS,true,true,"^"},
		{BIT_OR,10,MATHS,true,true,"|"},
		{QUESTION,13,MATHS,true,true,"?"},
		{EQUAL,14,MATHS,true,true,"="},
		{AND_W,11,LOGIC,true,true,"AND"},
		{OR_W,12,LOGIC,true,true,"OR"},
		{IN_,2,LOGIC,true,true,"IN"},
		{NOT_IN,2,LOGIC,true,true,"NOT IN"}
	};
	static OPERATOR parseOperation(const char*& data)
	{
		for (uint8_t idx = 0; idx < AND_W; idx++)
		{
			uint8_t len = 0;
			do {
				if (operationInfos[idx].signStr[len] != data[len])
					break;
			} while (operationInfos[idx].signStr[++len] != '\0');
			if (operationInfos[idx].signStr[len] == '\0')
			{
				data += len;
				return operationInfos[idx].type;
			}
		}
		for (uint8_t idx = AND_W; idx < NOT_IN; idx++)
		{
			if (strncasecmp(operationInfos[idx].signStr, data, strlen(operationInfos[idx].signStr)) == 0)
			{
				data += strlen(operationInfos[idx].signStr);
				return operationInfos[idx].type;
			}
		}
		for (uint8_t idx = NOT_IN; idx < NOT_OPERATION; idx++)
		{
			const char* src = operationInfos[idx].signStr;
			const char* dest = data;
			const char* wordEnd = endOfWord(src);
			while (*src != '\0')
			{
				if (strncasecmp(src, dest, wordEnd - src) != 0)
					break;
				dest += wordEnd - src;
				if (*wordEnd != '\0')
				{
					if (*dest != ' ' || *dest != '\t')
						break;
				}
				src = nextWord(wordEnd);
				wordEnd = endOfWord(src);
			}
			if (*src == '\0')
			{
				data = dest;
				return operationInfos[idx].type;
			}
		}
		return NOT_OPERATION;
	}
}
