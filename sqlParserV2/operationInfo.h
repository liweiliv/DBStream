#pragma once
#include "token.h"
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
		{LEFT_SQUARE_BRACKET,1,POSITION,false,false,"["},
		{RIGHT_SQUARE_BRACKET,1,POSITION,false,false,"]"},
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
		{NOT_EQUAL,7,LOGIC,true,true,"<>"},
		/*,
		{AND_W,11,LOGIC,true,true,"AND"},
		{OR_W,12,LOGIC,true,true,"OR"},
		{IN_,2,LOGIC,true,true,"IN ("},
		{NOT_IN,2,LOGIC,true,true,"NOT IN ("}
		*/
	};
	class operationInfoTree {
	private:
		struct node {
			char value;
			node* child[256];
			operatorSymbol* operations[256];
			node(char v) :value(v) {
				memset(child, 0, sizeof(child));
				memset(operations, 0, sizeof(operations));
			}
		};
		node m_root;
	private:
		operatorSymbol* createSqlValue(const operationInfo * op)
		{
			operatorSymbol* v = new operatorSymbol();
			v->op = op;
			v->value.assign(op->signStr, strlen(op->signStr));
			v->type = tokenType::symbol;
			return v;
		}
	public:
		operationInfoTree() :m_root('\0') 
		{
			for (uint32_t i = 0; i < sizeof(operationInfos) / sizeof(operationInfo); i++)
				add(&operationInfos[i]);
		}
		void add(const operationInfo* info)
		{
			const uint8_t * s = (const uint8_t *)&info->signStr[0];
			if (*(s + 1) == '\0')
			{
				if (m_root.operations[*s] == nullptr)
					m_root.operations[*s] = createSqlValue(info);
			}
			else if (*(s + 2) == '\0')
			{
				if (m_root.child[*s] == nullptr)
					m_root.child[*s] = new node(*s);
				if (m_root.child[*s]->operations[*(s + 1)] == nullptr)
					m_root.child[*s]->operations[*(s + 1)] = createSqlValue(info);
			}
			else
			{
				node* n = &m_root;
				while (*s != '\0')
				{
					if (*(s + 1) == '\0')
					{
						if (n->operations[*s] != nullptr)
							n->operations[*s] = createSqlValue(info);
						return;
					}
					else
					{
						if (n->child[*s] == nullptr)
							n->child[*s] = new node(*s);
						n = n->child[*s];
						s++;
					}
				}
			}
		}
		operatorSymbol* match(char*& sql)
		{
			node* n = &m_root;
			for(;;)
			{
				uint8_t c = (uint8_t)*sql;
				if (*(sql + 1) == '\0')
				{
					if (n->operations[c] != nullptr)
					{
						sql++;
						return n->operations[c];
					}
					return nullptr;
				}
				if (n->child[c] != nullptr)
				{
					if (n->child[c]->operations[(uint8_t)*(sql + 1)] != nullptr || n->child[c]->child[(uint8_t)*(sql + 1)] != nullptr)
					{
						sql++;
						n = n->child[c];
					}
					else
					{
						if (n->operations[c] != nullptr)
						{
							sql++;
							return n->operations[c];
						}
						return nullptr;
					}
				}
				else
				{
					if (n->operations[c] != nullptr)
					{
						sql++;
						return n->operations[c];
					}
					return nullptr;
				}
			}
		}
	};
}
