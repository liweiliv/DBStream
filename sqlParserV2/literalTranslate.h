#pragma once
#include <functional>
#include "token.h"
namespace SQL_PARSER
{
	class literalTranslate {
	public:
		bool m_rule[static_cast<int>(literalType::UNKNOWN)][static_cast<int>(literalType::UNKNOWN)];
		std::function<bool(literal*)> m_ruleFunc[static_cast<int>(literalType::UNKNOWN)][static_cast<int>(literalType::UNKNOWN)];
		void init()
		{
			memset(m_rule, 0, sizeof(m_rule));
			m_rule[static_cast<int>(literalType::CHARACTER_STRING)][static_cast<int>(literalType::NUMBER_VALUE)] = true;
		}
	public:
		inline bool canTrans(literalType src, literalType dest) const
		{
			return m_rule[static_cast<int>(src)][static_cast<int>(dest)];
		}
	};
}