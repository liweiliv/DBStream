#include "token.h"
namespace SQL_PARSER {
	DLL_EXPORT bool token::compare(const token& t)
	{
		if (type !=t.type)
			return false;
		switch (type)
		{
		case tokenType::identifier:
			break;
		case tokenType::keyword:
		case tokenType::specialCharacter:
		case tokenType::symbol:
			if (value.compare(t.value) != 0)
				return false;
			break;
		case tokenType::literal:
		{
			if (static_cast<literal*>(this)->lType != static_cast<const literal*>(&t)->lType)
				return false;
			break;
		}
		default:
			break;
		}
		return true;
	}
}