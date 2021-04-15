#include "sqlHandle.h"
#include "sqlStack.h"
namespace SQL_PARSER
{
	DLL_EXPORT sqlHandle::sqlHandle():userData(nullptr), uid(0), tid(0), stack(new sqlParserStack()), literalTrans(nullptr), sqlList(nullptr), tail(nullptr), sqlCount(0)
	{

	}
	DLL_EXPORT sqlHandle::~sqlHandle()
	{
		if (stack != nullptr)
			delete stack;
	}
}