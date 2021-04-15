#include "sqlParser.h"
namespace SQL_PARSER {
	DLL_EXPORT DS sqlParser::parseOneSentence(sqlHandle* handle, char*& sqlStr, sql* s)
	{
		dsFailedAndLogIt(errorCode::SYNTAX_ERROR, "not match :" << sqlStr, ERROR);
	}
}