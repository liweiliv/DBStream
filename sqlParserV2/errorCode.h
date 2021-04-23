#pragma once
namespace SQL_PARSER {
	enum errorCode {
		OK,
		LOAD_GRAMMAR_FAILED,
		SYNTAX_ERROR,
		OVER_LIMIT
	};
}