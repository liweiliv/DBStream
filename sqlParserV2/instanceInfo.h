#pragma once
#include <stdint.h>
#include <string>
#include "meta/charset.h"
namespace SQL_PARSER
{
	enum class DATABASE_TYPE {
		ORACLE,
		MYSQL,
		POSTGRESQL,
		SQL_SERVER,
		DB2
	};

	struct instanceInfo {
		DATABASE_TYPE type;
		uint64_t versionCode;
		std::string version;
		bool m_casesensitive;
		CHARSET charset;
	};
}