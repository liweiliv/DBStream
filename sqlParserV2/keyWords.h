#pragma once
#include <functional>
#include "util/status.h"
#include "str.h"
#include "token.h"
namespace SQL_PARSER {

	constexpr static char* const reserverdKeyWords[] = {
	"ABSOLUTE", "CASCADED", "CURRENT_ROLE",
	"ACTION", "CASE", "CURRENT_TIME",
	"ADD", "CAST", "CURRENT_TIMESTAMP",
	"ADMIN", "CATALOG", "CURRENT_USER",
	"AFTER", "CHAR", "CURSOR",
	"AGGREGATE", "CHARACTER", "CYCLE",
	"ALIAS", "CHECK", "DATA",
	"ALL", "CLASS", "DATALINK",
	"ALLOCATE", "CLOB", "DATE",
	"ALTER", "CLOSE", "DAY",
	"AND", "COLLATE", "DEALLOCATE",
	"ANY", "COLLATION", "DEC",
	"ARE", "COLUMN", "DECIMAL",
	"ARRAY", "COMMIT", "DECLARE",
	"AS", "COMPLETION", "DEFAULT",
	"ASC", "CONDITION", "DEFERRABLE",
	"ASSERTION", "CONNECT", "DEFERRED",
	"AT", "CONNECTION", "DELETE",
	"AUTHORIZATION", "CONSTRAINT", "DEPTH",
	"BEFORE", "CONSTRAINTS", "DEREF",
	"BEGIN", "CONSTRUCTOR", "DESC",
	"BINARY", "CONTAINS", "DESCRIBE",
	"BIT", "CONTINUE", "DESCRIPTOR",
	"BLOB", "CORRESPONDING", "DESTROY",
	"BOOLEAN", "CREATE", "DESTRUCTOR",
	"BOTH", "CROSS", "DETERMINISTIC",
	"BREADTH", "CUBE", "DICTONARY",
	"BY", "CURRENT", "DIAGNOSTICS",
	"CALL", "CURRENT_DATE", "DISCONNECT",
	"CASCADE", "CURRENT_PATH", "DISTINCT",
	"FULL", "LANGUAGE", "OF",
	"FUNCTION", "LARGE", "OFF",
	"GENERAL", "LAST", "OLD",
	"GET", "LATERAL", "ON",
	"GLOBAL", "LEADING", "ONLY",
	"GO", "LEAVE", "OPEN",
	"GOTO", "LEFT", "OPERATION",
	"GRANT", "LESS", "OPTION",
	"GROUP", "LEVEL", "OR",
	"GROUPING", "LIKE", "ORDER",
	"HANDLER", "LIMIT", "ORDINALITY",
	"HAVING", "LOCAL", "OUT",
	"HASH", "LOCALTIME", "OUTER",
	"HOST", "LOCALTIMESTAMP", "OUTPUT",
	"HOUR", "LOCATOR", "PAD",
	"IDENTITY", "LOOP", "PARAMETER",
	"IF", "MATCH", "PARAMETERS",
	"IGNORE", "MEETS", "PARTIAL",
	"IMMEDIATE", "MINUTE", "PATH",
	"IN", "MODIFIES", "PERIOD",
	"INDICATOR", "MODIFY", "POSTFIX",
	"INITIALIZE", "MODULE", "PRECEDES",
	"INITIALLY", "MONTH", "PRECISION",
	"INNER", "NAMES", "PREFIX",
	"INOUT", "NATIONAL", "PREORDER",
	"INPUT", "NATURAL", "PREPARE",
	"INSERT", "NCHAR", "PRESERVE",
	"INT", "NCLOB", "PRIMARY",
	"INTEGER", "NEW", "PRIOR",
	"INTERSECT", "NEXT", "PRIVILEGES",
	"INTERVAL", "NO", "PROCEDURE",
	"INTO", "NONE", "PUBLIC",
	"IS", "NORMALIZE", "READ",
	"ISOLATION", "NOT", "READS",
	"ITERATE", "NULL", "REAL",
	"JOIN", "NUMERIC", "RECURSIVE",
	"KEY", "OBJECT", "REDO",
	"SQL", "THAN", "UNDER",
	"SQLEXCEPTION", "THEN", "UNDO",
	"SQLSTATE", "TIME", "UNION",
	"SQLWARNING", "TIMESTAMP", "UNIQUE",
	"START", "TIMEZONE_HOUR", "UNKNOWN",
	"STATE", "TIMEZONE_MINUTE", "UNTIL",
	"STATIC", "TO", "UPDATE",
	"STRUCTURE", "TRAILING", "USAGE",
	"SUCCEEDS", "TRANSACTION", "USER",
	"SYSTEM_USER", "TRANSLATION", "USING",
	"TABLE", "TREAT", "VALUE",
	"TEMPORARY", "TRIGGER", "VALUES",
	"TERMINATE", "TRUE", "VARCHAR"
	};

	constexpr static char* nonReserverdKeyWords[] = {
		"ABS", "CHARACTER_LENGTH", "CONDITION_NUMBER",
		"ADA", "CHARACTER_SET_CATALOG", "CONNECTION_NAME",
		"ASENSITIVE", "CHARACTER_SET_NAME", "CONSTRAINT_CATALOG",
		"ASSIGNMENT", "CHARACTER_SET_SCHEMA", "CONSTRAINT_NAME",
		"ASYMMETRIC", "CHECKED", "CONSTRAINT_SCHEMA",
		"ATOMIC", "CLASS_ORGIN", "CONTAINS",
		"AVG", "COALESCE", "CONTROL",
		"BETWEEN", "COBOL", "CONVERT",
		"BIT_LENGTH", "COLLATION_CATALOG", "COUNT",
		"BITVAR", "COLLATION_NAME", "CURSOR_NAME",
		"BLOCKED", "COLLATION_SCHEMA", "DATETIME_INTERVAL_CODE",
		"C", "COLUMN_NAME", "DATETIME_INTERVAL_PRECISION",
		"CARDINALITY", "COMMAND_FUNCTION", "DB",
		"CATALOG_NAME", "COMMAND_FUNCTION_CODE", "DISPATCH",
		"CHAIN", "COMMITTED", "DLCOMMENT",
		"CHAR_LENGTH", "CONCATENATE", "DLFILESIZE",
		"DLFILESIZEEXACT", "NULLABLE", "SERVER_NAME",
		"DLLINKTYPE", "NUMBER", "SIMPLE",
		"DLURLCOMPLETE", "NULLIF", "SOURCE",
		"DLURLPATH", "OCTET_LENGTH", "SPECIFIC_NAME",
		"DLURLPATHONLY", "OPTION", "SIMILAR",
		"DLURLSCHEMA", "OVERLAPS", "STRUCTURE",
		"DLURLSERVER", "OVERLAY", "SUBLIST",
		"DLVALUE", "OVERRIDING", "SUBSTRING",
		"DYNAMIC_FUNCTION", "PASCAL", "SUM",
		"DYNAMIC_FUNCTION_CODE", "PARAMETER_MODE", "STYLE",
		"EXISTING", "PARAMETER_ORDINAL_POSITION", "SUBCLASS_ORIGIN",
		"EXISTS", "PARAMETER_SPECIFIC_CATALOG", "SYMMETRIC",
		"EXTRACT", "PARAMETER_SPECIFIC_NAME", "SYSTEM",
		"FILE", "PARAMETER_SPECIFIC_SCHEMA", "TABLE_NAME",
		"FINAL", "PERMISSION", "TRANSACTIONS_COMMITTED",
		"FORTRAN", "PLI", "TRANSACTIONS_ROLLED_BACK",
		"GENERATED", "POSITION", "TRANSACTION_ACTIVE",
		"HOLD", "RECOVERY", "TRANSFORM",
		"INFIX", "REPEATABLE", "TRANSLATE",
		"INSENSITIVE", "RESTORE", "TRIGGER_CATALOG",
		"INSTANTIABLE", "RETURNED_LENGTH", "TRIGGER_SCHEMA",
		"INTEGRITY", "RETURNED_OCTET_LENGTH", "TRIGGER_NAME",
		"KEY_MEMBER", "RETURNED_SQLSTATE", "TRIM",
		"KEY_TYPE", "ROUTINE_CATALOG", "TYPE",
		"LENGTH", "ROUTINE_NAME", "UNCOMMITTED",
		"LINK", "ROUTINE_SCHEMA", "UNLINK",
		"LOWER", "ROW_COUNT", "UNNAMED",
		"MAX", "ROW_TYPE_CATALOG", "UPPER",
		"MIN", "ROW_TYPE_SCHEMA", "USER_DEFINED_TYPE_CATALOG",
		"MESSAGE_LENGTH", "ROW_TYPE_NAME", "USER_DEFINED_TYPE_NAME",
		"MESSAGE_OCTET_LENGTH", "SCALE", "USER_DEFINED_TYPE_SCHEMA",
		"MESSAGE_TEXT", "SCHEMA_NAME", "YES",
		"METHOD", "SELECTIVE",
		"MOD", "SELF",
		"MORE", "SENSITIVE",
		"MUMPS", "SERIALIZABLE",
		"NAME" 
	};

	struct sqlParserStack;
	struct keyWordInfo {
		std::function<dsStatus& (sqlParserStack*, token*&, const char*&)> func;
		bool couldBeValue;
	};

}
