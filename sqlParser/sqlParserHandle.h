#pragma once
#include <string>
namespace SQL_PARSER
{
	enum parseValue
	{
		OK, NOT_MATCH, INVALID, COMMENT, NOT_SUPPORT
	};
	enum SQL_TYPE
	{
		BEGIN,
		COMMIT,
		ROLLBACK,
		INSERT_INTO,
		DELETE_FROM,
		UPDATE_SET,
		REPLACE,
		SELECT,
		USE_DATABASE,
		CREATE_DATABASE,
		DROP_DATABASE,
		ALTER_DATABASE,
		CREATE_TABLE,
		CREATE_TABLE_LIKE,
		RENAME_TABLE,
		DROP_TABLE,
		CREATE_INDEX,
		DROP_INDEX,
		ALTER_TABLE_DROP_FOREIGN_KEY,
		ALTER_TABLE_DROP_PRIMARY_KEY,
		ALTER_TABLE_DROP_INDEX,
		ALTER_TABLE_DROP_COLUMN,
		ALTER_TABLE_ADD_KEY,
		ALTER_TABLE_ADD_CONSTRAINT,
		ALTER_TABLE_ADD_COLUMN,
		ALTER_TABLE_ADD_COLUMNS,
		ALTER_TABLE_CHANGE_COLUMN,
		ALTER_TABLE_MODIFY_COLUMN,
		MAX_SUPPORT,
		UNSUPPORT = 255
	};
	struct handle;
	struct statusInfo
	{
		parseValue(*parserFunc)(handle* h, const std::string &sql);
		std::string sql;
		statusInfo * next;
		statusInfo() :parserFunc(nullptr), next(nullptr) {}
	};
	struct handle
	{
		uint8_t charset;
		SQL_TYPE type;
		std::string dbName;
		void * userData;
		statusInfo * head;
		statusInfo * end;
		handle * next;
		void(*destroyUserDataFunc)(handle *h);
		handle(void *_userData = nullptr) :charset(0), type(UNSUPPORT),
			userData(_userData), head(nullptr), end(nullptr), next(nullptr), destroyUserDataFunc(nullptr)
		{
		}
		~handle()
		{
			if (head != nullptr)
			{
				while (head != nullptr)
				{
					statusInfo * tmp = head->next;
					delete head;
					if (head == end)
						break;
					head = tmp;
				}
			}
			/*不递归调用next的析构函数，使用循环的方式，
			 * 避免解析较大的测试文件形成了巨大的handle链表在析构时堆栈超过上限*/
			while (next != nullptr)
			{
				handle * tmp = next->next;
				next->next = nullptr;
				delete next;
				next = tmp;
			}
			if (userData&&destroyUserDataFunc)
				destroyUserDataFunc(this);
		}
	};
}
