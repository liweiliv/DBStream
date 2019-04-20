#pragma once
#include <string>
namespace SQL_PARSER
{
	enum parseValue
	{
		OK, NOT_MATCH, INVALID, COMMENT, NOT_SUPPORT
	};
	struct handle;
	struct statusInfo
	{
		parseValue(*parserFunc)(handle* h, const std::string &sql);
		std::string sql;
		statusInfo * next;
		statusInfo() :parserFunc(NULL), next(NULL) {}
	};
	struct handle
	{
		std::string dbName;
		void * userData;
		statusInfo * head;
		statusInfo * end;
		handle * next;
		void(*destroyUserDataFunc)(handle *h);
		handle(void *_userData = nullptr) :
			userData(_userData), head(NULL), end(NULL), next(NULL), destroyUserDataFunc(nullptr)
		{
		}
		~handle()
		{
			if (head != NULL)
			{
				while (head != NULL)
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
			while (next != NULL)
			{
				handle * tmp = next->next;
				next->next = NULL;
				delete next;
				next = tmp;
			}
			if (userData&&destroyUserDataFunc)
				destroyUserDataFunc(this);
		}
	};
}
