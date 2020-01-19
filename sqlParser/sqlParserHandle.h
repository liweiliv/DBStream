#pragma once
#include <string>
#include "sqlValue.h"
namespace SQL_PARSER
{
	enum parseValue
	{
		OK, NOT_MATCH, INVALID, COMMENT, NOT_SUPPORT
	};

	struct handle;
	typedef parseValue(*parserFuncType)(handle*, SQLValue*);
	struct statusInfo
	{
		statusInfo* next;
		parserFuncType parserFunc;
		SQLValue* value;
		statusInfo() : next(nullptr), parserFunc(nullptr), value(nullptr) {}
		virtual parseValue process(handle* h)
		{
			if (parserFunc != nullptr)
				return parserFunc(h, value);
			else
				return OK;
		}
		virtual ~statusInfo()
		{
			if (value != nullptr && --value->ref <= 0)
				delete value;
		}
	};
	struct handle
	{
		uint8_t charset;
		std::string dbName;
		const char* sql;
		void* userData;
		statusInfo* head;
		statusInfo* end;
		handle* next;
		void(*destroyUserDataFunc)(handle* h);
		handle(void* _userData = nullptr) :charset(0), sql(nullptr),
			userData(_userData), head(nullptr), end(nullptr), next(nullptr), destroyUserDataFunc(nullptr)
		{
		}
		void addStatus(statusInfo* s)
		{
			if (head == nullptr)
				head = s;
			else
				end->next = s;
			end = s;
		}
		void rollbackTo(statusInfo* s)
		{
			statusInfo* status;
			if (s == nullptr)//save point is marked when end is null,clear all
				status = head;
			else
				status = s->next;
			while (status != nullptr)
			{
				statusInfo* tmp = status->next;
				delete status;
				status = tmp;
			}
			if (s != nullptr)
			{
				end = s;
				end->next = nullptr;
			}
			else
				head = end = nullptr;
		}
		~handle()
		{
			if (head != nullptr)
			{
				while (head != nullptr)
				{
					statusInfo* tmp = head->next;
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
				handle* tmp = next->next;
				next->next = nullptr;
				delete next;
				next = tmp;
			}
			if (userData && destroyUserDataFunc)
				destroyUserDataFunc(this);
		}
	};
}
