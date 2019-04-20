/*
 * stackLog.cpp
 *
 *  Created on: 2017年9月20日
 *      Author: liwei
 */

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <thread>
#include <mutex>
#include <stdarg.h>
#define OS_WIN //todo
#ifdef OS_LINUX
#include <execinfo.h>
#else ifdef OS_WIN
#include <Windows.h>
#include <DbgHelp.h>
#endif
#include "stackLog.h"
#include <string.h>
using namespace std;
struct stackLog
{
    std::thread::id tid;
    void * stacks[512];
    char ** logs;
    int * codes;
    int stacksSize;
};
static std::map<std::thread::id,stackLog*> *stackLogMap = NULL;
static std::mutex * stackLogMapLock = NULL;
static void __cleanStackLog(stackLog * sl)
{
    if(sl == NULL)
        return;
    if(sl->codes)
    {
        free(sl->codes);
        sl->codes = NULL;
    }
    memset(sl->stacks,0,sizeof(sl->stacks));
    if(sl->logs)
    {
        for(int i=0;i<sl->stacksSize;i++)
        {
            if(sl->logs[i]!=NULL)
                free(sl->logs[i]);
        }
        free(sl->logs);
        sl->logs = NULL;
    }
    sl->stacksSize = 0;
}
int initStackLog()
{
    if(stackLogMap!=NULL||stackLogMapLock!=NULL)
        return -1;
    stackLogMap = new map<std::thread::id,stackLog*>;
	stackLogMapLock = new std::mutex();
    return 0;
}

int destroyStackLog()
{
    if(stackLogMap==NULL||stackLogMapLock==NULL)
        return -1;
    for(std::map<std::thread::id,stackLog*>::iterator iter = stackLogMap->begin();iter!=stackLogMap->end();iter++)
    {
        stackLog * sl = iter->second;
        __cleanStackLog(sl);
        free(sl);
    }
    delete stackLogMap;
    stackLogMap = NULL;
    delete stackLogMapLock;
    stackLogMapLock = NULL;
    return 0;
}
/*
 * 获取当前线程的stackLog，默认不存在返回NULL，如果create =true ，不存在则为当前线程创建一个
 * */
static stackLog * getCurrentThreadStackLog(bool create = false)
{
    stackLog * sl = NULL;
    std::thread::id tid = std::this_thread::get_id();
	stackLogMapLock->lock();
    std::map<std::thread::id,stackLog*>::iterator iter = stackLogMap->find(tid);
    if(iter==stackLogMap->end())
    {
		stackLogMapLock->unlock();
        if(create)
        {
            sl = (stackLog*)malloc(sizeof(stackLog));
            if(sl==NULL)
                abort();
            sl->tid = tid;
            sl->logs = NULL;
            sl->codes = NULL;
            sl->stacksSize =0;
            memset(sl->stacks,0,sizeof(sl->stacks));
			stackLogMapLock->lock();
            stackLogMap->insert(pair<std::thread::id,stackLog*>(tid,sl));
			stackLogMapLock->unlock();
            return sl;
        }
        else
            return NULL;
    }
    else
    {
        sl = iter->second;
		stackLogMapLock->unlock();
        return sl;
    }
}
/*清理当前线程的stacklog
 * */
void cleanStackLog()
{
    if(stackLogMap==NULL||stackLogMapLock==NULL)
        abort();
    __cleanStackLog(getCurrentThreadStackLog());
}
#ifdef OS_WIN
long expt_handler(LPEXCEPTION_POINTERS ep)
{
	STACKFRAME64 sf = { 0 };
#ifdef _M_IX86 // ignore IA64
	auto image_type = IMAGE_FILE_MACHINE_I386;
	sf.AddrPC.Offset = ep->ContextRecord->Eip;
	sf.AddrFrame.Offset = sf.AddrStack.Offset = ep->ContextRecord->Esp;
#elif _M_X64
	auto image_type = IMAGE_FILE_MACHINE_AMD64;
	sf.AddrPC.Offset = ep->ContextRecord->Rip;
	sf.AddrFrame.Offset = sf.AddrStack.Offset = ep->ContextRecord->Rsp;
#endif
	sf.AddrPC.Mode = sf.AddrFrame.Mode = sf.AddrStack.Mode = AddrModeFlat;

	PIMAGEHLP_SYMBOL64 sym = (IMAGEHLP_SYMBOL64 *)malloc(sizeof(IMAGEHLP_SYMBOL64) + 100);
	if (!sym)return EXCEPTION_CONTINUE_SEARCH;
	memset(sym, 0, sizeof(IMAGEHLP_SYMBOL64) + 100);
	sym->SizeOfStruct = sizeof(IMAGEHLP_SYMBOL64);
	sym->MaxNameLength = 100;

	IMAGEHLP_LINE64 line = { 0 };
	line.SizeOfStruct = sizeof(line);
	CONTEXT ctx = *ep->ContextRecord;
	while (StackWalk(image_type, (HANDLE)-1, (HANDLE)-2, &sf, &ctx, 0, SymFunctionTableAccess64, SymGetModuleBase64, 0)) {
		DWORD64 offset = 0;
		DWORD offset_for_line = 0;
		CHAR und_fullname[100];

		if (sf.AddrPC.Offset != 0) {
			if (SymGetSymFromAddr64((HANDLE)-1, sf.AddrPC.Offset, &offset, sym)) {
				UnDecorateSymbolName(sym->Name, und_fullname, 100, UNDNAME_COMPLETE);
				//cout << und_fullname;
			}

			if (SymGetLineFromAddr64((HANDLE)-1, sf.AddrPC.Offset, &offset_for_line, &line)) {
			//	cout << " " << line.FileName << "(" << line.LineNumber << ")";
			}
			//cout << endl;
		}
	}
	return EXCEPTION_EXECUTE_HANDLER;
}
/*
 * 写log，一般使用SET_STACE_LOG_AND_RETURN或者SET_STACE_LOG来调用
 */
void setStackLog(int codeLine,const char * func,const char * file,int code,const char * fmt,...)
{
   
}

int getChildLogDetail(int &code,const char *&log)
{
	return 0;
}
int getChildLog(std::string &errorLog)
{
	const char * log;
	int code;
	if(getChildLogDetail(code,log)!=0||log==NULL)
		return -1;
	errorLog = log;
	return 0;
}
void  getFullStackLog(string &log)
{
  
    return ;
}
#else ifdef OS_LINUX
/*
 * 写log，一般使用SET_STACE_LOG_AND_RETURN或者SET_STACE_LOG来调用
 */
void setStackLog(int codeLine, const char * func, const char * file, int code, const char * fmt, ...)
{
	stackLog * sl = getCurrentThreadStackLog(true);
	void * stack[256] = { 0 };
	/*获取函数栈*/
	int stack_num = backtrace(stack, 256);
	/**/
	if (stack_num >= sl->stacksSize + 1 || memcmp(&sl->stacks[sl->stacksSize - stack_num + 2], &stack[2], sizeof(void*)*(stack_num - 2)) != 0)
	{
		__cleanStackLog(sl);
		sl->stacksSize = stack_num - 1;
		sl->codes = (int*)malloc(sl->stacksSize * sizeof(int));
		memset(sl->codes, 0, sl->stacksSize * sizeof(int));
		sl->logs = (char**)malloc(sl->stacksSize * sizeof(char*));
		memset(sl->logs, 0, sl->stacksSize * sizeof(char*));
		memcpy(sl->stacks, &stack[1], sizeof(void*)*(stack_num - 1));
	}
	char * msg = NULL;
	int msgLen = 0;
	if (fmt != NULL && fmt[0] != '\0')
	{
		va_list ap;
		va_start(ap, fmt);
		msgLen = vasprintf(&msg, fmt, ap);
		va_end(ap);
		if (msgLen == -1)
			abort();
	}
	char * fullMsg;
	msgLen = asprintf(&fullMsg, "@%s:%d.%s() [%s]", file == NULL ? "unknown source file" : file, codeLine, func, msg);
	if (msgLen == -1)
		abort();
	free(msg);
	sl->codes[stack_num - 2] = code;
	sl->logs[stack_num - 2] = fullMsg;
}

int getChildLogDetail(int &code, const char *&log)
{
	stackLog *sl = getCurrentThreadStackLog();
	if (sl == NULL)
		return -1;
	void * stack[256] = { 0 };
	int stack_num = backtrace(stack, 256);
	if (stack_num - 1 >= sl->stacksSize)
		return -1;
	else
	{
		code = sl->codes[stack_num - 1];
		log = sl->logs[stack_num - 1];
		return 0;
	}
}
int getChildLog(std::string &errorLog)
{
	const char * log;
	int code;
	if (getChildLogDetail(code, log) != 0 || log == NULL)
		return -1;
	errorLog = log;
	return 0;
}
void  getFullStackLog(string &log)
{
	stackLog *sl = getCurrentThreadStackLog();
	if (sl == NULL || sl->stacksSize == 0)
		return;
	char ** stacktrace = backtrace_symbols(sl->stacks, sl->stacksSize);
	for (int i = sl->stacksSize - 1; i >= 0; i--)
	{
		if (sl->logs[i] == NULL)
		{
			log += "@";
			log += stacktrace[sl->stacksSize - i - 1];
		}
		else
			log += sl->logs[i];
		if (i != 0)
			log += '\n';
		else
			break;
	}
	return;
}

#endif



