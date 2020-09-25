#pragma once
#include <string>
#include <vector>
#include "likely.h"
#include "file.h"
#include "String.h"
#include "winDll.h"
DLL_EXPORT class dsStatus
{
public:
	DLL_EXPORT struct stackInfo
	{
		std::string file;
		std::string func;
		int line;
		std::string errInfo;
		stackInfo(const char* file, const char* func, int line, const std::string& errInfo) :file(file), func(func), line(line), errInfo(errInfo) {}
		stackInfo(const char* file, const char* func, int line, const char* errInfo) :file(file), func(func), line(line), errInfo(errInfo == nullptr ? "" : errInfo) {}
		stackInfo(const  stackInfo& s) :file(s.file), func(s.func), line(s.line), errInfo(s.errInfo) {}
		stackInfo& operator=(const  stackInfo& s)
		{
			file = s.file;
			func = s.func;
			line = s.line;
			errInfo = s.errInfo;
			return *this;
		}
		~stackInfo() {}
	};
	int code;
	std::string errMessage;
	std::vector<stackInfo> stacks;
	DLL_EXPORT dsStatus() :code(0) {}
	DLL_EXPORT dsStatus(int code) :code(code) {}
	DLL_EXPORT dsStatus(int code, const std::string& errMessage) :code(code), errMessage(errMessage) {}
	DLL_EXPORT dsStatus(int code, const char* errMessage) :code(code), errMessage(errMessage) {}
	DLL_EXPORT dsStatus(const dsStatus& status) :code(status.code), errMessage(status.errMessage), stacks(status.stacks) {}
	DLL_EXPORT virtual ~dsStatus() {}
	DLL_EXPORT void clear()
	{
		code = 0;
		errMessage.clear();
		stacks.clear();
	}
	DLL_EXPORT void addStack(const stackInfo &s)
	{
		stacks.push_back(s);
	}
	DLL_EXPORT std::string toString()
	{
		std::string s;
		String _s;
		s.append(_s << "error code:" << code << ",error:" << errMessage << "\n");
		for (std::vector<stackInfo>::iterator iter = stacks.begin(); iter != stacks.end(); iter++)
		{
			s.append("\tat ").append(iter->func).append("(").append(iter->file).append(":").append(_s << iter->line).append(")");
			if (!iter->errInfo.empty())
				s.append("\t").append(iter->errInfo);
			s.append("\n");
		}
		return s;
	}
};
/*do not use*/
DLL_IMPORT extern dsStatus DS_OK;
DLL_EXPORT dsStatus& getLocalStatus();
DLL_EXPORT void setLocalStatus(dsStatus& s);
DLL_EXPORT void setFailed(int code, const char* errMsg, dsStatus::stackInfo stack);
DLL_EXPORT void setFailed(int code, const std::string& errMsg, dsStatus::stackInfo stack);

#define currentStack(errInfo) dsStatus::stackInfo(basename(__FILE__),__func__,__LINE__,errInfo)
/*use those*/


#define dsOk() return DS_OK
#define dsCheck(status) ((&(status)) == (&DS_OK))
#define dsReturnIfFailed(status) do{dsStatus& __s = (status);if(unlikely((&(__s)) != (&DS_OK))){(__s).addStack(currentStack(nullptr));return (__s);}}while(0)

#define dsReturn(status) do{dsStatus& __s = (status); if(likely((&(__s)) == (&DS_OK))){return (__s);}else{(__s).addStack(currentStack(nullptr));return (__s);}}while(0)
#define dsReturnForFailedAndLogIt(errInfo) do{String __s;__s = __s<<errInfo;LOG(logType)<<__s;getLocalStatus().addStack(currentStack(__s));return getLocalStatus();}while(0)

//#define dsReturnForFailed() do{getLocalStatus().addStack(currentStack(nullptr));return getLocalStatus();}while(0);
#define dsReturnForFailed(errInfo) do{getLocalStatus().addStack(currentStack(errInfo));return getLocalStatus();}while(0)


#define dsFailed(code,errInfo)  do{setFailed((code),(errInfo),currentStack(errInfo));return getLocalStatus();}while(0)

#define dsFailedAndLogIt(code,errInfo,logType)  do{String __s;__s = __s<<errInfo;LOG(logType)<<__s;setFailed((code),__s,currentStack(__s));return getLocalStatus();}while(0)


/*function return value is not dsStatus&,use dsFailedReturn to save error info ,and return value*/
#define dsFailedReturn(code,errInfo,returnValue) do{setFailed((code),(errInfo),currentStack(errInfo));return (returnValue);}while(0)
/*errInfo must not be null*/
#define dsFailedReturnAndLogIt(code,errInfo,logType,returnValue) do{String __s;__s = __s<<errInfo;LOG(logType)<<__s;setFailed((code),(errInfo),currentStack(errInfo));return (returnValue);}while(0)

#define dsTest(status) do{dsStatus& __s = (status);if(unlikely(!dsCheck(__s))){LOG(ERROR)<<__s.toString();abort();}}while(0);
