#pragma once
#include <string>
#include <vector>
#include "likely.h"
#include "file.h"
#include "String.h"
#include "winDll.h"

typedef int32_t DS;
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
	DS code;
	std::string errMessage;
	std::vector<stackInfo> stacks;
	dsStatus* cause;
	DLL_EXPORT dsStatus() :code(0), cause(nullptr) {}
	DLL_EXPORT dsStatus(int code) :code(code), cause(nullptr) {}
	DLL_EXPORT dsStatus(int code, const std::string& errMessage) :code(code), errMessage(errMessage), cause(nullptr) {}
	DLL_EXPORT dsStatus(int code, const char* errMessage) :code(code), errMessage(errMessage), cause(nullptr) {}
	DLL_EXPORT dsStatus(const dsStatus& status) :code(status.code), errMessage(status.errMessage), stacks(status.stacks), cause(nullptr)
	{
		if (status.cause != nullptr)
			cause = new dsStatus(*status.cause);
	}
	DLL_EXPORT virtual ~dsStatus()
	{
		if (cause != nullptr)
			delete cause;
	}
	DLL_EXPORT void clear()
	{
		code = 0;
		errMessage.clear();
		stacks.clear();
		if (cause != nullptr)
		{
			delete cause;
			cause = nullptr;
		}
	}
	void setCause(const dsStatus* s)
	{
		if (s == nullptr)
			return;
		if (cause != nullptr)
		{
			delete cause;
		}
		cause = new dsStatus(*s);
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
		if (cause != nullptr)
		{
			s.append("caused by \n").append(cause->toString());
		}
		return s;
	}
	inline DS getCode()
	{
		return code;
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

#define dsOk() return 0
#define dsCheck(status) (likely((status) >= 0))
#define dsReturnIfFailed(status)  do{if(!(dsCheck(status))){dsStatus& __s = getLocalStatus();__s.addStack(currentStack(nullptr));return  __s.getCode();}}while(0)

#define dsReturnIfNotOk(status)  do{DS _s = (status);if(_s == 0){break;} else if(_s > 0){return _s;}else{dsStatus& __s = getLocalStatus();__s.addStack(currentStack(nullptr));return _s;}}while(0)


#define dsReturnIfFailedWithOp(status,op)  do{if(!(dsCheck(status))){op;dsStatus& __s = getLocalStatus();__s.addStack(currentStack(nullptr));return  __s.getCode();}}while(0)

//define dsReturnIfFailed(status) do{dsStatus& __s = (status);if(unlikely((&(__s)) != (&DS_OK))){(__s).addStack(currentStack(nullptr));return (__s);}}while(0)

#define dsReturnCode(code) return (code)

#define dsReturn(status) do{DS __s = (status); if(dsCheck(__s)){return __s;}else{dsStatus& __st = getLocalStatus();__st.addStack(currentStack(nullptr));return  __s;}}while(0)

#define dsReturnWithOp(status,op) do{DS __s = (status); if(dsCheck(__s)){op;return __s;}else{op;dsStatus& __st = getLocalStatus();__st.addStack(currentStack(nullptr));return  __s;}}while(0)


#define dsFailedAndReturn() do{dsStatus& __st = getLocalStatus();assert(&__st != &DS_OK); __st.addStack(currentStack(nullptr));return __st.getCode(); }while(0)


//#define dsReturn(status) do{dsStatus& __s = (status); if(likely((&(__s)) == (&DS_OK))){return (__s);}else{(__s).addStack(currentStack(nullptr));return (__s);}}while(0)


#define dsFailed(code,errInfo)  do{String __s;__s = __s<<errInfo;setFailed((code),(__s),currentStack(__s));return getLocalStatus().getCode();}while(0)

#define dsFailedWithCause(code,errInfo, cause)  do{String __s;__s = __s<<errInfo;setFailed((code),(__s),currentStack(__s));getLocalStatus().setCause(cause);return getLocalStatus().getCode();}while(0)


#define dsFailedAndLogIt(code,errInfo,logType)  do{String __s;__s = __s<<errInfo;LOG(logType)<<__s;setFailed((code),__s,currentStack(__s));return getLocalStatus().getCode();}while(0)

#define dsFailedAndLogItWithCause(code,errInfo,logType,cause)  do{String __s;__s = __s<<errInfo;LOG(logType)<<__s;setFailed((code),__s,currentStack(__s));getLocalStatus().setCause(cause);return getLocalStatus().getCode();}while(0)


#define resetStatus() do{if(&getLocalStatus() != &DS_OK ){getLocalStatus().clear();setLocalStatus(DS_OK);}}while(0);

#define dsCheckButIgnore(status) do{if(unlikely(!dsCheck(status))){LOG(WARNING)<<getLocalStatus().toString(); resetStatus();}}while(0)

#define dsTest(status) do{if(unlikely(!dsCheck(status))){LOG(ERROR)<<getLocalStatus().toString();abort();}}while(0);


