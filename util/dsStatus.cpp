#include "status.h"
DLL_EXPORT dsStatus DS_OK;
static thread_local dsStatus failed;
static thread_local dsStatus * localStatus = &DS_OK;
DLL_EXPORT dsStatus& getLocalStatus() { return *localStatus; }
DLL_EXPORT void setFailed(int code, const char* errMsg, dsStatus::stackInfo stack)
{
	failed.clear();
	failed.code = code;
	if (errMsg != nullptr)
		failed.errMessage.assign(errMsg);
	failed.addStack(stack);
	localStatus = &failed;
}
DLL_EXPORT void setFailed(int code, const std::string& errMsg, dsStatus::stackInfo stack)
{
	failed.clear();
	failed.code = code;
	failed.errMessage = errMsg;
	failed.addStack(stack);
	localStatus = &failed;
}
DLL_EXPORT void setLocalStatus(dsStatus& s) { localStatus = &s; }
