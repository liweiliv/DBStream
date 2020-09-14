#pragma once
#include "util/status.h"
namespace CLUSTER
{
	enum class status {
		ok,
		unexpectLogOffset,
	};
	struct clusterStatus :public dsStatus{
		status s;
		clusterStatus(int code, const char* errMsg, status s) :dsStatus(code, errMsg), s(s) {}
		clusterStatus(const clusterStatus cs):dsStatus(cs),s(cs.s) {}
	};
#define CL_FAILED(code,errMsg,st) 
}