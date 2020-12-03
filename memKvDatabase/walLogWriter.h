#pragma once
#include "util/status.h"
namespace KVDB {
	class version;
	class walLogWriter {
	public:
		virtual dsStatus& writeTrans(const version* redoList) = 0;
	};
}