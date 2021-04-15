#pragma once
#include "util/status.h"
namespace KVDB {
	class version;
	class walLogWriter {
	public:
		virtual DS writeTrans(const version* redoList) = 0;
	};
}