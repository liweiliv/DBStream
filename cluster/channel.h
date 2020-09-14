#pragma once
#include<stdint.h>
#include "processor.h"
namespace CLUSTER {
	class channel {
	private:
		processor* src;
		processor* dest;
	public:
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs) = 0;
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs) = 0;
		virtual ~channel() {}
	};
}