#pragma once
#include<stdint.h>
namespace CLUSTER {
	class channel {
	public:
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs) = 0;
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs) = 0;
		virtual ~channel() {}
	};
}