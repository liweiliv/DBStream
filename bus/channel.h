#pragma once
#include<stdint.h>
namespace BUS {
	class channel {
	public:
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs) = 0;
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs) = 0;
	};
}