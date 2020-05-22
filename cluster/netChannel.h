#pragma once
#include "channel.h"
namespace NET {
	struct netHandle;
}
namespace CLUSTER
{
	class netChannel :public channel
	{
	private:
		NET::netHandle* m_net;
	public:
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs);
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs);
	};
}