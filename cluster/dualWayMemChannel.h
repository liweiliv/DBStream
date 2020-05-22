#pragma once
#include "memChannel.h"
namespace CLUSTER
{
	class dualWayMemChannel :public channel
	{
	private:
		memChannel* in;
		memChannel* out;
	public:
		dualWayMemChannel(memChannel* in, memChannel* out) :in(in), out(out) {}
		virtual ~dualWayMemChannel() {}
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs)
		{
			return out->send(data, size, outtimeMs);
		}
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs)
		{
			return in->recv(data, size, outtimeMs);
		}
	};
}