#pragma once
#include "channel.h"
#include <boost/asio/ip/tcp.hpp>
namespace CLUSTER
{
	class netChannel :public channel
	{
	private:
		boost::asio::ip::tcp::socket* m_fd;
	public:
		virtual int32_t send(const char* data, uint32_t size, int outtimeMs);
		virtual int32_t recv(char* data, uint32_t size, int outtimeMs);
	};
}