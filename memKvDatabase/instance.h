#pragma once
#include "clientHandel.h"
#include "thread/shared_mutex.h"
namespace AUTH{
	class server;
}
namespace KVDB {
	class database;
	class instance
	{
	private:
		std::shared_mutex m_lock;
		bool m_needLogin;
		AUTH::server* m_authServr;
	public:

	};
}