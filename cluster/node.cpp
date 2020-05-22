#include "processor.h"
#include <event2/event.h> 
#include <glog/logging.h>
namespace CLUSTER
{
	int node::clean()
	{
		if (m_listenEvent != nullptr)
		{
			::event_free(m_listenEvent);
			m_listenEvent = nullptr;
		}
		if (m_listenFd >= 0)
		{
			closesocket(m_listenFd);
			m_listenFd = -1;
		}
		if (m_baseEvent != nullptr)
		{
			::event_base_free(m_baseEvent);
			m_baseEvent = nullptr;
		}
	}


	int node::initNet()
	{
		clean();
		if (nullptr == (m_baseEvent = ::event_base_new()))
		{
			LOG(ERROR) << "node create base event failed";
			return -1;
		}
		const char* method = event_base_get_method(m_baseEvent);
		if (method != nullptr)
			LOG(INFO) << "libevent work on:" << method;


		if ((m_listenFd = NET::netHandle::createListenSocket(m_nodeInfo.m_ip, m_nodeInfo.port)) < 0)
		{
			LOG(ERROR) << "create listen socket failed";
			goto FAILED;
		}

		evutil_make_listen_socket_reuseable(m_listenFd);
		evutil_make_socket_nonblocking(m_listenFd);
		if (0 != ::listen(m_listenFd, 5))
		{
			LOG(INFO) << "listen on " << m_nodeInfo.m_ip << ":" << m_nodeInfo.port << " failed for:" << errno << "," << strerror(errno);
			goto FAILED;
		}
		if ((m_listenEvent = ::event_new(m_baseEvent, m_listenFd, EV_TIMEOUT | EV_READ | EV_PERSIST, listenFdCallBack, this)) == nullptr)
		{
			LOG(ERROR) << "create listen event failed";
			goto FAILED;
		}
		struct timeval tm = { 1, 0 };
		if (0 != event_add(m_listenEvent, &tm))
		{
			LOG(ERROR) << "add listenEvent to scheduler failed";
			goto FAILED;
		}
		return 0;
	FAILED:
		clean();
		return -1;
	}
	int node::accept()//todo
	{
		SOCKET clientFd;
		struct sockaddr_in clientAddr;
		int size = sizeof(struct sockaddr_in);
		if ((clientFd = ::accept(m_listenFd, (struct sockaddr*) & clientAddr, &size)) < 0)
		{
			
		}
		return 0;
	}
}