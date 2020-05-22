#pragma once
#include <string>
#include <stdint.h>
#include "util/sparsepp/spp.h"
#include "net/net.h"
struct event_base;
struct event;

namespace CLUSTER
{
	class processor;
	class cluster;
	typedef spp::sparse_hash_map<int32_t, processor*> processTree;
	struct nodeInfo {
		std::string m_ip;
		uint16_t port;
		int32_t m_id;
		std::string m_name;
	};
	class node
	{
	private:
		int32_t m_clusterId;
		nodeInfo m_nodeInfo;
		struct event_base * m_baseEvent;
		struct event* m_listenEvent;
		SOCKET m_listenFd;

		cluster* m_cluster;
		processTree* m_procesors;
		int clean();
		int initNet();
		int accept();
		static void listenFdCallBack(int64_t fd, short ev, void* arg)
		{
			static_cast<node*>(arg)->accept();
		}

	};
}