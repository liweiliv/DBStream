#pragma once
#include <string>
#include <stdint.h>
#include <boost/asio/ip/tcp.hpp>
#include "util/sparsepp/spp.h"
#include "util/status.h"
#include "rpc.h"
#include "clusterLog.h"
namespace CLUSTER
{
	class processor;
	class cluster;
	typedef spp::sparse_hash_map<int32_t, processor*> processTree;
	enum class clusterRole {
		LEADER,
		FOLLOWER,
		LEARNER,
		CANDIDATE
	};
	struct nodeInfo {
		std::string m_ip;
		uint16_t m_port;
		int32_t m_id;
		clusterRole m_role;
		std::string m_name;
		processTree m_procesors;
		logIndexInfo m_currentLogIndex;
		boost::asio::ip::tcp::socket * m_fd;
		nodeInfo(const std::string & ip,uint16_t port,int32_t id): m_ip(ip), m_port(port), m_id(id), m_role(clusterRole::CANDIDATE), m_fd(nullptr)
		{
		}
		~nodeInfo()
		{
			if (m_fd != nullptr)
			{
				m_fd->close();
				delete m_fd;
			}
		}
	};
	class node
	{
	private:
		int32_t m_clusterId;
		nodeInfo m_nodeInfo;
		cluster* m_cluster;
		int clean();
		DS apply(const logEntryRpcBase* rpc)
		{

		}
	public:
		DS applyTo(const logIndexInfo& logIndex)
		{

		}
		nodeInfo& getNodeInfo()
		{
			return m_nodeInfo;
		}
		const logIndexInfo& incAndGetLogIndex()
		{
			m_nodeInfo.m_currentLogIndex.logIndex++;
			return m_nodeInfo.m_currentLogIndex;
		}
	};
}