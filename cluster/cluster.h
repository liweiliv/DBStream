#pragma once
#include "memory/bufferPool.h"
#include "util/sparsepp/spp.h"
#include "util/status.h"
#include "thread/shared_mutex.h"
#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
namespace DATABASE {
	class database;
}
namespace META {
	struct tableMeta;
}
namespace CLUSTER
{
	struct nodeInfo;
	class node;
	class processor;
	class clusterLog;
	struct logEntryRpcBase;
	typedef spp::sparse_hash_map<int32_t, nodeInfo*> nodeTree;
	typedef spp::sparse_hash_map<std::string,uint32_t> nodeList;

	typedef boost::asio::io_context io_context;
	typedef boost::asio::ip::tcp::acceptor acceptor;
	typedef boost::asio::ip::tcp::socket asocket;
	enum class authReturnCode {
		sucess,
		illegalPackage,
		crcCheckFailed,
		unkownNode,
		clusterIdNotMatch,
		nodeConnectExist,
	};
	class cluster
	{
	private:
		io_context *m_netService;
		acceptor *m_acceptor;
		node* m_myself;
		nodeInfo* m_leader;
		nodeTree m_nodes;
		nodeList m_allRegistedNodes;
		uint32_t m_clusterId;
		shared_mutex m_lock;
		bufferPool* m_pool;
		clusterLog* m_log;
	private:
		int apply(const logEntryRpcBase* rpc);
		int addNode(nodeInfo *node);
		int deleteNode(int32_t id);
		int addProcessor(processor * p);
		int deleteProcessor(int32_t nodeId,int32_t id);
		void formatAuthRspMsg(char* msg, uint32_t migicNum, authReturnCode code);
		void auth(asocket* socket);
		void recvMsgFromNode(nodeInfo* ni);
		void sendMsgToNode(nodeInfo* ni, const char* msg);
		void sendLogEntryToFollower(nodeInfo* ni)
		{

		}
	public:
		int init();
		dsStatus& processMessage(const char* msg);
	};
}
