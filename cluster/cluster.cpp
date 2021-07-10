#include <glog/logging.h>
#include "cluster.h"
#include "clusterLog.h"
#include "processor.h"
#include "node.h"
#include <functional>
#include <string>
#include "util/crcBySSE.h"
#include "util/likely.h"
#include "message/record.h"
#include "meta/metaDataCollection.h"
#include "meta/metaData.h"
#include "snapshot.h"
namespace CLUSTER
{
	constexpr static const char* authRspMsgs[] = {
		"auth sucess",//sucess
		"server get an illegal auth package",//illegalPackage
		"auth package crc check failed",//crcCheckFailed
		"node is unkonwn",//unkownNode
		"cluster id not match",//clusterIdNotMatch
		"connetion from this node exist,can not connect before close it"//nodeConnectExist
	};

#define raftRpcBaseColumn "recordType tinyint," \
	"version int,"\
	"clusterId int unsigned,"\
	"term bigint unsigned,"\
	"logIndex bigint unsigned,"\
	"prevRecordTerm bigint unsigned,"\
	"prevRecordLogIndex bigint unsigned,"\
	"leaderCommitIndex bigint unsigned,"

#define  clusterDataBase "cluster"

	const static auto raftRpcNodeTableSql = "create table "
		clusterDataBase ".node"
		"{"
		raftRpcBaseColumn
		"nodeId int unsigned primary key,"
		"role tinyint unsigned,"
		"port short unsigned,"
		"host varchar(256)"
		"}";
	const static auto raftRpcProcessorTableSql = "create table "
		clusterDataBase ".processor"
		"{"
		raftRpcBaseColumn
		"processorId int unsigned primary key,"
		"nodeId int unsigned,"
		"role tinyint unsigned,"
		"config text"
		"}";

	const static auto raftRpcEndpointTableSql = "create table "
		clusterDataBase ".endPoint"
		"{"
		raftRpcBaseColumn
		"endPointId int unsigned primary key,"
		"endPointType tinyint unsigned,"
		"endPointInfo text"
		"}";

	const static auto raftRpcRollbackTableSql = "create table "
		clusterDataBase ".rollback"
		"{"
		"version int,"\
		"clusterId int unsigned,"\
		"term bigint unsigned,"\
		"logIndex bigint unsigned,"\
		"}";
	constexpr static auto nodeTableId = META::TableMeta::genTableId(1, 1);
	constexpr static auto processorTableId = META::TableMeta::genTableId(2, 1);
	constexpr static auto endPointTableId = META::TableMeta::genTableId(3, 1);
	constexpr static auto heartbeatTableId = META::TableMeta::genTableId(4, 1);
	constexpr static auto checkpointTableId = META::TableMeta::genTableId(5, 1);
	constexpr static auto rollbackTableId = META::TableMeta::genTableId(6, 1);

	void cluster::formatAuthRspMsg(char* msg, uint32_t migicNum, authReturnCode code) {
		authRsp* rsp = (authRsp*)msg;
		rsp->migicNum = ~migicNum;
		rsp->nodeId = m_myself->getNodeInfo().m_id;
		rsp->success = static_cast<int8_t>(code);
		strcpy(rsp->msg, authRspMsgs[static_cast<int>(code)]);
		rsp->size = sizeof(authRsp) - offsetof(authRsp, crc) + strlen(rsp->msg);
		rsp->crc = hwCrc32c(0, ((char*)&rsp->crc) + sizeof(rsp->crc), rsp->size - offsetof(authRsp, crc) - sizeof(rsp->crc));
	}
	void cluster::auth(asocket* socket)
	{
		nodeInfo* ni = new nodeInfo(socket->remote_endpoint().address().to_string(), 0, -1);
		ni->m_fd = socket;
		boost::asio::spawn(*m_netService,
			[this, ni](boost::asio::yield_context yield)
			{
				try
				{
					char data[256], portStr[6] = { 0 };
					std::size_t n = ni->m_fd->async_read_some(boost::asio::buffer(data, 4), yield);
					int size = *(int*)&data[0];
					int migicNum = -1;
					authReturnCode code = authReturnCode::sucess;
					authReq* req = (authReq*)&data[0];
					nodeList::const_iterator nlIter;
					if (size > 256 || size < 0)
					{
						code = authReturnCode::illegalPackage;
						goto FAILED;
					}
					n = ni->m_fd->async_read_some(boost::asio::buffer((&data[0]) + 4, size), yield);
					if (n != (size_t)size)
					{
						code = authReturnCode::illegalPackage;
						goto FAILED;
					}
					if (req->crc != hwCrc32c(0, ((const char*)&req->crc) + sizeof(req->crc), req->size - offsetof(authReq, crc) - sizeof(req->crc)))
					{
						code = authReturnCode::crcCheckFailed;
						goto FAILED;
					}
					if (req->clusterId != m_clusterId)
					{
						code = authReturnCode::clusterIdNotMatch;
						goto FAILED;
					}
					ni->m_ip.assign(req->host, req->size - offsetof(authReq, host) - sizeof(req->size));
					ni->m_port = req->port;
					sprintf(portStr, "%u", req->port);
					m_lock.lock_shared();
					if ((nlIter = m_allRegistedNodes.find(ni->m_ip + ":" + portStr)) == m_allRegistedNodes.end())
					{
						m_lock.unlock_shared();
						code = authReturnCode::unkownNode;
						goto FAILED;
					}
					if (m_nodes.find(nlIter->second) != m_nodes.end())
					{
						m_lock.unlock_shared();
						code = authReturnCode::nodeConnectExist;
						goto FAILED;
					}
					m_lock.unlock_shared();
					ni->m_id = nlIter->second;
					m_lock.lock();
					m_nodes.insert(std::pair<uint32_t, nodeInfo*>(ni->m_id, ni));
					m_lock.unlock();
					//send response
					formatAuthRspMsg(data, migicNum, authReturnCode::sucess);
					boost::asio::async_write(*ni->m_fd, boost::asio::buffer(data, size), yield);
					//start recv message from node
					recvMsgFromNode(ni);
					return;
				FAILED:
					formatAuthRspMsg(data, migicNum, code);
					boost::asio::async_write(*ni->m_fd, boost::asio::buffer(data, 4 + *(int*)&data[0]), yield);
					LOG(ERROR) << "auth from " << ni->m_fd->remote_endpoint().address().to_string() << " failed for:" << authRspMsgs[static_cast<int>(code)];
					ni->m_fd->close();
					delete ni;
					return;
				}
				catch (std::exception& e)
				{
					LOG(ERROR) << "read auth package from " << ni->m_fd->remote_endpoint().address().to_string() << " failed for:" << e.what();
					ni->m_fd->close();
					delete ni;
				}
			});
	}
	void cluster::recvMsgFromNode(nodeInfo* ni)
	{
		boost::asio::spawn(*m_netService, [this, ni](boost::asio::yield_context yield)
			{
				uint32_t size = 0;
				try {
					size_t n = ni->m_fd->async_read_some(boost::asio::buffer((char*)&size, 4), yield);
					if (n != sizeof(size))
					{
						LOG(ERROR) << "read package from " << ni->m_fd->remote_endpoint().address().to_string() << " failed";
					}
					char defaultBuf[256], * buf = defaultBuf;
					if (unlikely(size > sizeof(defaultBuf)))
					{
						buf = (char*)m_pool->alloc(sizeof(int) + size);
					}
					ni->m_fd->async_read_some(boost::asio::buffer(buf + 4, size), yield);

				}
				catch (std::exception& e)
				{
					LOG(ERROR) << "read package from " << ni->m_fd->remote_endpoint().address().to_string() << " failed for:" << e.what();
					m_lock.lock();
					m_nodes.erase(ni->m_id);
					m_lock.unlock();
					ni->m_fd->close();
					delete ni;
				}
			});
	}
	void cluster::sendMsgToNode(nodeInfo* ni, const char* msg)
	{
		boost::asio::spawn(*m_netService, [&](boost::asio::yield_context yield)
			{
				boost::asio::async_write(*ni->m_fd, boost::asio::buffer(msg, (*(const int*)msg) + 4), yield);
			});
	}
	int cluster::init()
	{
		boost::asio::spawn(*m_netService,
			[&](boost::asio::yield_context yield)
			{
				boost::asio::ip::tcp::acceptor acceptor(*m_netService,
					boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), m_myself->getNodeInfo().m_port));
				for (;;)
				{
					boost::system::error_code ec;
					asocket* socket = new asocket(*m_netService);
					acceptor.async_accept(*socket, yield[ec]);
					if (!ec)
					{
						auth(socket);
					}
					else
					{
						//todo
						delete socket;
					}
				}
			});
		m_netService->run();
		return 0;
	}
	int cluster::apply(const logEntryRpcBase* rpc)
	{
		return 0;//todo
	}

	DS cluster::processMessage(const char* msg)
	{
		logEntryRpcBase* rpc = (logEntryRpcBase*)msg;
		if (rpc->recordType >= static_cast<uint8_t>(rpcType::endOfFile))
			dsFailedAndLogIt(errorCode::illegalLogEntry, "get illegal rpc type:" << rpc->recordType, ERROR);
		if (rpc->size < sizeof(logEntryRpcBase))
			dsFailedAndLogIt(errorCode::illegalLogEntry, "get illegal rpc ,size:" << rpc->size << " is to small", ERROR);
		switch (m_myself->getNodeInfo().m_role)
		{
		case clusterRole::LEADER:
		{
			rpc->logIndex = m_myself->incAndGetLogIndex();
			dsReturnIfFailed(m_log->append(rpc));
			break;
		}
		case clusterRole::FOLLOWER:
		{
			if (!dsCheck(m_log->append(rpc)))
			{
				if (getLocalStatus().code == errorCode::prevNotMatch)
				{
					logEntryRpcBase msg;
					msg.size = sizeof(logEntryRpcBase);
					msg.recordType = static_cast<uint8_t>(rpcType::prevNotMatch);
					msg.version = VERSION;
					msg.logIndex = m_myself->getNodeInfo().m_currentLogIndex;
					try {
						if (msg.size != m_leader->m_fd->send(boost::asio::buffer((char*)&msg, msg.size)))
							dsFailedAndLogIt(errorCode::netError, "only send part of package to leader ", ERROR);
					}
					catch (std::exception& e) {
						dsFailedAndLogIt(errorCode::netError, "send package to leader failed for:" << e.what(), ERROR);
					}
				}
				else
					dsFailedAndReturn();
			}
			if (rpc->logIndex.term != m_myself->getNodeInfo().m_currentLogIndex.term)
			{

			}
			if (rpc->leaderCommitIndex > m_myself->getNodeInfo().m_currentLogIndex.logIndex)
			{
				/*
				const logEntryRpcBase* LogEntry;
				do {
					dsReturnIfFailed(m_logIter->next(LogEntry));
					dsReturnIfFailed(apply(LogEntry));
				} while (LogEntry->logIndex.logIndex < rpc->leaderCommitIndex);
				*/
				//todo
			}
			break;
		}

		case clusterRole::CANDIDATE:
		case clusterRole::LEARNER:
			break;
		}
	}

	int cluster::addNode(nodeInfo* node)
	{
		m_lock.lock();
		if (m_nodes.contains(node->m_id))
		{
			m_lock.unlock();
			LOG(WARNING) << "add node:" << node->m_id << " failed for node exist";
			return -1;
		}
		m_nodes.insert(std::pair<int32_t, nodeInfo*>(node->m_id, node));
		m_lock.unlock();
		return 0;
	}
	int cluster::deleteNode(int32_t id)
	{
		m_lock.lock();
		if (!m_nodes.contains(id))
		{
			m_lock.unlock();
			LOG(WARNING) << "delete node:" << id << " failed for node not exist";
			return -1;
		}
		m_nodes.erase(id);
		m_lock.unlock();
		return 0;
	}
	int cluster::addProcessor(processor* p)
	{
		m_lock.lock();
		nodeTree::iterator iter = m_nodes.find(p->getNodeId());
		if (iter == m_nodes.end())
		{
			m_lock.unlock();
			LOG(WARNING) << "add processor:" << p->getNodeId() << ":" << p->getId() << " failed for node not exist";
			return -1;
		}
		if (iter->second->m_procesors.contains(p->getId()))
		{
			m_lock.unlock();
			LOG(WARNING) << "add processor:" << p->getNodeId() << ":" << p->getId() << " failed for processor exist";
			return -1;
		}
		iter->second->m_procesors.insert(std::pair<int32_t, processor*>(p->getId(), p));
		p->setNode(iter->second);
		m_lock.unlock();
		return 0;
	}
	int cluster::deleteProcessor(int32_t nodeId, int32_t id)
	{
		m_lock.lock();
		nodeTree::iterator iter = m_nodes.find(nodeId);
		if (iter == m_nodes.end())
		{
			m_lock.unlock();
			LOG(WARNING) << "delete processor:" << nodeId << ":" << id << " failed for node not exist";
			return -1;
		}
		if (!iter->second->m_procesors.contains(id))
		{
			m_lock.unlock();
			LOG(WARNING) << "delete processor:" << nodeId << ":" << id << " failed for processor not exist";
			return -1;
		}
		iter->second->m_procesors.erase(id);
		m_lock.unlock();
		return 0;
	}

}
