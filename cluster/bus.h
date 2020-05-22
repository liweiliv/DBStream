#pragma once
#include <stdint.h>
#include <string>
#include <map>
#include "database/database.h"
#include "net/net.h"
namespace CLUSTER {
	enum class role{
		learner,
		follower,
		candidater,
		leader,
	};
	struct voteMessage {
		uint8_t type;
		uint32_t serverId;
		uint32_t nodeId;
		uint64_t term;
		uint64_t logIndex;
	};
	struct voteReqMessage {
		uint8_t type;
		uint8_t success;
		uint32_t serverId;
		uint64_t term;
	};
	enum class status {
		waitFor
	};
	struct clientInfo {
		std::string m_host;
		uint16_t m_port;
		uint32_t m_id;
		role m_role;
		uint64_t m_logIndex;
		uint64_t m_prevLogIndex;
		uint64_t m_term;
		uint64_t m_prevTerm;
		uint64_t committedSeqNum;
		SOCKET m_fd;
	};
	class client {
	private:
		DATABASE::database* m_db;
		clientInfo m_me;
		std::map<uint32_t, clientInfo*> m_clients;
		bool m_running;
		DB_NET::netServer m_net;
	public:
		int workThread()
		{
			while (m_running)
			{
				if (m_me.m_role == role::leader)
				{
					if (getMessage() > 0)
					{
						if (broadcastMessage() < 0)
						{
							voteForMe()
						}
					}
					else
					{
						if (sendHeartBeat() < 0)
						{
							voteForMe(m_me.committedSeqNum);
						}
					}
				}
			}
		}
		int voteForMe()
		{
			for (std::map<uint32_t, clientInfo*>::iterator iter = m_clients.begin(); iter != m_clients.end(); iter++)
			{
				clientInfo* client = iter->second;
				if (client->m_fd < 0)
				{

				}
			}
		}
		int recvLogFromLeader()
		{

		}
		int getMessage()
		{

		}
		int broadcastMessage()
		{

		}
		int sendHeartBeat()
		{

		}
		int ackVote(uint64_t seqNum, uint32_t id)
		{

		}

	};
	class bus {
		
	};
}