#pragma once
#include <string>
#include <stdint.h>
#include "node.h"
namespace CLUSTER
{
	enum class ROLE {
		DATABASE_LOG_READER,
		REPLICATOR,
		MASTER_INSTANCE,
		SLAVE_INSTANCE
	};
	struct processorInfo {
		ROLE role;
		int32_t pid;
		int32_t nodeId;
	};
	class cluster;
	class processor {
	public:
		friend class cluster;
	private:
		void setNode(nodeInfo* node)
		{
			this->m_node = node;
		}
	protected:
		int32_t m_id;
		int32_t m_nodeId;
		nodeInfo* m_node;
		ROLE m_role;
	public:
		processor(int32_t id, nodeInfo * n, ROLE role):m_id(id),m_node(n),m_role(role){}
		inline int32_t getId()
		{
			return m_id;
		}
		inline int32_t getNodeId()
		{
			return m_nodeId;
		}
		virtual ~processor() {}
	};

}