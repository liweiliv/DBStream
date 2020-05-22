#pragma once
#include <string>
#include <stdint.h>
#include "node.h"
namespace CLUSTER
{
	enum class ROLE {
		DATABASE_LOG_READER,
		REPLICATOR,
		MASTER_STORE,
		SLAVE_STORE
	};
	struct processorInfo {
		ROLE role;
		int32_t pid;
		int32_t nodeId;
	};
	class processor {
	public:

	protected:
		int32_t m_id;
		node* m_node;
		ROLE m_role;
	public:
		processor(int32_t id, node * n, ROLE role):m_id(id),m_node(n),m_role(role){}
		virtual ~processor() {}
	};

}