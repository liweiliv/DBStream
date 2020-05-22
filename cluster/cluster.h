#pragma once
#include "util/sparsepp/spp.h"
namespace CLUSTER
{
	struct nodeInfo;
	typedef spp::sparse_hash_map<int32_t, nodeInfo*> nodeTree;
	class cluster
	{
	private:
		node* m_myself;
		nodeTree m_nodes;
		int addNode();
		int deleteNode();
		int addProcessor();
		int deleteProcessor();
	};
}