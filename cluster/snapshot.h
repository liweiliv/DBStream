#pragma once
#include <string>
#include "cluster.h"
#include "util/file.h"
namespace CLUSTER
{
	class snapshot
	{
	private:
		std::string name;
		fileHandle fd;
	public:
		void save(const cluster* c)
		{

		}
		void load(cluster* c)
		{

		}
	};
}