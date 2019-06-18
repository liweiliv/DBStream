#pragma once
#include <stdint.h>
#include "../dataSource.h"
#include "../../util/ringFixedQueue.h"
namespace DATA_SOURCE
{
	class mysqlDataSource :public dataSource {
	private:
		static constexpr auto NAME = "mysql";
		static constexpr auto ASYNC = "async";
		bool m_async;
		bool m_running;
		ringFixedQueue<DATABASE_INCREASE::record*> m_outputQueue;
	public:
		virtual DATABASE_INCREASE::record* read();
	};
}
