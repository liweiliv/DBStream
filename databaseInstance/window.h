#pragma once
#include "stream.h"
namespace DB_INSTANCE {
	class window :public DBStream {
	public:
		enum windowType {
			W_TUMBLING,
			W_SLIDING,
			W_SESSION
		};
	protected:
		DBStream * m_sourceStream;
		windowType m_type;
	};
	class countWindow :public window {
	private:
		uint32_t m_count;
	};
	class timeWindow :public window {
		uint32_t m_milSeconds;
	};
}
