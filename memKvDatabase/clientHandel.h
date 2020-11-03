#pragma once
#include <stdint.h>
#include "auth/client.h"
namespace AUTH {
	class serverHandle;
}
namespace KVDB {
	class transaction;
	class clientHandle {
	private:
		AUTH::serverHandle* m_authHandle;
	public:
		uint32_t m_uid;
		uint64_t m_txnId;
		transaction * m_trans;
	};
}