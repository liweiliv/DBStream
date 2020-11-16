#pragma once
#include <stdint.h>
#include <string>
#include "auth/client.h"
namespace AUTH {
	class serverHandle;
}
namespace DATABASE_INCREASE{
	struct dmlRecord;
}
namespace KVDB {
	class transaction;
	struct rowChange;
	struct version;
	class clientHandle {
	private:
		AUTH::serverHandle* m_authHandle;
	public:
		uint32_t m_uid;
		uint64_t m_txnId;
		std::string m_currentDatabase;
		transaction * m_trans;
		rowChange* m_change;
		char m_resultDefaultBuffer[2048];
		DATABASE_INCREASE::dmlRecord* m_result;

	public:
		void copyResult(const version ** v);
	};
}