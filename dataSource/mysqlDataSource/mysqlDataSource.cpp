#include "mysqlDataSource.h"
namespace DATA_SOURCE {
	DATABASE_INCREASE::record* mysqlDataSource::read()
	{
		DATABASE_INCREASE::record* record;
		if (m_async)
		{
			do {
				if (m_outputQueue.pop(record, 10))
					return record;
			} while (m_running);
			return nullptr;
		}
		else
		{

		}
	}
}
