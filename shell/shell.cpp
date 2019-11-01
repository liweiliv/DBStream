#include "store/store.h"
#include "sqlParser/sqlParser.h"
#include "thread/threadPool.h"
#include "util/ringFixedQueue.h"
#include "userHandle.h"
namespace SHELL {
	class shell {
	private:
		SQL_PARSER::sqlParser* m_sql;
		STORE::store* m_store;
		threadPool<shell, void> m_threadPool;
		ringFixedQueue<userHandle*> m_preProcessList;
		bool m_running;
		void run()
		{
			userHandle* handle = nullptr;
			uint32_t idleRound = 0;
			while (likely(m_running))
			{
				if (!m_preProcessList.pop(handle, 1000))
				{
					if (++idleRound > 100 && m_threadPool.quitIfThreadMoreThan(1))
						return;
					else
						continue;
				}
				if (idleRound > 2)
					idleRound -= 2;
				else if(m_preProcessList.size() > m_threadPool.getCurrentThreadNumber() * 10)
					m_threadPool.createNewThread();

			}
		}
	};
}
