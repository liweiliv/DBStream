#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include "util/arrayQueue.h"
#include "row.h"
namespace KVDB {
	constexpr static auto UNDO_ROW_LIST_VOLUMN = 256;
	constexpr static auto MAX_UNDO_LIST_SIZE = 1024;
	class undoPurgeThread {
	public:
		struct undoRowList {
			row* rows[UNDO_ROW_LIST_VOLUMN];
			uint16_t count;
		};
	private:
		bool m_running;
		arrayQueue<undoRowList*> m_queue;
	public:
		inline void put(undoRowList* rowList)
		{
			m_queue.pushWithLock(rowList); 
		}
	private:
		dsStatus& run()
		{
			while (m_running)
			{
				undoRowList* rows = nullptr;
				if (!m_queue.popWithCond(rows, 1000))
					continue;

			}
		}
	};
}