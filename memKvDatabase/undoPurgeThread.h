#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include "row.h"
namespace KVDB {
	constexpr static auto UNDO_ROW_LIST_VOLUMN = 256;
	constexpr static auto MAX_UNDO_LIST_SIZE = 1024;

	class undoPurgeThread {
	public:
		struct undoRowList {
			row* rows[UNDO_ROW_LIST_VOLUMN];
			uint16_t count;
			undoRowList* next;
		};
	private:
		undoRowList* m_queue[MAX_UNDO_LIST_SIZE] ;
		uint32_t m_head;
		uint32_t m_tail;
		std::mutex m_lock;
		std::condition_variable m_cond;
	public:

		void push(undoRowList* rowList)
		{
			m_lock.lock();

		}
	};
}