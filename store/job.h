#pragma once
#include <stdint.h>
#include "schedule.h"
#include "../util/threadLocal.h"
namespace STORE {
        extern thread_local job* currentJob;
	class jobProcess {
	public:
		virtual bool process() = 0;
	};
	class job {
		friend class schedule;
	public:
		enum jobStatus {
			WAIT_NEXT,
			READY_FOR_PROCESS,
			KILLED,
			FINISH,
			FAULT
		};

		job* next;
		schedule * m_sc;
		uint32_t m_vtime;
		uint16_t m_nice;
		int m_threadId;
		jobStatus m_status;
		uint64_t checkpoint;
		jobProcess * m_process;

		job() :m_sc(nullptr), m_vtime(0), m_nice(0), m_threadId(0), m_status(WAIT_NEXT)
		{

		}
		inline void sign()
		{
			currentJob = this;
			m_threadId = getThreadId();
		}
		inline void kill()
		{

		}
		inline void wakeUp()
		{
			m_sc->putJobToRunning(this);
		}
	};
}
