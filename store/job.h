#pragma once
#include <stdint.h>
#include "schedule.h"
namespace STORE {
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
			INTERRUPTTED
		};
		schedule * m_sc;
		uint32_t m_vtime;
		uint16_t m_nice;
		uint16_t m_prevThreadID;
		jobStatus m_status;
		uint64_t checkpoint;
		jobProcess * m_process;
		job() :m_sc(nullptr), m_vtime(0), m_nice(0), m_prevThreadID(0), m_status(WAIT_NEXT)
		{

		}
		inline void wakeUp()
		{
			m_sc->wakeUp(this);
		}
	};
}
