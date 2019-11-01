#pragma once
#include <stdint.h>
#include "schedule.h"
#include "thread/threadLocal.h"
#include "thread/runable.h"
namespace STORE {
        extern thread_local job* currentJob;
	class jobProcess {
	public:
		virtual bool process() = 0;
	};
	class job :public runable {
		friend class schedule;
	public:
		schedule * m_sc;
		uint32_t m_vtime;
		uint16_t m_nice;
		uint64_t checkpoint;
		jobProcess * m_process;

		job() :m_sc(nullptr), m_vtime(0), m_nice(0), checkpoint(0), m_process(nullptr)
		{

		}
		inline void kill()
		{

		}
		inline void action()
		{
			m_sc->putJobToRunning(this);
		}
	};
}
