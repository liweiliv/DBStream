#pragma once
#include <string>
#include "messageWrap.h"
namespace REPLICATOR
{
	class applier {
	protected:
		int m_errno;
		std::string m_error;
	public:
		applier(){}
		virtual ~applier() {}
		virtual int reconnect() = 0;
		virtual int apply(transaction* t) = 0;
		int getErrno()
		{
			return m_errno;
		}
		const std::string& getError()
		{
			return m_error;
		}
	};
}
