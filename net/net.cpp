#include "net.h"
namespace NET {
	netServer::netServer(const char* host, uint16_t port, const char* lockSocketFile, bool compress, int compressBufferSize, int maxWorkThreadCount) :m_listenHandle(host, port, lockSocketFile),
		m_compress(compress), m_compressBufferSize(compressBufferSize), m_compressWaitTime(100), m_maxWorkThreadCount(maxWorkThreadCount), m_running(false)
	{
	}
}
