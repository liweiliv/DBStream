#include "windowsNetServer.h"
namespace NET{
	windowsNetServer::windowsNetServer(const char* host, uint16_t port, const char* lockSocketFile, bool compress, int compressBufferSize, int maxWorkThreadCount) :
		netServer(host, port, lockSocketFile, compress, compressBufferSize, maxWorkThreadCount), m_hIoCom(nullptr), m_workThread(createThreadPool(m_maxWorkThreadCount, this, &windowsNetServer::workThread, "iocp"))
		{
		}
	
}