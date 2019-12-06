#pragma once
//#ifdef OS_WIN
#include <Mswsock.h>
#include "net.h"
#include "glog/logging.h"
#include "thread/threadPool.h"
namespace NET {
	class windowsNetServer:public netServer{
	private:
		HANDLE m_hIoCom;
		threadPool<windowsNetServer, void> m_workThread;
		int accpetedNewConnect()
		{

		}
		void workThread()
		{
			DWORD lpNumberOfBytesTransferred;
			netHandle *handle;
			OVERLAPPED *ol;
			while (m_running)
			{
				if (!GetQueuedCompletionStatus(m_hIoCom, &lpNumberOfBytesTransferred, (PULONG_PTR)&handle, (LPOVERLAPPED*)&ol, 1000))
				{
					if (handle == nullptr)
						break;
					if (ol == nullptr)
						continue;
					DWORD errorCode = GetLastError();
					if (errorCode == 0)//socket has been closed
					{
						LOG(INFO) << "connect " << handle->connId << "closed";
						handle->close();
						if (handle == &m_listenHandle)
						{
							LOG(INFO) << "listen socket closed,netServer thread exit now";
							m_running = false;
						}
						else
							delete handle;
					}
					else
					{
					}
				}
				else
				{
					if (handle->status == netHandleStatus::RECV_HEAD)
					{
						int ret = WSARecv(handle->fd, &handle->currentReadMsg->msg, 1, 4, 0, ol, nullptr);

					}
					else if (handle->status == netHandleStatus::RECV_DATA)
					{

					}
					else if (handle->status == netHandleStatus::WRITE_DATA)
					{

					}
					else if (handle->status == netHandleStatus::ACCEPT)
					{
						if (0 != accpetedNewConnect())
							break;
					}
				}
			}
		}
	public:
		windowsNetServer(const char* host, uint16_t port, const char* lockSocketFile, bool compress, int compressBufferSize, int maxWorkThreadCount);
		virtual int start(bool accept = true)
		{
			if (nullptr == (m_hIoCom = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0)))
			{
				LOG(ERROR) << "CreateIoCompletionPort failed for:" << GetLastError();
				return -1;
			}
			if (accept)
			{
				if (nullptr == CreateIoCompletionPort((HANDLE)m_listenHandle.fd, m_hIoCom, (ULONG_PTR)&m_listenHandle, 0))
				{
					LOG(ERROR) << "call CreateIoCompletionPort to put listenfd to IOCP failed for:" << GetLastError();
					PostQueuedCompletionStatus(m_hIoCom, 0, 0, nullptr);
					return -1;
				}
			}
			m_workThread.createNewThread();
			return 0;
		}

	};
}
//#endif