#pragma once
#include <string>
#include <stdint.h>
#include <mutex>
#include <thread>
#include <event2/event.h>
#ifdef OS_WIN
#include <winsock2.h>
#pragma comment(lib,"ws2_32.lib")
#endif
#ifdef OS_LINUX
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
typedef int SOCKET;
#define closesocket ::close
#define SOCKADDR sockaddr
#endif
#include "glog/logging.h"
#include "util/sparsepp/spp.h"
#include "util/ringFixedQueue.h"
#include "util/winDll.h"
namespace NET
{
#ifdef OS_LINUX
	struct netMessage {
		uint32_t len;
		char* buf;
	};
#endif
#ifdef OS_WIN
	typedef WSABUF netMessage;
#endif
	struct messageBuffer {
		uint32_t createTime;
		uint32_t id;
		netMessage msg;
		uint32_t volumn;
	};
	enum class netHandleStatus {
		INIT,
		RECV_HEAD,
		RECV_DATA,
		WRITE_DATA,
		WRITE_STREAM_DATA,
		ACCEPT
	};
	struct netHandle {
		int connId;
		SOCKET fd;
		std::string host;
		std::string lockSocketFile;
		uint16_t port;
		int64_t flowLimit;
		int64_t rpsLimit;
		ringFixedQueue<messageBuffer> msg;
		messageBuffer* currentReadMsg;
		messageBuffer* currentWriteMsg;
		netHandleStatus status;
#ifdef OS_WIN
		OVERLAPPED winOverLapped;
#endif
		void* userDate;
		inline bool writeMessage(const char* message, uint32_t size)
		{
			if (currentWriteMsg->volumn - currentWriteMsg->msg.len < size)
			{
				if (msg.size() >= 4)
				{
					return false;
				}
				messageBuffer* tmp = new  messageBuffer;
				tmp->createTime = clock();
			}
		}
		int close()
		{
			return 0;
		}
		netHandle(const char* host, uint16_t port, const char* lockSocketFile) :connId(0), fd(-1), host(host), lockSocketFile(lockSocketFile == nullptr ? "" : lockSocketFile), 
			port(port), flowLimit(0), rpsLimit(0), currentReadMsg(nullptr),currentWriteMsg(nullptr), status(netHandleStatus::INIT), userDate(nullptr)
		{

		}
		int listen()
		{
			if (0 > (fd = createListenSocket(host, port)))
				return -1;
			return 0;
		}
		DLL_EXPORT static SOCKET createListenSocket(const std::string& host, uint16_t port)
		{
#ifdef OS_WIN
			WSADATA wsaData;
			WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
#ifdef OS_LINUX
			SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#endif
#ifdef OS_WIN
			SOCKET sock = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0, 0, WSA_FLAG_OVERLAPPED);
#endif
			if (sock < 0)
			{
				LOG(ERROR) << "create listen socket failed for:" << errno << "," << strerror(errno);
				return -1;
			}
			sockaddr_in sockAddr;
			memset(&sockAddr, 0, sizeof(sockAddr));
			sockAddr.sin_family = PF_INET;
			sockAddr.sin_addr.s_addr = inet_addr(host.c_str());
			sockAddr.sin_port = htons(port);
			if (0 != ::bind(sock, (SOCKADDR*)&sockAddr, sizeof(SOCKADDR)))
			{
				LOG(ERROR) << "bind listen socket to " << host << ":" << port << " failed for:" << errno << "," << strerror(errno);
				closesocket(sock);
				return -1;
			}
			evutil_make_listen_socket_reuseable(sock);
			evutil_make_socket_nonblocking(sock);
			if (0 != ::listen(sock, 5))
			{
				LOG(INFO) << "listen on " << host << ":" << port << " failed for:" << errno << "," << strerror(errno);
				closesocket(sock);
				return -1;
			}
			LOG(INFO) << "create linsten socket:" << sock << " on:" << host << ":" << port << " success";
			return sock;
		}
	};
	class netServer {
	protected:
		netHandle m_listenHandle;
		spp::sparse_hash_map<SOCKET, netHandle*> m_allConnects;
		std::mutex m_lock;
		bool m_compress;
		int m_compressBufferSize;
		uint32_t m_compressWaitTime;
		int m_maxWorkThreadCount;
		bool m_running;
	public:
		netServer(const char* host, uint16_t port, const char* lockSocketFile, bool compress, int compressBufferSize, int maxWorkThreadCount);
		int addNetHandle(netHandle* handle)
		{
			std::lock_guard<std::mutex> guard(m_lock);
			m_allConnects.insert(std::pair<SOCKET, netHandle*>(handle->fd, handle));
			return 0;
		}
		virtual int start(bool accept = true) = 0;

	};
}
