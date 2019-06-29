#include "C:\\Program Files\\MySQL\\MySQL Server 8.0\\include\mysql.h" //todo 
#include <sys/stat.h>
#ifdef OS_LINUX
#include <sys/socket.h>
#include <sys/un.h>
#include <ifaddrs.h>
#include "sql_common.h"
#endif
#ifdef OS_WIN
#include <winsock2.h>
#include <ws2tcpip.h>
#include "mysql/sql_common.h"
#endif
#include "C:\\Program Files\\MySQL\\MySQL Server 8.0\\include\\\mysqlx_error.h"
#include "..//..//glog/logging.h"
namespace DATA_SOURCE {
	class mysqlConnector {
	private:
		static constexpr auto HOST = "host";
		static constexpr auto PORT = "port";
		static constexpr auto USER = "user";
		static constexpr auto PASSWD = "password";
		std::string m_host;
		std::string m_user;
		std::string m_password;
		std::string m_sslKey;
		std::string m_sslCert;
		std::string m_sslCa;
		uint16_t m_port;
	public:
		MYSQL* getConnect()
		{
			MYSQL* conn = mysql_init(NULL);
			uint32_t timeout = 10;
			mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
			mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
			if (m_sslKey.empty() && m_sslCert.empty() && m_sslCa.empty())
			{
				int ssl_mode = SSL_MODE_DISABLED;
				mysql_options(conn, MYSQL_OPT_SSL_MODE, &ssl_mode);
			}
			else
			{
				if (mysql_ssl_set(conn, m_sslKey.empty() ? nullptr : m_sslKey.c_str(), m_sslCert.empty() ? nullptr : m_sslCert.c_str(), m_sslCa.empty() ? nullptr : m_sslCa.c_str(), NULL, NULL) != 0)
				{
					LOG(ERROR) << "init ssl failed for " << mysql_errno(conn) << "," << mysql_error(conn);
					mysql_close(conn);
					return nullptr;
				}
				else
				{
					LOG(INFO) << "init ssl success ";
				}
			}
			if (!mysql_real_connect(conn, m_host.c_str(), m_user.c_str(), m_password.c_str(), nullptr, m_port, nullptr, 0))
			{
				LOG(ERROR) << "connect to " << m_host << ":" << m_port << " by user " << m_user << " failed for " << mysql_errno(conn) << "," << mysql_error(conn);
				mysql_close(conn);
				return nullptr;
			}
			return conn;
		}
		bool getVariables(MYSQL* conn, const char* variableName, std::string& v)
		{
			MYSQL_RES* res;
			MYSQL_ROW row;
			std::string sql = std::string("show variables where Variable_name='")
				+ std::string(variableName) + std::string("'");
			if (0 != mysql_real_query(conn, sql.c_str(), sql.length()))
			{
				LOG(ERROR) << "show variabls :" << variableName << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn) << ",sql:" << sql.c_str();
				return false;
			}
			if (mysql_field_count(conn) == 0)
			{
				LOG(ERROR) << "show variabls :" << variableName << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn) << ",sql:" << sql.c_str();
				return false;
			}
			if (NULL == ((res) = mysql_store_result(conn)))
			{
				LOG(ERROR) << "show variabls :" << variableName << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn) << ",sql:" << sql.c_str();
				return false;
			}
			if (NULL != (row = mysql_fetch_row(res)))
			{
				v.assign(row[1]);
				mysql_free_result(res);
				return true;
			}
			else
			{
				mysql_free_result(res);
				LOG(ERROR) << "show variabls :" << variableName << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn) << ",sql:" << sql.c_str();
				return false;
			}
		}
		static int startDumpBinlog(MYSQL* conn, const char* file, uint64_t pos,int32_t serverId)
		{
			const char* cmd = "set @master_binlog_checksum = 'NONE'";
			if (mysql_query(conn, cmd) != 0)
			{
				LOG(ERROR) << "exec sql :" << cmd << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn);
				return -1;
			}
			cmd = "SET @master_heartbeat_period = 500000000";
			if (mysql_query(conn, cmd) != 0)
			{
				LOG(ERROR) << "exec sql :" << cmd << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn);
				return -1;
			}
			size_t len = strlen(file);
			unsigned char buf[128];
			int4store(buf, pos > 4 ? pos : 4);
			int2store(buf + 4, 0);
			int4store(buf + 6, serverId);
			memcpy(buf + 10, file, len);
			if (simple_command(conn, COM_BINLOG_DUMP, buf, len + 10, 1))
			{
				LOG(ERROR) << "exec sql :" << cmd << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn);
				return -1;
			}
			return 0;
		}
#ifdef OS_LINUX
		static uint32_t genSrvierId(uint32_t seed)
		{
			uint32_t serverId;
			struct ifaddrs* ifAddrStruct = NULL;
			struct ifaddrs* ifa = NULL;
			getifaddrs(&ifAddrStruct);
			for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
			{
				if (ifa->ifa_addr->sa_family == AF_INET)
				{
					in_addr_t addr =
						((struct sockaddr_in*) ifa->ifa_addr)->sin_addr.s_addr;
					if (addr != 16777343) //127.0.0.1
					{
						srand(seed);
						serverId = rand();
						serverId <<= 20;
						// linux litte endian n.n+1.n+2.n+3 [n+3 n+2 n+1 n] in int32_t
						serverId |= (addr & 0x0fffffff);
						break;
					}
				}
			}
			if (ifAddrStruct != NULL)
				freeifaddrs(ifAddrStruct);
			if (0 == serverId)
				LOG(ERROR) << ("auto gen server id error.");
			return serverId;
		}
#endif
#ifdef OS_WIN
		static uint32_t genSrvierId(uint32_t seed)
		{

			WSADATA wsaData;
			WORD sockVersion = MAKEWORD(2, 2);
			if (::WSAStartup(sockVersion, &wsaData) != 0)
			{
				LOG(ERROR) << "WSAStartup failed for ErrorCode:"<<GetLastError();
				return 0;
			}

			char host[256] = { 0 };
			::gethostname(host, 256);
			struct hostent *pHost = gethostbyname(host);
			in_addr addr;
			char* ip = nullptr;
			for (int i = 0;; i++)
			{
				char* p = pHost->h_addr_list[i];
				if (p == nullptr)
					break;
				memcpy(&addr.S_un.S_addr, p, pHost->h_length);
				if (nullptr != (ip = ::inet_ntoa(addr))&&strcmp(ip,"127.0.0.1")!=0)
					break;
			}
			::WSACleanup();
			if (ip == nullptr) 
			{
				LOG(ERROR) << ("auto gen server id error.");
				return 0;
			}
			srand(seed);
			uint32_t serverId = rand();
			serverId <<= 20;
			serverId |= (addr.S_un.S_addr&0x0fffffff);
			return serverId;
	}
#endif
	};
}

