#pragma once
#include <sys/stat.h>
#include "util/config.h"
#include "dataSource/dataSourceConf.h"
#ifdef OS_LINUX
#include <sys/socket.h>
#include <sys/un.h>
#include <ifaddrs.h>
#include "mysql/sql_common.h"
#endif
#ifdef OS_WIN
#include <winsock2.h>
#include <ws2tcpip.h>
#include "mysql/sql_common.h"
#endif
#include "glog/logging.h"
#include "mysql/my_byteorder.h"
#include "mysql.h" 
#include "mysqlx_error.h"

namespace DATA_SOURCE {
	class mysqlConnector {
	private:
		static constexpr auto SSL_KEY = "sslKey";
		static constexpr auto SSL_CA = "sslCa";
		static constexpr auto SSL_CERT = "sslCert";
		std::string m_host;
		std::string m_user;
		std::string m_password;
		std::string m_sslKey;
		std::string m_sslCert;
		std::string m_sslCa;
		uint16_t m_port;
		uint32_t m_readTimeOut;
		uint32_t m_connectTimeOut;
		config* m_conf;
	public:
		mysqlConnector(config *conf):m_port(0), m_readTimeOut(0), m_connectTimeOut(0),m_conf(conf)
		{
		}
		std::string initByConf()
		{
			m_host = m_conf->get(SECTION, std::string(CONN_SECTION).append(HOST).c_str());
			if (m_host.empty())
				return "host can not be null in config";
			m_user = m_conf->get(SECTION, std::string(CONN_SECTION).append(USER).c_str());
			if (m_user.empty())
				return "user can not be null in config";
			m_password = m_conf->get(SECTION, std::string(CONN_SECTION).append(PASSWORD).c_str());
			if (m_password.empty())
				return "password can not be null in config";
			if((m_port = m_conf->getLong(SECTION, std::string(CONN_SECTION).append(PORT).c_str(),0,0,65536))==0)
				return "port can not be null in config";
			m_sslKey = m_conf->get(SECTION, std::string(CONN_SECTION).append(SSL_KEY).c_str());
			m_sslCert = m_conf->get(SECTION, std::string(CONN_SECTION).append(SSL_CERT).c_str());
			m_sslCa = m_conf->get(SECTION, std::string(CONN_SECTION).append(SSL_CA).c_str());
			m_readTimeOut = m_conf->getLong(SECTION, std::string(CONN_SECTION).append(READ_TIMEOUT).c_str(), 10, 0, 65536);
			m_connectTimeOut = m_conf->getLong(SECTION, std::string(CONN_SECTION).append(CONNNECT_TIMEOUT).c_str(), 10, 0, 65536);
			return "";
		}
		std::string updateConfig(const char * key,const char * value)
		{
			if (strcmp(key, HOST) == 0)
				m_host = value;
			else if (strcmp(key, USER) == 0)
				m_user = value;
			if (strcmp(key, PASSWORD) == 0)
				m_password = value;
			else if (strcmp(key, SSL_KEY) == 0)
				m_sslKey = value;
			else if (strcmp(key, SSL_CERT) == 0)
				m_sslCert = value;
			if (strcmp(key, SSL_CA) == 0)
				m_sslCa = value;
			else if (strcmp(key, PORT) == 0)
			{
				const char* ptr = value;
				int port = 0;
				while (*ptr != '\0')
				{
					if (*(ptr) <= '9' && *(ptr) >= '0')
						port = port * 10 + (*ptr) - '0';
					else
						return std::string("config ") + PORT + " must be number";
				}
				if (port > 0xffff)
					return std::string("config ") + PORT + " must less than 65536";
				m_port = port;
			}
			else
				return std::string("unknown config:") + key + ":" + value;
			return "";
		}
		MYSQL* getConnect()
		{
			MYSQL* conn = mysql_init(NULL);
			mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &m_connectTimeOut);
			mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &m_readTimeOut);
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
		static bool getVariables(MYSQL* conn, const char* variableName, std::string& v)
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

