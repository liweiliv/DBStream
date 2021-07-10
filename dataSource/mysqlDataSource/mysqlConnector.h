#pragma once
#include <sys/stat.h>
#include <string>
#include <initializer_list>
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
#include "util/status.h"

namespace DATA_SOURCE {
	constexpr static int BINLOG_POS_INFO_SIZE = 8;
	constexpr static int BINLOG_DATA_SIZE_INFO_SIZE = 4;
	constexpr static int BINLOG_POS_OLD_INFO_SIZE = 4;
	constexpr static int BINLOG_FLAGS_INFO_SIZE = 2;
	constexpr static int BINLOG_SERVER_ID_INFO_SIZE = 4;
	constexpr static int BINLOG_NAME_SIZE_INFO_SIZE = 4;
	struct mysqlResWrap {
		MYSQL_RES* rs;
		mysqlResWrap() :rs(nullptr) {};
		mysqlResWrap(MYSQL_RES* rs) :rs(rs) {};
		~mysqlResWrap() { if (rs != nullptr)mysql_free_result(rs); }
	};

	struct mysqlStmtWrap {
		MYSQL_STMT* stmt;
		mysqlStmtWrap() :stmt(nullptr) {};
		mysqlStmtWrap(MYSQL_STMT* stmt) :stmt(stmt) {};
		~mysqlStmtWrap() { if (stmt != nullptr)mysql_stmt_close(stmt); }
	};
	

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
		Config* m_conf;
	public:
		mysqlConnector(Config* conf) :m_port(0), m_readTimeOut(0), m_connectTimeOut(0), m_conf(conf)
		{
		}
		std::string initByConf()
		{
			m_host = m_conf->get(SECTION, HOST);
			if (m_host.empty())
				return "host can not be null in config";
			m_user = m_conf->get(SECTION, USER);
			if (m_user.empty())
				return "user can not be null in config";
			m_password = m_conf->get(SECTION, PASSWORD);
			if (m_password.empty())
				return "password can not be null in config";
			if ((m_port = m_conf->getLong(SECTION, PORT, 0, 0, 65536)) == 0)
				return "port can not be null in config";
			m_sslKey = m_conf->get(SECTION, SSL_KEY);
			m_sslCert = m_conf->get(SECTION, SSL_CERT);
			m_sslCa = m_conf->get(SECTION, SSL_CA);
			m_readTimeOut = m_conf->getLong(SECTION, READ_TIMEOUT, 10, 0, 65536);
			m_connectTimeOut = m_conf->getLong(SECTION, CONNNECT_TIMEOUT, 10, 0, 65536);
			return "";
		}
		std::string updateConfig(const char* key, const char* value)
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

		static DS createMysqlResAndQuery(MYSQL* conn, mysqlResWrap &rs, const char* sql)
		{
			if (mysql_query(conn, sql) != 0)
				dsFailedAndLogIt(mysql_errno(conn), "exec sql :" << sql << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			if (nullptr == (rs.rs = mysql_store_result(conn)))
				dsFailedAndLogIt(1, "exec sql :" << sql << " failed for result is null", ERROR);
			dsOk();
		}

		static DS query(MYSQL* conn, std::function <DS(MYSQL_RES*) > func, const char* sql)
		{
			mysqlResWrap rs;
			dsReturnIfFailed(createMysqlResAndQuery(conn, rs, sql));
			dsReturn(func(rs.rs));
		}

		static DS query(MYSQL* conn, std::function <void(MYSQL_RES*) > func, const char* sql)
		{
			mysqlResWrap rs;
			dsReturnIfFailed(createMysqlResAndQuery(conn, rs, sql));
			func(rs.rs);
			dsOk();
		}

		static DS query(MYSQL* conn, std::function <DS(MYSQL_ROW) > func, const char* sql)
		{
			mysqlResWrap rs;
			dsReturnIfFailed(createMysqlResAndQuery(conn, rs, sql));
			MYSQL_ROW row;
			while (nullptr != (row = mysql_fetch_row(rs.rs)))
				dsReturnIfFailed(func(row));
			dsOk();
		}

		static DS query(MYSQL* conn, std::function <void(MYSQL_ROW) > func, const char* sql)
		{
			mysqlResWrap rs;
			dsReturnIfFailed(createMysqlResAndQuery(conn, rs, sql));
			MYSQL_ROW row;
			while (nullptr != (row = mysql_fetch_row(rs.rs)))
				func(row);
			dsOk();
		}

		static DS executeSql(MYSQL* conn, const char* sql)
		{
			if (mysql_query(conn, sql) != 0)
				dsFailedAndLogIt(mysql_errno(conn), "exec sql :" << sql << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			dsOk();
		}

		static DS createStmtAndExcute(MYSQL* conn, mysqlStmtWrap& stmt, const char* sql, std::initializer_list<const char*> &argvList)
		{
			if ((stmt.stmt = mysql_stmt_init(conn)) == nullptr)
				dsFailedAndLogIt(mysql_errno(conn), "call mysql_stmt_init failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			if (mysql_stmt_prepare(stmt.stmt, sql, strlen(sql)))
				dsFailedAndLogIt(mysql_errno(conn), "call mysql_stmt_prepare failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			int paramCount = mysql_stmt_param_count(stmt.stmt);
			if (paramCount != argvList.size())
				dsFailedAndLogIt(1, "prepare sql:" << sql << " failed for param count of sql is " << paramCount << ", but argv list size is " << argvList.size(), ERROR);
			MYSQL_BIND* bind = new MYSQL_BIND[paramCount];
			memset(bind, 0, sizeof(MYSQL_BIND) * paramCount);
			int id = 0;
			for (auto& iter : argvList)
			{
				bool isNull = iter == nullptr;
				unsigned long length = isNull ? 0 : strlen(iter);
				bind[id].buffer_type = MYSQL_TYPE_STRING;
				bind[id].buffer = (void*)iter;
				bind[id].is_null = &isNull;
				bind[id].length = &length;
				id++;
			}
			if (mysql_stmt_bind_param(stmt.stmt, bind))
			{
				delete[] bind;
				dsFailedAndLogIt(mysql_errno(conn), "call mysql_stmt_bind_param failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			}
			if (mysql_stmt_execute(stmt.stmt))
			{
				delete[] bind;
				dsFailedAndLogIt(mysql_errno(conn), "excute " << sql << " failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			}
			delete[] bind;
			dsOk();
		}


		static DS executeStmt(MYSQL* conn,const char* sql, std::initializer_list<const char*>& argvList)
		{
			mysqlStmtWrap stmt;
			dsReturn(createStmtAndExcute(conn, stmt, sql, argvList));
		}

		static DS query(MYSQL* conn, std::function <void(MYSQL_RES*) > func, const char* sql, std::initializer_list<const char *> &argvList)
		{
			mysqlStmtWrap stmt;
			dsReturnIfFailed(createStmtAndExcute(conn, stmt,sql, argvList));
			mysqlResWrap rs;
			if (nullptr == (rs.rs = mysql_store_result(conn)))
				dsFailedAndLogIt(1, "exec sql :" << sql << " failed for result is null", ERROR);
			func(rs.rs);
			dsOk();
		}

		static DS query(MYSQL* conn, std::function <DS(MYSQL_RES*) > func, const char* sql, std::initializer_list<const char*> argvList)
		{
			mysqlStmtWrap stmt;
			dsReturnIfFailed(createStmtAndExcute(conn, stmt, sql, argvList));
			mysqlResWrap rs;
			if (nullptr == (rs.rs = mysql_store_result(conn)))
				dsFailedAndLogIt(1, "exec sql :" << sql << " failed for result is null", ERROR);
			dsReturn(func(rs.rs));
		}

		static DS query(MYSQL* conn, std::function <void(MYSQL_ROW) > func, const char* sql, std::initializer_list<const char*> argvList)
		{
			mysqlStmtWrap stmt;
			dsReturnIfFailed(createStmtAndExcute(conn, stmt, sql, argvList));
			mysqlResWrap rs;
			if (nullptr == (rs.rs = mysql_store_result(conn)))
				dsFailedAndLogIt(1, "exec sql :" << sql << " failed for result is null", ERROR);
			MYSQL_ROW row;
			while (nullptr != (row = mysql_fetch_row(rs.rs)))
				func(row);
			dsOk();
		}

		static DS query(MYSQL* conn, std::function <DS (MYSQL_ROW) > func, const char* sql, std::initializer_list<const char*> argvList)
		{
			mysqlStmtWrap stmt;
			dsReturnIfFailed(createStmtAndExcute(conn, stmt, sql, argvList));
			mysqlResWrap rs;
			if (nullptr == (rs.rs = mysql_store_result(conn)))
				dsFailedAndLogIt(1, "exec sql :" << sql << " failed for result is null", ERROR);
			MYSQL_ROW row;
			while (nullptr != (row = mysql_fetch_row(rs.rs)))
				dsReturnIfFailed(func(row));
			dsOk();
		}


		DS getConnect(MYSQL*& conn)
		{
			conn = mysql_init(NULL);
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
					int errCode = mysql_errno(conn);
					std::string errInfo = mysql_error(conn);
					mysql_close(conn);
					conn = nullptr;
					dsFailedAndLogIt(errCode, "init ssl failed for " << errCode << "," << errInfo, ERROR);
				}
				else
				{
					LOG(INFO) << "init ssl success ";
				}
			}
			if (!mysql_real_connect(conn, m_host.c_str(), m_user.c_str(), m_password.c_str(), nullptr, m_port, nullptr, 0))
			{
				int errCode = mysql_errno(conn);
				std::string errInfo = mysql_error(conn);
				mysql_close(conn);
				conn = nullptr;
				dsFailedAndLogIt(errCode, "connect to " << m_host << ":" << m_port << " by user " << m_user << " failed for " << errCode << "," << errInfo, ERROR);

			}
			dsOk();
		}

		static DS getVariables(MYSQL* conn, const char* variableName, std::string& v)
		{
			dsReturn(query(conn, [&v](MYSQL_ROW row){v.assign(row[1]);}, "show variables where Variable_name=?", { v.c_str() }));
		}

		static DS startDumpBinlog(MYSQL* conn, const char* file, uint64_t pos, int32_t serverId)
		{
			executeSql(conn, "SET @master_binlog_checksum = 'NONE'");
			executeSql(conn, "SET @master_heartbeat_period = 500000000");
			size_t len = strlen(file);
			unsigned char buf[256] = {0};
			int4store(buf, pos > 4 ? pos : 4);
			int2store(buf + 4, 0);
			int4store(buf + 6, serverId);
			memcpy(buf + 10, file, len);
			if (simple_command(conn, COM_BINLOG_DUMP, buf, len + 10, 1))
				dsFailedAndLogIt(mysql_errno(conn), "execute binlog dump command failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			dsOk();
		}

		static DS startDumpBinlogByGtid(MYSQL* conn, const char* gtid, int32_t serverId)
		{
			executeSql(conn, "SET @master_binlog_checksum = 'NONE'");
			executeSql(conn, "SET @master_heartbeat_period = 500000000");
			unsigned char buf[256] = { 0 };
			unsigned char* ptr = buf;
			int2store(ptr,0);
			ptr += BINLOG_FLAGS_INFO_SIZE;
			int4store(ptr, serverId);
			ptr += BINLOG_SERVER_ID_INFO_SIZE;
			int4store(ptr, 0);
			ptr += BINLOG_NAME_SIZE_INFO_SIZE;
			int8store(ptr, 0);
			ptr += BINLOG_POS_INFO_SIZE;
			int4store(ptr, strlen(gtid));
			ptr += BINLOG_DATA_SIZE_INFO_SIZE;
			memcpy(ptr, gtid, strlen(gtid));
			ptr += strlen(gtid);
			int size = ptr - buf;
			if (simple_command(conn, COM_BINLOG_DUMP_GTID, buf, size, 1))
				dsFailedAndLogIt(mysql_errno(conn), "execute binlog gtid dump command failed for:" << mysql_errno(conn) << "," << mysql_errno(conn), ERROR);
			dsOk();
		}

#ifdef OS_LINUX
		static uint32_t genSrvierId(uint32_t seed)
		{
			uint32_t serverId = 0;
			struct ifaddrs* ifAddrStruct = NULL;
			struct ifaddrs* ifa = NULL;
			getifaddrs(&ifAddrStruct);
			for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next)
			{
				if (ifa->ifa_addr->sa_family == AF_INET)
				{
					in_addr_t addr =
						((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr;
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
				LOG(ERROR) << "auto gen server id error.";
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
				LOG(ERROR) << "WSAStartup failed for ErrorCode:" << GetLastError();
				return 0;
			}

			char host[256] = { 0 };
			::gethostname(host, 256);
			struct hostent* pHost = gethostbyname(host);
			in_addr addr;
			char* ip = nullptr;
			for (int i = 0;; i++)
			{
				char* p = pHost->h_addr_list[i];
				if (p == nullptr)
					break;
				memcpy(&addr.S_un.S_addr, p, pHost->h_length);
				if (nullptr != (ip = ::inet_ntoa(addr)) && strcmp(ip, "127.0.0.1") != 0)
					break;
			}
			::WSACleanup();
			if (ip == nullptr)
			{
				LOG(ERROR) << "auto gen server id error.";
				return 0;
			}
			srand(seed);
			uint32_t serverId = rand();
			serverId <<= 20;
			serverId |= (addr.S_un.S_addr & 0x0fffffff);
			return serverId;
		}
#endif


	};
}

