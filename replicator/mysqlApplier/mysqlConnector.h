#pragma once
#include "util/config.h"
#include "glog/logging.h"
#include "mysql.h" 
#include "mysqlx_error.h"
#include "replicator/replicatorConf.h"
namespace REPLICATOR {
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
		MYSQL* getConnect(int32_t &connErrno,const char *& connError)
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
					connErrno = mysql_errno(conn);
					connError = mysql_error(conn);
					LOG(ERROR) << "init ssl failed for " << connErrno << "," << connError;
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
				connErrno = mysql_errno(conn);
				connError = mysql_error(conn);
				LOG(ERROR) << "connect to " << m_host << ":" << m_port << " by user " << m_user << " failed for " << connErrno << "," << connError;
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
	};
}

