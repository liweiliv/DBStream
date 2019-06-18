#pragma once
#include <stdint.h>
#include <string>
namespace DATA_SOURCE
{
	class mysqlBinlogReader {
	private:
		static constexpr auto HOST = "host";
		static constexpr auto PORT = "port";
		static constexpr auto USER = "user";
		static constexpr auto PASSWD = "password";
		std::string m_host;
		std::string m_user;
		std::string m_password;
		uint16_t m_port;
		uint64_t m_fileId;
		uint64_t m_logOffset;
	};
}
