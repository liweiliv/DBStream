#pragma once
#include <string>
#include <stdint.h>
namespace CLIENT
{
	class client {
	public:
		enum CLIENT_STATUS {
			IDLE,
			DISCONNECTED,

		};
	private:
		CLIENT_STATUS m_status;
		std::string m_host;
		uint16_t m_port;
		std::string m_user;
		std::string m_passWord;
	public:
		CLIENT_STATUS getStatus()
		{
			return m_status;
		}
		int connect()
		{
			return -1;
		}
		const char* askTableMeta(const char* database, const char* table, uint64_t offset)
		{
			return nullptr;
		}
		const char* askTableMeta(uint64_t tableID)
		{
			return nullptr;
		}
		const char* askDatabaseMeta(const char* database, uint64_t offset)
		{
			return nullptr;
		}
		const char* askDatabaseMeta(uint64_t databaseID)
		{
			return nullptr;
		}
	};
}

