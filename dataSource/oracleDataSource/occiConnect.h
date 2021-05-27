#pragma once
#include <thread>
#include <chrono>
#include <initializer_list>
#include "oracleConnectBase.h"
#include "occi.h"
namespace DATA_SOURCE
{
	class occiConnect :public oracleConnectBase {


	private:
		oracle::occi::Environment* m_env;
	public:
		DLL_EXPORT occiConnect(config* conf);
		DLL_EXPORT ~occiConnect();
	private:
		DLL_EXPORT DS connect(oracle::occi::Connection*& conn, const std::string& sid, const std::string& serviceName);
	public:
		DLL_EXPORT void closeStmt(oracle::occi::Connection* conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		DLL_EXPORT void close(oracle::occi::Connection*& conn);

		DLL_EXPORT void close(oracle::occi::Connection*& conn, oracle::occi::Statement*& stmt, oracle::occi::ResultSet*& rs);

		DLL_EXPORT DS init();

		DLL_EXPORT DS connect(oracle::occi::Connection*& conn);

		DLL_EXPORT DS connectByNodeId(int threadId, oracle::occi::Connection*& conn);

		template<typename T>
		static DLL_EXPORT DS query(occiConnect* connector, oracle::occi::Connection*& conn, int fetchSize, 
			std::function<DS(occiConnect*, oracle::occi::Connection*&)> reConnectFunc,
			std::function<DS(oracle::occi::ResultSet*)> func, const std::string& sql, std::initializer_list<T> argvList)
		{
			bool stopRetryFlag = false;
			dsReturn(query(connector, conn, stopRetryFlag, fetchSize, reConnectFunc, func, sql, argvList));
		}

		template<typename T>
		static void setParament(oracle::occi::Statement* stmt,int idx, T argv)
		{
			abort();//dot not direct use
		}
		/*
		template<>
		static void setParament(oracle::occi::Statement* stmt, int idx, int argv)
		{
			stmt->setInt(idx, argv);
		}
		template<>
		static void setParament(oracle::occi::Statement* stmt, int idx, std::string& argv)
		{
			stmt->setString(idx, argv);
		}
		*/
		template<typename T>
		static DLL_EXPORT DS query(occiConnect* connector, oracle::occi::Connection*& conn, bool& stopRetryFlag, int fetchSize, 
			std::function<DS(occiConnect*, oracle::occi::Connection*&)> reConnectFunc, std::function<DS(oracle::occi::ResultSet*)> func,
			const std::string& sql, std::initializer_list<T> argvList)
		{
			oracle::occi::Statement* stmt = nullptr;
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::SQLException exp;
			for (int i = 0; i < 10; i++)
			{
				try
				{
					stmt = conn->createStatement(sql);
					if (fetchSize > 0)
						stmt->setPrefetchRowCount(fetchSize);
					if (argvList.size() > 0)
					{
						int id = 1;
						for (auto& it : argvList)
							setParament(stmt, id, it);
					}
					if (nullptr == (rs = stmt->executeQuery()) || !rs->next())
					{
						connector->closeStmt(conn, stmt, rs);
						dsReturnCode(1);//empty result
					}

					DS s = func(rs);
					connector->closeStmt(conn, stmt, rs);
					dsReturn(s);
				}
				catch (oracle::occi::SQLException &e)
				{
					exp = e;
					if (errorRecoverable(e.getErrorCode()))
					{
						LOG(WARNING) << "excute query failed for " << e.what();
						connector->close(conn, stmt, rs);
						for (int c = 0; !stopRetryFlag && c < (i < 5 ? i : i * i); c++)
							std::this_thread::sleep_for(std::chrono::milliseconds(100));
						dsReturnIfFailed(reConnectFunc(connector, conn));
					}
					else
					{
						connector->close(conn, stmt, rs);
						dsFailedAndLogIt(1, "excute query failed for " << exp.what() << ", sql:" << sql, ERROR);
					}
				}
				if (stopRetryFlag)
					break;
			}
			dsFailedAndLogIt(1, "excute query failed for " << exp.what() << ", sql:" << sql, ERROR);
		}

		static DLL_EXPORT DS query(occiConnect* connector, oracle::occi::Connection*& conn, int fetchSize,
			std::function<DS(occiConnect*, oracle::occi::Connection*&)> reConnectFunc,
			std::function<DS(oracle::occi::ResultSet*)> func, const std::string& sql)
		{
			bool stopRetryFlag = false;
			dsReturn(query(connector, conn, stopRetryFlag, fetchSize, reConnectFunc, func, sql));
		}

		static DLL_EXPORT DS query(occiConnect* connector, oracle::occi::Connection*& conn, bool& stopRetryFlag, int fetchSize,
			std::function<DS(occiConnect*, oracle::occi::Connection*&)> reConnectFunc, std::function<DS(oracle::occi::ResultSet*)> func,
			const std::string & sql)
		{
			oracle::occi::Statement* stmt = nullptr;
			oracle::occi::ResultSet* rs = nullptr;
			oracle::occi::SQLException exp;
			for (int i = 0; i < 10; i++)
			{
				try
				{
					stmt = conn->createStatement();
					if (fetchSize > 0)
						stmt->setPrefetchRowCount(fetchSize);
					if (nullptr == (rs = stmt->executeQuery(sql)) || !rs->next())
					{
						connector->closeStmt(conn, stmt, rs);
						dsReturnCode(1);//empty result
					}

					DS s = func(rs);
					connector->closeStmt(conn, stmt, rs);
					dsReturn(s);
				}
				catch (oracle::occi::SQLException &e)
				{
					exp = e;
					if (errorRecoverable(e.getErrorCode()))
					{
						LOG(WARNING) << "excute query failed for " << e.what();
						connector->close(conn, stmt, rs);
						for (int c = 0; !stopRetryFlag && c < (i < 5 ? i : i * i); c++)
							std::this_thread::sleep_for(std::chrono::milliseconds(100));
						dsReturnIfFailed(reConnectFunc(connector, conn));
					}
					else
					{
						connector->close(conn, stmt, rs);
						dsFailedAndLogIt(1, "excute query failed for " << exp.what() << ", sql:" << sql, ERROR);
					}
				}
				if (stopRetryFlag)
					break;
			}
			dsFailedAndLogIt(1, "excute query failed for " << exp.what() << ", sql:" << sql, ERROR);
		}
	};
	/*
	template<>
	void occiConnect::setParament(oracle::occi::Statement* stmt, int idx, int argv)
	{
		stmt->setInt(idx, argv);
	}
	template<>
	void occiConnect::setParament(oracle::occi::Statement* stmt, int idx, std::string& argv)
	{
		stmt->setString(idx, argv);
	}
	*/

}
