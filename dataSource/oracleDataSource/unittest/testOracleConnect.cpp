#include <glog/logging.h>
#include "dataSource/oracleDataSource/occiConnect.h"
#include "dataSource/oracleDataSource/ociConnect.h"
#include "dataSource/oracleDataSource/xstream/OracleXStreamDataSource.h"
#include "dataSource/oracleDataSource/oracleMetaDataCollection.h"
int testOcci(config& conf)
{
	DATA_SOURCE::occiConnect conn(&conf);
	oracle::occi::Connection* c;
	if (!dsCheck(conn.init()) || !dsCheck(conn.connect(c)))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	oracle::occi::Statement* stmp = c->createStatement();
	oracle::occi::ResultSet* rs = stmp->executeQuery("select current_scn from v$database");
	rs->next();
	LOG(ERROR) << rs->getString(1);
	conn.close(c);
}

int testOci(config& conf)
{
	DATA_SOURCE::ociConnect conn(&conf);
	DATA_SOURCE::ociConnect::oci* c;
	if (!dsCheck(conn.init()) || !dsCheck(conn.connect(c)))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	dsReturnIfFailed(c->openStmt());

	OCIDefine* defnp[1] = { 0 };
	int scn = 0;

	if (OCIStmtPrepare(c->stmtp, c->errp, (const text*)"select current_scn from v$database", (ub4)strlen("select current_scn from v$database"), (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT) != OCI_SUCCESS)
	{
		int32_t errcode;
		uint8_t errBuf[512] = { 0 };
		OCIErrorGet(c->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
		delete c;
		dsFailedAndLogIt(1, "call OCIStmtPrepare failed for " << errcode << "," << (char*)errBuf, ERROR);
	}

	OCIDefineByPos(c->stmtp, &defnp[0], c->errp, (ub4)1, &scn, 4, SQLT_INT, (void*)0, nullptr, (ub2*)0, OCI_DEFAULT);

	if (OCIStmtExecute(c->svcp, c->stmtp, c->errp, (ub4)1, (ub4)0, (OCISnapshot*)NULL, (OCISnapshot*)NULL, OCI_DEFAULT) != OCI_SUCCESS)
	{
		int32_t errcode;
		uint8_t errBuf[512] = { 0 };
		OCIErrorGet(c->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
		delete c;
		dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << errcode << "," << (char*)errBuf, ERROR);
	}
	sword rs;
	do
	{
		LOG(ERROR) << scn;
	} while ((rs = OCIStmtFetch(c->stmtp, c->errp, 1, OCI_FETCH_NEXT, OCI_DEFAULT)) == OCI_SUCCESS);
	if (rs != OCI_NO_DATA)
	{
		int32_t errcode;
		uint8_t errBuf[512] = { 0 };
		OCIErrorGet(c->errp, (ub4)1, (text*)NULL, &errcode, errBuf, (ub4)sizeof(errBuf), (ub4)OCI_HTYPE_ERROR);
		delete c;
		dsFailedAndLogIt(1, "call OCIStmtExecute failed for " << errcode << "," << (char*)errBuf, ERROR);
	}
	delete c;
}
int testXstream(config& conf)
{
	DATA_SOURCE::occiConnect conni(&conf);
	if (!dsCheck(conni.init()))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	DATA_SOURCE::tableList w;
	w.init("{\"C##ORATEST\":{\"hasShardingRule\":false,\"table\":{\"T1\":{}}}}");
	DATA_SOURCE::tableList b;
	DATA_SOURCE::oracleMetaDataCollection im(&conni, &w, &b);
	if (!dsCheck(im.init(nullptr)))
	{
		LOG(ERROR) << getLocalStatus().toString();
	}

	DATA_SOURCE::ociConnect conn(&conf);
	if (!dsCheck(conn.init()))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	ringBuffer buf;
	DATA_SOURCE::oracleXStreamLogReader reader(&conf,nullptr,nullptr);
	if (!dsCheck(reader.init()))
	{
		LOG(ERROR)<<getLocalStatus().toString();
	}
	if (!dsCheck(reader.start()))
	{
		LOG(ERROR) << getLocalStatus().toString();
	}
	while (reader.running())
	{
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	return 0;
}
int testInitMeta(config& conf)
{
	DATA_SOURCE::occiConnect conn(&conf);
	if (!dsCheck(conn.init()))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	DATA_SOURCE::tableList w;
	w.init("{\"C##ORATEST\":{\"hasShardingRule\":false,\"table\":{\"T1\":{}}}}");
	DATA_SOURCE::tableList b;

	DATA_SOURCE::oracleMetaDataCollection im(&conn, &w, &b);
	if (!dsCheck(im.init(nullptr)))
	{
		LOG(ERROR) << getLocalStatus().toString();
	}
	return 0;
}
int main()
{
	config conf;
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::HOST, "localhost");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::PORT, "1521");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::USER, "c##oraTest");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::PASSWORD, "oraTest123");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::SID, "orcl");
	//testOcci(conf);
	//testOci(conf);
	testXstream(conf);
	//testInitMeta(conf);
	return 0;
}
