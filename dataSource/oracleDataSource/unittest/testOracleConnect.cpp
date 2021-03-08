#include <glog/logging.h>
#include "dataSource/oracleDataSource/oracleConnect.h"
int main()
{
	config conf;
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::HOST, "localhost");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::PORT, "1521");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::USER, "oraTest");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::PASSWORD, "oraTest123");
	conf.set(DATA_SOURCE::SECTION, DATA_SOURCE::SID, "orcl");
	DATA_SOURCE::oracleConnect conn(&conf);
	oracle::occi::Connection* c;
	if (!dsCheck(conn.init())||!dsCheck(conn.connect(c)))
	{
		LOG(ERROR) << "conn failed";
		return -1;
	}
	oracle::occi::Statement* stmp = c->createStatement();
	oracle::occi::ResultSet * rs = stmp->executeQuery("select current_scn from v$database");
	rs->next();
	LOG(ERROR) << rs->getString(1);
	conn.close(c);
	return 0;
}