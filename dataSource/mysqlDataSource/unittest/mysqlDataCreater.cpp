/*
 * mysqlDataCreater.cpp
 *
 *  Created on: 2019年12月27日
 *      Author: liwei
 */
#include "dataSource/mysqlDataSource/mysqlConnector.h"
#include <map>
#include <set>

#include <stdlib.h>
int create(MYSQL *conn) {
	mysql_query(conn,
			"create table if not exists test.test11(a int primary key,b varchar(20),c timestamp,d int ,e varchar(20))");
	mysql_query(conn, "select max(a) from test.test11");
	MYSQL_RES *res;
	MYSQL_ROW row;
	int start = 0;
	if (NULL != ((res) = mysql_store_result(conn))) {
		if (NULL != (row = mysql_fetch_row(res))&&row[0]!=nullptr) {
			start = atoi(row[0]);
		}
		mysql_free_result(res);
	}

	std::map<int, int> keys;
	std::set<int> dkeys;

	int c = 0;
	for (int t = 0; t < 10000; t++) {
		int tc = random() % 100;
		if (tc < 0)
			tc = -tc;
		mysql_query(conn,"begin");
		for (int i = 0; i < tc; i++) {
			c++;
			char sql[100];
			int r = random();
			if (r < 0)
				r = -r;
			if (r % 100 >= 90) {
				int key = start + r % (1+keys.size());
				std::map<int, int>::iterator iter =keys.find(key);
				if(iter==keys.end()||iter->second<0)
					continue;
				sprintf(sql,"delete from test.test11 where a=%d",key);
				keys.find(key)->second = -1;
				dkeys.insert(key);
			} else if (r % 100 >= 60) {
				int key = start + r % (keys.size()+1);
				std::map<int, int>::iterator iter =keys.find(key);
				if(iter==keys.end()||iter->second<0)
					continue;
				sprintf(sql,"update test.test11 set d =%d,c=now(), where a=%d",c,key);
				if(!keys.insert(std::pair<int,int>(key,c)).second)
					keys.find(key)->second = c;
			} else {

				int is =0;
				if(dkeys.empty()||r%10<=8){
					if(keys.empty())
						is = start+1;
					else
						is = keys.rbegin()->first+1;
				}else{
					is = *dkeys.begin();
					dkeys.erase(is);
				}
				sprintf(sql,"insert into test.test11 values(%d,'dwafefwa',now(),%d,'dwafwadwa')",is,c);
				if(!keys.insert(std::pair<int,int>(is,c)).second)
					keys.find(is)->second = c;
			}
			mysql_query(conn,sql);
		}
		mysql_query(conn,"commit");
	}
	return 0;
}
int main()
{
	Config conf(nullptr);
	conf.set("dataSource","host","192.168.52.131");
	conf.set("dataSource","port","3306");
	conf.set("dataSource","user","mtest");
	conf.set("dataSource","password","Mtest_123");
	DATA_SOURCE::mysqlConnector ct(&conf);
	ct.initByConf();
	MYSQL * conn  = ct.getConnect();
	create(conn);
	mysql_close(conn);
}

