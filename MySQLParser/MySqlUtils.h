/*
 * MySqlUtil.h
 *
 *  Created on: 2017年2月22日
 *      Author: liwei
 */

#ifndef SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLUTIL_H_
#define SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLUTIL_H_
#include <stdint.h>
#include <time.h>
#include "stackLog.h"
#include <stdlib.h>
static st_mysql * connectDB(const char * host, uint16_t port,
        const char * user, const char * passwd,
        const char * ca = NULL,const char *cert = NULL,const char * key=NULL)
{
    MYSQL *conn = mysql_init(NULL);
    uint32_t timeout = 10;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &timeout);
    if(ca == NULL &&cert==NULL &&key==NULL)
    {
        int ssl_mode = SSL_MODE_DISABLED;
        mysql_options(conn, MYSQL_OPT_SSL_MODE, &ssl_mode);
    }
    else
    {
        if(mysql_ssl_set(conn,key,cert,ca,NULL,NULL)!=0)
        {
            SET_STACE_LOG_(-1, "init ssl failed for %d %s",mysql_errno(conn), mysql_error(conn));
            mysql_close(conn);
            return NULL;
        }
        else
        {
            Log_r::Notice("init ssl success");
        }
    }

    if (!mysql_real_connect(conn, host, user, passwd, NULL, port, NULL, 0))
    {
        SET_STACE_LOG_(-1, "connect to %s:%d by user %s failed for %d ,%s",host,port,user,
                mysql_errno(conn), mysql_error(conn));
        mysql_close(conn);
        return NULL;
    }
    return conn;
}
static int getVariables(MYSQL *mysqld,const char * variableName,std::string &v)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    string sql = string("show variables where Variable_name='") + string(variableName)  + string("'");
    if (0 != mysql_real_query(mysqld, sql.c_str(), sql.length()))
    {
        SET_STACE_LOG_AND_RETURN_(-1,-1,
                "show variabls :%s failed for:%d,%s,sql:%s", variableName, mysql_errno(mysqld), mysql_error(mysqld), sql.c_str());
    }
    if (mysql_field_count(mysqld) == 0)
    {
        SET_STACE_LOG_AND_RETURN_(-1,-1,
                "show variabls :%s failed for:%d,%s,sql:%s", variableName, mysql_errno(mysqld), mysql_error(mysqld), sql.c_str());
    }
    if (NULL == ((res) = mysql_store_result(mysqld)))
    {
        SET_STACE_LOG_AND_RETURN_(-1,-1,
                "show variabls :%s failed for no result:%d,%s,sql:%s", variableName, mysql_errno(mysqld), mysql_error(mysqld), sql.c_str());
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
        return false;
    }
}
#endif/*SRC_CONGO_DRC_LIB_MYSQL_BINLOG_READER_MYSQLUTIL_H_*/
