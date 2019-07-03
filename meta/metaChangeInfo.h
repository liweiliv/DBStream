/*
 * metaChangeInfo.h
 *
 *  Created on: 2018年11月20日
 *      Author: liwei
 */

#ifndef METACHANGEINFO_H_
#define METACHANGEINFO_H_
#include <string>
#include <list>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "mysqlTypes.h"
#include "charset.h"
using namespace std;
namespace META
{
struct Table
{
    string database;
    string table;
    Table(){}
    Table(const Table&t):database(t.database),table(t.table){}
    Table(const string &db,const string &tab):database(db),table(tab){}
    Table &operator =(const Table &t)
    {database = t.database;
    table = t.table;
    return *this;}
};
class newColumnInfo
{
public:
    enum colummDDLType{
        COLUMN_IN_CREATE_TABLE,
        COLUMN_IN_ADD_COLUMN,
        COLUMN_IN_MODIFY_COLUMN,
        COLUMN_IN_CHANGE_COLUMN
    };
    string name;
    uint8_t type;
	uint8_t rawType;
    bool after;
    int index;
    string afterColumnName;
    const charsetInfo * charset;
    uint32_t size;
    uint32_t precision;
    uint32_t decimals;
    list<string> setAndEnumValueList;
    bool isSigned;
    bool isPrimary;
    bool isUnique;
    bool generated;
    newColumnInfo():type(0), rawType(0),after(false),index(-1),charset(nullptr),size(0),precision(0),
            decimals(0),isSigned(true),isPrimary(false),isUnique(false),generated(false)
    {

    }
    void print()
    {
        printf("new column \nname [%s]:\n",name.c_str());
        printf("type %u\n",type);
        if(columnInfos[type].stringType)
            printf("charset:%s\n",charset?charset->name:"NO CHARSET");
        if(size>0)
            printf("size:%u\n",size);
        if(!isSigned)
            printf("unsigned\n");
        if(isPrimary)
            printf("isPrimary\n");
        if(isUnique)
            printf("isUnique\n");
    }
};
class newKeyInfo
{
public:
    enum KeyType{
        PRIMARY_KEY,
        UNIQUE_KEY,
        FOREIGN_KEY,
        KEY,
        MAX_KEY_TYPE
    };
    string name;
    KeyType type;
    list<string> columns;
    string refrenceDatabase;
    string refrenceTable;
    newKeyInfo():type(MAX_KEY_TYPE)
    {

    }
    void print()
    {
        switch(type)
        {
        case PRIMARY_KEY:
            printf("PRIMARY_KEY\n");
            break;
        case UNIQUE_KEY:
            printf("UNIQUE_KEY :%s\n",name.c_str());
            break;
        case FOREIGN_KEY:
            printf("FOREIGN_KEY :%s\n",name.c_str());
            break;
        case KEY:
            printf("KEY :%s\n",name.c_str());
            break;
        default:
            break;
        }
        for(list<string>::iterator iter = columns.begin();iter!=columns.end();iter++)
        {
            printf("%s ",(*iter).c_str());
        }
        printf("\n");
    }
};
class newTableInfo
{
public:
    enum tableChangeType{
        CREATE_TABLE,
        ALTER_TABLE,
        MAX_TABLE_TYPE
    };
    tableChangeType type;
    Table table;
    bool createLike;
    Table likedTable;
    const charsetInfo * defaultCharset;
    list<newColumnInfo*> newColumns;
    list<string> oldColumns;
    list<newKeyInfo*> newKeys;
    list<string> oldKeys;
    newTableInfo():type(MAX_TABLE_TYPE),createLike(false),defaultCharset(nullptr)
    {
    }
    ~newTableInfo()
    {
        for(list<newColumnInfo*>::iterator iter = newColumns.begin();iter!=newColumns.end();iter++)
            delete *iter;
        for(list<newKeyInfo*>::iterator iter = newKeys.begin();iter!=newKeys.end();iter++)
            delete *iter;
    }
    void print()
    {
        printf("table:%s%s%s\n",table.database.c_str(),table.database.empty()?"":".",table.table.c_str());
        if(createLike)
            printf("liked table:%s%s%s\n",table.database.c_str(),table.database.empty()?"":".",table.table.c_str());
        if(!newColumns.empty())
            printf("-----------new columns---------\n");
        for(list<newColumnInfo*>::iterator iter = newColumns.begin();iter!=newColumns.end();iter++)
            (*iter)->print();
        if(!oldColumns.empty())
            printf("-----------old columns---------\n");
        for(list<string>::iterator iter = oldColumns.begin();iter!=oldColumns.end();iter++)
            printf("%s\n",(*iter).c_str());
        if(!newKeys.empty())
            printf("-----------new keys---------\n");
        for(list<newKeyInfo*>::iterator iter = newKeys.begin();iter!=newKeys.end();iter++)
            (*iter)->print();
        if(!oldColumns.empty())
            printf("-----------old keys---------\n");
        for(list<string>::iterator iter = oldKeys.begin();iter!=oldKeys.end();iter++)
            printf("%s\n",(*iter).c_str());
        if(defaultCharset!=nullptr)
        {
            printf("defaultCharset:%s\n",defaultCharset->name);
        }

    }
};
struct databaseInfo
{
    enum databaseDDlType
    {
        CREATE_DATABASE,
        DROP_DATABASE,
        ALTER_DATABASE,
        MAX_DATABASEDDL_TYPE
    };
    databaseDDlType type;
    string name;
	const charsetInfo * charset;
    databaseInfo():type(MAX_DATABASEDDL_TYPE),charset(nullptr){}
};
struct metaChangeInfo
{
public:
    databaseInfo database;
    list<newTableInfo*> newTables;
    list<Table> oldTables;
    ~metaChangeInfo()
    {
        for(list<newTableInfo*>::iterator iter = newTables.begin();iter!=newTables.end();iter++)
            delete *iter;
    }
    void print()
    {
        for(list<newTableInfo*>::iterator iter = newTables.begin();iter!=newTables.end();iter++)
            (*iter)->print();
        for(list<Table>::iterator iter = oldTables.begin();iter!=oldTables.end();iter++)
        {
            printf("old table:%s%s%s\n",(*iter).database.c_str(),(*iter).database.empty()?"":".",(*iter).table.c_str());
        }
    }
};
}
#endif /* METACHANGEINFO_H_ */
