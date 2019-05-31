/*
 * record.h
 *
 *  Created on: 2018年12月12日
 *      Author: liwei
 */

#ifndef RECORD_H_
#define RECORD_H_
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "../meta/metaData.h"
#include "../meta/metaDataCollection.h"
namespace DATABASE_INCREASE
{
struct recordHead
{
    uint64_t size;
    uint16_t headSize;
	uint16_t flag;
    uint8_t type;
    uint8_t version;
    uint64_t recordId;
    uint64_t logOffset;
    uint64_t timestamp;
	uint32_t txnId;
};
#define ENDOF_REDO_NUM  0xfff8fff4fff2fff1
#define UNSIGNED_COLUMN 0x01
#define GENRATED_COLUMN 0x02
#define PRIMARY_KEY_COLUMN 0x04
#define UNIQUE_KEY_COLUMN 0x08
enum RecordType
{
	R_INSERT,
	R_UPDATE,
	R_DELETE,
	R_REPLACE,
	R_ROLLBACK,
	R_DDL,
	R_TABLE_META,
	R_DATABASE_META,
    R_HEARTBEAT,
	R_MAX_RECORD_TYPE
};
#define COLUMN_FLAG_VIRTUAL 0x01
#define COLUMN_FLAG_SIGNED 0x02

struct columnDef
{
    uint8_t type;
	uint8_t srcType;
    uint8_t flag;
    uint16_t charsetID;
    uint32_t size;
	uint32_t precision;
	uint32_t decimals;
	uint32_t nameOffset;
	uint32_t setOrEnumInfoOffset;
};
struct record
{
    const char * data;
    recordHead *head;
    record(){}
    record(const char * data){
        this->data = data;
        head = (recordHead*)data;
    }
};
#define TEST_BITMAP(m,i) (((m)[(i)>>3]>>(i&0x7))&0x1)
#define SET_BITMAP(m,i)  (m)[(i)>>3]|= (0x01<<(i&0x7))
/*
* [16 bit charsetID][database string+ 1 byte '\0']
*/
struct DatabaseMetaMessage :public record
{
	uint64_t id;
	uint16_t charsetID;
	const char * dbName;
	DatabaseMetaMessage(const char * data) :record(data) {
		id =*(uint64_t*)(data + +head->headSize);
		charsetID = *(uint16_t*)(data + +head->headSize+sizeof(uint64_t));
		dbName = data + +head->headSize +sizeof(uint64_t)+sizeof(uint16_t);
	}
};
struct TableMetaMessage :public record
{
    /*--------------version 0----------------*/
    /*[64 bit tableMetaID][16 bit charsetID][16 bit column count][16 bit pk columns count][16 bit uk count]
	 *[8 bit database size][database string+ 1 byte '\0'][8 bit table size][table string+ 1 byte '\0']
     * [columnDef 1]...[columnDef n]
     * [16 bit pk column id 1]...[16 bit pk column id n]
     * [16 bit uk 1 column count]...[16 bit uk n column count]
     *[32 bit uk 1 name offset][16 bit uk 1 column 1 id ]...[16 bit uk 1 column n id ]
     * ...
     *[32 bit uk m name offset][16 bit uk m column 1 id ]...[16 bit uk m column n id ]
	 *[32 bit column names size]
	 *[column 1 name string+'\0']...[column n name string+'\0']
	 *[32 bit unique key names size]
	 *[uk 1 name string+'\0']...[uk n name string+'\0']
	 *[32 bit enum or set value lists size]
	 *[enum or set value list 0+'\0']...[enum or set value list m+'\0']
     * */
	struct tableMetaHead {
		uint64_t tableMetaID;
		uint16_t charsetId;
		uint16_t columnCount;
		uint16_t primaryKeyColumnCount;
		uint16_t uniqueKeyCount;
	};
	tableMetaHead metaHead;
    const char * database;
    const char * table;
    const columnDef *columns;
    const uint16_t * primaryKeys;
    const uint16_t * uniqueKeyColumnCounts;
	uint32_t* uniqueKeyNameOffset;
    uint16_t ** uniqueKeys;
    /*-----------------------------------------------*/
    TableMetaMessage(const char * data):record(data){
		const char * ptr = data + +head->headSize;
		memcpy(&metaHead, ptr, sizeof(metaHead));
		ptr += sizeof(metaHead);
        database = ptr+sizeof(uint8_t);
        table = database+*(uint8_t*)(database- sizeof(uint8_t));
		ptr = table + *(uint8_t*)(table - sizeof(uint8_t));

        columns = (columnDef*)ptr;
        ptr = (const char*)&columns[metaHead.columnCount];
        if(metaHead.primaryKeyColumnCount !=0)
        {
			primaryKeys = (uint16_t*)ptr;
            ptr = (const char*)&primaryKeys[metaHead.primaryKeyColumnCount];
        }
        else
        {
            primaryKeys = NULL;
        }
        if(metaHead.uniqueKeyCount>0)
        {
            uniqueKeyColumnCounts = (uint16_t*)(ptr);
            ptr = (const char*)&uniqueKeyColumnCounts[metaHead.uniqueKeyCount];
            uniqueKeys = (uint16_t**)malloc(sizeof(uint16_t*)*metaHead.uniqueKeyCount);
			uniqueKeyNameOffset = (uint32_t*)malloc(sizeof(uint32_t)*metaHead.uniqueKeyCount);
            for(uint16_t idx =0;idx< metaHead.uniqueKeyCount;idx++)
            {
				uniqueKeyNameOffset[idx] = *(uint32_t*)ptr;
                uniqueKeys[idx] = (uint16_t*)(ptr+sizeof(uint32_t));
                ptr = (const char*)&uniqueKeys[idx][uniqueKeyColumnCounts[idx]];
            }
        }
        else
        {
			uniqueKeyNameOffset = nullptr;
            uniqueKeyColumnCounts = nullptr;
            uniqueKeys = nullptr;
        }
		ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over column names
		ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over uk names
		ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over enum and set value lists
		if (head->version == 0)
			assert(ptr = data + head->size);
        /*if version increased in future,add those code:
          if(head->version >0)
          {do some thing}
          if(head->version >1)
          {do some thing}
             ...
           */
    }
	inline const char * columnName(uint16_t columnIndex)
	{
		return data+columns[columnIndex].nameOffset;
	}
	inline bool setOrEnumValues(uint16_t columnIndex,const char *& base,uint16_t *&valueList,uint16_t &valueListSize)
	{
		if (columns[columnIndex].setOrEnumInfoOffset != 0)
		{
			base = data + columns[columnIndex].setOrEnumInfoOffset;
			valueListSize = *(uint16_t*)base;
			valueList = (uint16_t*)(base + sizeof(valueListSize));

			return true;
		}
		else
			return false;
	}
    ~TableMetaMessage()
    {
        if(uniqueKeys!=NULL)
            free(uniqueKeys);
    }
};
struct DMLRecord:public record
{
    /*--------------version 0----------------*/
    /*[64 bit tableMetaID]
	 * [column count bit null bitmap]
     * [new columns(if insert/update)| old columns(if delete)]
     * [old column bit bitmap] (if update)
     * [32 byte old column 1 size]...[32 byte old column n size] (if exist old column)
     * [old column 1+ 1byte '\0']...[old column n+ 1byte '\0']
     * */
	uint64_t tableMetaID;
	META::tableMeta * meta;
	const uint8_t  * nullBitmap;
    const char * columns;
	const uint32_t * varLengthColumns;
	uint32_t oldColumnsSizeOfUpdateType;
	const char * oldColumnsOfUpdateType;
    uint8_t *updatedBitmap;
    DMLRecord(){}
    /*----------------------------------------------*/
    DMLRecord(const char * data,META::metaDataCollection * mc)  :
        record(data)
    {
        const char * ptr = data+head->headSize;
		tableMetaID = *(uint64_t*)ptr;
		if (mc == nullptr || (meta = mc->get(tableMetaID))== nullptr)
			return;
        ptr += sizeof(tableMetaID);
		nullBitmap = (const uint8_t*)ptr;
		ptr += (meta->m_columnsCount >> 3) + (meta->m_columnsCount & 0x7f ? 1 : 0);
		columns = ptr;
		ptr += meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount];
		varLengthColumns = (const uint32_t *)ptr;
		ptr = columns + varLengthColumns[meta->m_varColumnCount];
        if(head->type == R_UPDATE||head->type == R_REPLACE)
        {
            updatedBitmap = (uint8_t*)ptr;
            ptr+= (meta->m_columnsCount >>3)+(meta->m_columnsCount &0x7f?1:0);
			oldColumnsSizeOfUpdateType = *(uint32_t*)ptr;
			oldColumnsOfUpdateType = ptr+sizeof(uint32_t*);
			ptr+= oldColumnsSizeOfUpdateType+ sizeof(uint32_t*);
        }
		else
		{
			updatedBitmap = nullptr;
			oldColumnsOfUpdateType = nullptr;
		}
      /*if version increased in future,add those code:
        if(head->version >0)
        {do some thing}
        if(head->version >1)
        {do some thing}
           ...
         */
    }
	static inline uint64_t tableId(const char * data)
	{
		return *(uint64_t*)(data + ((const recordHead*)data)->headSize);
	}
	inline const char* column(uint16_t index) const
	{
		if (TEST_BITMAP(nullBitmap, index))
			return nullptr;
		if (columnInfos[meta->m_columns[index].m_columnType].fixed)
			return columns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[index]];
		else
			return columns + varLengthColumns[meta->m_realIndexInRowFormat[index]];
	}
	inline uint32_t varColumnSize(uint16_t index)const 
	{
		return varLengthColumns[meta->m_realIndexInRowFormat[index+1]] - varLengthColumns[meta->m_realIndexInRowFormat[index]];
	}
    inline const char* oldColumnOfUpdateType(uint16_t index) const
    {
        if(TEST_BITMAP(updatedBitmap,index))
        {
		const char * pos = oldColumnsOfUpdateType;
		for (uint16_t i = 0; i < meta->m_columnsCount; i++)
		{
			if (*(uint16_t*)(pos) < index)
			{
				if (columnInfos[meta->m_columns[*(uint16_t*)(pos)].m_columnType].fixed)
					pos += sizeof(uint16_t) + columnInfos[meta->m_columns[*(uint16_t*)(pos)].m_columnType].columnTypeSize;
				else
					pos += sizeof(uint16_t) + sizeof(uint32_t) + *(uint32_t*)(pos + sizeof(uint16_t));
			}
			else if (*(uint16_t*)(pos) == index)
			{
				if (columnInfos[meta->m_columns[*(uint16_t*)(pos)].m_columnType].fixed)
					return pos + sizeof(uint16_t);
				else
					return pos + sizeof(uint16_t) + sizeof(uint32_t);
			}
			else
				return nullptr;
		}
		return nullptr;
        }
        else
        {
            return column(index);
        }
    }
	inline uint32_t oldVarColumnSizeOfUpdateType(uint16_t index,const char * value) const
	{
		if (TEST_BITMAP(updatedBitmap, index))
			return *(uint32_t*)(value - sizeof(uint32_t));
		else
			return varColumnSize(index);
	}
    inline bool isKeyUpdated(const uint16_t * key,uint16_t keyColumnCount) const
    {
        for(uint16_t idx=0;idx<keyColumnCount;idx++)
        {
            if(TEST_BITMAP(updatedBitmap,key[idx]))
                return true;
        }
        return false;
    }
};
struct DDLRecord :public record
{
    /*--------------version 0----------------*/
    /*[32 bit sqlMode]
     * [8 bit charset size][charset string + 1byte '\0']
     * [8 bit database size][database string + 1byte '\0']
     * [8 bit ddl size][ddl string + 1byte '\0']
     * */
    uint32_t sqlMode;
    const char * charset;
    const char * database;
    const char * ddl;
    /*----------------------------------------------*/
    DDLRecord(const char* data) :
            record(data)
    {
        const char * ptr = data;
        sqlMode = *(uint32_t*) (ptr + head->headSize);
        database = ptr + head->headSize + sizeof(sqlMode) + 1;
        ddl = database + *(uint8_t*) (database - 1);
        charset = ddl + *(uint8_t*) (ddl - 1);
        /*if version increased in future,add those code:
          if(head->version >0)
          {do some thing}
          if(head->version >1)
          {do some thing}
             ...
           */
    }
};
static record * createRecord(const char * data, META::metaDataCollection * mc)
{
	recordHead * head = (recordHead*)data;
	switch (head->type)
	{
	case R_INSERT:
	case R_DELETE:
	case R_UPDATE:
	case R_REPLACE:
	{
		return new DMLRecord(data, mc);
	}
	case R_DDL:
	{
		return new DDLRecord(data);
	}
	case R_TABLE_META:
	{
		return new TableMetaMessage(data);
	}
	case R_DATABASE_META:
	{
		return new DatabaseMetaMessage(data);
	}
	default:
		return new record(data);
	}
}
}




#endif /* RECORD_H_ */
