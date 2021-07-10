#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "util/String.h"
#include "meta/metaData.h"
#include "meta/metaDataBaseCollection.h"
#include "util/likely.h"
namespace RPC
{
#pragma pack(1)
#define DBS_MESSAGE_VERSION 1
	struct MinRecordHead
	{
		uint32_t size;
		uint8_t externSize;//only used in dml type,support row data size greater than 4G
		uint8_t headSize;
		uint8_t type;
		uint8_t version;
	};

	typedef union {
		struct
		{
			uint32_t logId;
			uint32_t fileId;
		};
		uint64_t seqNo;
	} LogSeqNo;

	struct Checkpoint
	{
		uint64_t timestamp;
		uint64_t logOffset;
		LogSeqNo seqNo;
		uint64_t txnId;
		uint16_t externSize;
		char externInfo[1];//like mysql gtid , oracle logminer safe checkpoint, oracle xstream position
		inline uint16_t checkpointSize() const
		{
			return sizeof(Checkpoint) - 1 + externSize;
		}
		Checkpoint* clone()
		{
			Checkpoint* c = (Checkpoint*)malloc(checkpointSize());
			memcpy(c, this, checkpointSize());
			return c;
		}
	};

	struct RecordHead
	{
		MinRecordHead minHead;
		uint16_t flag;
		uint64_t recordId;
		Checkpoint checkpoint;
		inline uint16_t size()
		{
			return sizeof(RecordHead) + checkpoint.externSize - 1;
		}
	};
#define ENDOF_REDO_NUM  0xfff8fff4fff2fff1
#define UNSIGNED_COLUMN 0x01
#define GENRATED_COLUMN 0x02
#define PRIMARY_KEY_COLUMN 0x04
#define UNIQUE_KEY_COLUMN 0x08
	enum class RecordType
	{
		R_INSERT,
		R_UPDATE,
		R_DELETE,
		R_REPLACE,
		R_DDL,
		R_BEGIN,
		R_COMMIT,
		R_ROLLBACK,
		R_LOB_TRIM,
		R_LOB_WRITE,
		R_LOB_ERASE,
		R_CONTROL,
		R_TABLE_META,
		M_TABLE_META_REQ,
		R_DATABASE_META,
		M_DATABASE_META_REQ,
		R_HEARTBEAT,
		M_LOGIN_REQ,
		M_LOGIN_ACK,
		M_LOGIN,
		M_LOGIN_RESULT,
		M_SQL,
		MAX_RECORD_TYPE
	};
#define COLUMN_FLAG_VIRTUAL 0x01
#define COLUMN_FLAG_SIGNED 0x02

	struct ColumnDef
	{
		uint8_t type;
		uint8_t srcType;
		uint16_t charsetID;
		uint32_t flag;
		uint32_t size;
		uint32_t precision;
		uint32_t decimals;
		uint32_t nameOffset;
		uint32_t collateOffset;
		uint32_t setOrEnumInfoOffset;
		uint32_t defaultValueOffset;
	};
	struct Record
	{
		const char* data;
		RecordHead* head;
		inline void init(const char* data)
		{
			this->data = data;
			head = (RecordHead*)data;
			memset(&head->checkpoint, 0, sizeof(head->checkpoint) - 1);
			head->minHead.externSize = 0;
			head->minHead.version = DBS_MESSAGE_VERSION;
			head->minHead.headSize = head->size();
			head->flag = 0;
			head->recordId = 0;
		}

		inline void init(const char* data, const Checkpoint * ckp)
		{
			this->data = data;
			head = (RecordHead*)data;
			memcpy(&head->checkpoint, ckp, ckp->checkpointSize());
			head->minHead.externSize = 0;
			head->minHead.version = DBS_MESSAGE_VERSION;
			head->minHead.headSize = head->size();
			head->flag = 0;
			head->recordId = 0;
		}

		Record() {}
		Record(const char* data) {
			this->data = data;
			head = (RecordHead*)data;
		}
		String toString()
		{
			head->recordId = 0;
			String str;
			str = str << "type:" << head->minHead.type;
			str = str << "\ntimestamp:" << head->checkpoint.timestamp;
			str = str << "\nlogOffset:" << head->checkpoint.logOffset;
			str = str << "\ntid:" << head->checkpoint.txnId << "\n";
			str = str << "\nrecordId:" << head->recordId;
			str = str << "\nversion:" << head->minHead.version;
			return str;
		}
	};
	static constexpr auto recordSize = sizeof(Record) + sizeof(RecordHead);
	static constexpr auto recordRealSize = sizeof(RecordHead);
	static constexpr auto recordHeadSize = sizeof(RecordHead);
#define TEST_BITMAP(m,i) (((m)[(i)>>3]>>(i&0x7))&0x1)
#define SET_BITMAP(m,i)  (m)[(i)>>3]|= (0x01<<(i&0x7))
#define UNSET_BITMAP(m,i) (m)[(i)>>3] &= (~(0x01<<(i&0x7)))
#define BITMAP_SIZE(l) (((l) >> 3) + ((l) & 0x7f ? 1 : 0))
	/*
	* [16 bit charsetID][database string+ 1 byte '\0']
	*/
	struct DatabaseMetaMessage :public Record
	{
		uint64_t id;
		uint16_t charsetID;
		const char* dbName;
		DatabaseMetaMessage(const char* data) :Record(data) {
			id = *(uint64_t*)(data + +head->minHead.headSize);
			charsetID = *(uint16_t*)(data + +head->minHead.headSize + sizeof(uint64_t));
			dbName = data + +head->minHead.headSize + sizeof(uint64_t) + sizeof(uint16_t);
		}
	};
	struct TableMetaMessage :public Record
	{
		/*--------------version 0----------------*/
		/*[64 bit tableMetaID][64 bit tableMetaObjectId][32 bit tableMetaSubObjectIdCount][16 bit charsetID][16 bit column count][16 bit pk columns count][16 bit uk count][16 bit unsused column count][8 bit caseSensitive]
		 *[8 bit database size][database string+ 1 byte '\0'][8 bit table size][table string+ 1 byte '\0']
		 * [columnDef 1]...[columnDef n]
		 * [8 bit pkname size][pkname string+ 1 byte '\0']
		 * [16 bit pk column id 1]...[16 bit pk column id n]
		 * [16 bit uk 1 column count]...[16 bit uk n column count]
		 *[32 bit uk 1 name offset][16 bit uk 1 column 1 id ]...[16 bit uk 1 column n id ]
		 * ...
		 *[32 bit uk m name offset][16 bit uk m column 1 id ]...[16 bit uk m column n id ]
		 *[64bit tableMetaSubObjectId]...[64 bit tableMetaSubObjectIdCount]
		 *[32 bit column names size]
		 *[column 1 name string+'\0']...[column n name string+'\0']
		 *[32 bit unique key names size]
		 *[uk 1 name string+'\0']...[uk n name string+'\0']
		 *[32 bit enum or set value lists size]
		 *[enum or set value list 0+'\0']...[enum or set value list m+'\0']
		 *[16 bit default value length,8 bit default value type, default value] ... [16 bit default value length,8 bit default value type, default value]
		 *[16 bit unsused column id]...[16 bit unsused column id]
		 * */
		struct TableMetaHead {
			uint64_t tableMetaID;
			uint64_t tableMetaObjectId;
			uint32_t tableMetaSubObjectIdCount;
			uint16_t charsetId;
			uint16_t columnCount;
			uint16_t primaryKeyColumnCount;
			uint16_t uniqueKeyCount;
			uint16_t unusedColumnCount;
			uint8_t caseSensitive;
		};
		TableMetaHead metaHead;
		const char* database;
		const char* table;
		const ColumnDef* columns;
		const uint16_t* primaryKeys;
		const char* primaryKeyName;
		const uint16_t* uniqueKeyColumnCounts;
		uint32_t* uniqueKeyNameOffset;
		uint16_t** uniqueKeys;
		uint64_t* tableMetaSubObjectIds;
		uint16_t* unusedColumnIds;
		/*-----------------------------------------------*/
		TableMetaMessage(const char* data) :Record(data) {
			const char* ptr = data + head->minHead.headSize;
			memcpy(&metaHead, ptr, sizeof(metaHead));
			ptr += sizeof(metaHead);
			database = ptr + sizeof(uint8_t);
			table = database + *(uint8_t*)(database - sizeof(uint8_t));
			ptr = table + *(uint8_t*)(table - sizeof(uint8_t));

			columns = (ColumnDef*)ptr;
			ptr = (const char*)&columns[metaHead.columnCount];
			if (metaHead.primaryKeyColumnCount != 0)
			{
				uint8_t pkNameSize = *ptr++;
				primaryKeyName = ptr;
				ptr += 1 + pkNameSize;
				primaryKeys = (uint16_t*)ptr;
				ptr = (const char*)&primaryKeys[metaHead.primaryKeyColumnCount];
			}
			else
			{
				primaryKeyName = nullptr;
				primaryKeys = nullptr;
			}
			if (metaHead.uniqueKeyCount > 0)
			{
				uniqueKeyColumnCounts = (uint16_t*)(ptr);
				ptr = (const char*)&uniqueKeyColumnCounts[metaHead.uniqueKeyCount];
				uniqueKeys = (uint16_t**)malloc(sizeof(uint16_t*) * metaHead.uniqueKeyCount);
				uniqueKeyNameOffset = (uint32_t*)malloc(sizeof(uint32_t) * metaHead.uniqueKeyCount);
				for (uint16_t idx = 0; idx < metaHead.uniqueKeyCount; idx++)
				{
					uniqueKeyNameOffset[idx] = *(uint32_t*)ptr;
					uniqueKeys[idx] = (uint16_t*)(ptr + sizeof(uint32_t));
					ptr = (const char*)&uniqueKeys[idx][uniqueKeyColumnCounts[idx]];
				}
			}
			else
			{
				uniqueKeyNameOffset = nullptr;
				uniqueKeyColumnCounts = nullptr;
				uniqueKeys = nullptr;
			}
			if (metaHead.tableMetaSubObjectIdCount > 0)
			{
				tableMetaSubObjectIds = (uint64_t*)(ptr);
				ptr = (const char*)&uniqueKeyColumnCounts[metaHead.tableMetaSubObjectIdCount];
			}
			else
			{
				tableMetaSubObjectIds = nullptr;
			}
			if (metaHead.uniqueKeyCount > 0)
			{
				unusedColumnIds = (uint16_t*)ptr;
				ptr = (const char*)&unusedColumnIds[metaHead.uniqueKeyCount];
			}
			ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over column names
			ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over uk names
			ptr += *(uint32_t*)ptr + sizeof(uint32_t);//jump over enum and set value lists
			if (head->minHead.version == 1)
				assert(ptr == data + head->minHead.size);
			/*if version increased in future,add those code:
			  if(head->minHead.version >0)
			  {do some thing}
			  if(head->minHead.version >1)
			  {do some thing}
				 ...
			   */
		}
		inline const char* columnName(uint16_t columnIndex)
		{
			return data + columns[columnIndex].nameOffset;
		}
		inline const char* collateName(uint16_t columnIndex)
		{
			if (columns[columnIndex].collateOffset == 0)
				return nullptr;
			return data + columns[columnIndex].collateOffset;
		}
		inline bool setOrEnumValues(uint16_t columnIndex, const char*& base, uint16_t*& valueList, uint16_t& valueListSize)
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
			if (uniqueKeys != NULL)
				free(uniqueKeys);
		}
	};
	struct DMLRecord :public Record
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
		const META::TableMeta* meta;
		const uint8_t* nullBitmap;
		const char* columns;
		const uint32_t* varLengthColumns;
		const char* oldColumns;
		const uint32_t* oldVarLengthColumns;
		const uint8_t* updatedBitmap;
		const uint8_t* updatedNullBitmap;
		DMLRecord() {}
		DMLRecord(char* data, const META::TableMeta* meta, const Checkpoint* ckp,  RecordType type) //for write
		{
			initRecord(data, meta, ckp, type);
		}
		inline void initRecord(char* data, const META::TableMeta* meta, const Checkpoint* ckp, RecordType type)
		{
			init(data, ckp);
			this->meta = meta;
			oldColumns = nullptr;
			oldVarLengthColumns = nullptr;
			updatedBitmap = nullptr;
			updatedNullBitmap = nullptr;
			head->minHead.type = static_cast<uint8_t>(type);
			head->minHead.externSize = 0;
			char* ptr = data + head->minHead.headSize;
			tableMetaID = meta->m_id;
			*((uint64_t*)(ptr)) = tableMetaID;
			ptr += sizeof(tableMetaID);
			nullBitmap = (const uint8_t*)ptr;
			memset((void*)nullBitmap, 0, BITMAP_SIZE(meta->m_columnsCount));
			ptr += BITMAP_SIZE(meta->m_columnsCount);
			columns = ptr;
			ptr += meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount];
			varLengthColumns = (const uint32_t*)ptr;
			((uint32_t*)varLengthColumns)[meta->m_varColumnCount] = (char*)&varLengthColumns[meta->m_varColumnCount + 1] - columns;
		}
		static inline uint32_t allocSize(const META::TableMeta* meta)
		{
			return sizeof(RecordHead) + 8 //tableMetaID
				+ BITMAP_SIZE(meta->m_columnsCount) //nullBitmap
				+ meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount]//fixed column 
				+ sizeof(uint32_t) * (meta->m_varColumnCount + 1);//var coolumn
		}

		template<class T>
		inline void setFixedColumn(uint16_t id, T value)
		{
			assert(meta->m_realIndexInRowFormat[id] < meta->m_columnsCount);
			*(T*)(columns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[id]]) = value;
			SET_BITMAP((uint8_t*)nullBitmap, id);
		}
		inline void setFixedColumnByMemcopy(uint16_t id, const char* value)
		{
			assert(meta->m_realIndexInRowFormat[id] < meta->m_columnsCount);
			memcpy((char*)columns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[id]], value, META::columnInfos[static_cast<uint8_t>(meta->getColumn(id)->m_columnType)].columnTypeSize);
			SET_BITMAP((uint8_t*)nullBitmap, id);
		}
		inline char* allocVarColumn()
		{
			return ((char*)columns) + varLengthColumns[meta->m_varColumnCount];
		}
		inline void filledVarColumns(uint16_t id, size_t size)
		{
			((uint32_t*)varLengthColumns)[meta->m_realIndexInRowFormat[id]] = varLengthColumns[meta->m_varColumnCount];
			((uint32_t*)varLengthColumns)[meta->m_varColumnCount] += size;
			SET_BITMAP((uint8_t*)nullBitmap, id);
		}
		inline void setVarColumn(uint16_t id, const char* value, size_t size)
		{
			memcpy(((char*)columns) + varLengthColumns[meta->m_varColumnCount], value, size);
			filledVarColumns(id, size);
		}
		inline void setVarColumnNull(uint16_t idx)
		{
			UNSET_BITMAP((uint8_t*)nullBitmap, idx);
			((uint32_t*)varLengthColumns)[meta->m_realIndexInRowFormat[idx]] = varLengthColumns[meta->m_varColumnCount];
		}
		inline void setFixedColumnNull(uint16_t idx)
		{
			UNSET_BITMAP((uint8_t*)nullBitmap, idx);
			memset((char*)columns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[idx]], 0, META::columnInfos[TID(meta->m_columns[idx].m_columnType)].columnTypeSize);
		}
		inline void setColumnNotUpdate(uint16_t idx)
		{
			UNSET_BITMAP((uint8_t*)updatedBitmap, idx);
			if (META::columnInfos[static_cast<int>(meta->m_columns[idx].m_columnType)].fixed)
			{
				memset((char*)oldColumns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[idx]], 0, META::columnInfos[TID(meta->m_columns[idx].m_columnType)].columnTypeSize);
			}
			else
			{
				((uint32_t*)oldVarLengthColumns)[meta->m_realIndexInRowFormat[idx]] = oldVarLengthColumns[meta->m_varColumnCount];
			}
		}

		inline void startSetUpdateOldValue()
		{
			uint16_t bitmapSize = (meta->m_columnsCount >> 3) + (meta->m_columnsCount & 0x7f ? 1 : 0);
			updatedBitmap = (const uint8_t*)(columns + varLengthColumns[meta->m_varColumnCount]);
			memset((void*)updatedBitmap, 0, bitmapSize * 2);
			updatedNullBitmap = updatedBitmap + bitmapSize;
			oldColumns = (const char*)(updatedNullBitmap + bitmapSize);
			oldVarLengthColumns = (const uint32_t*)(((const char*)oldColumns) + meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount]);
			((uint32_t*)oldVarLengthColumns)[meta->m_varColumnCount] = (char*)&oldVarLengthColumns[meta->m_varColumnCount + 1] - oldColumns;

		}
		inline void setUpdatedVarColumnNull(uint16_t idx)
		{
			UNSET_BITMAP((uint8_t*)updatedNullBitmap, idx);
			((uint32_t*)oldVarLengthColumns)[meta->m_realIndexInRowFormat[idx]] = varLengthColumns[meta->m_varColumnCount];
		}
		inline void setUpdatedFixedColumnNull(uint16_t idx)
		{
			UNSET_BITMAP((uint8_t*)updatedNullBitmap, idx);
			memset((char*)oldColumns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[idx]], 0, META::columnInfos[TID(meta->m_columns[idx].m_columnType)].columnTypeSize);
		}
		template<class T>
		inline void setFixedUpdatedColumn(uint16_t id, T value)
		{
			*(T*)(oldColumns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[id]]) = value;
			SET_BITMAP((uint8_t*)updatedNullBitmap, id);
			SET_BITMAP((uint8_t*)updatedBitmap, id);
		}
		inline char* allocVardUpdatedColumn()
		{
			return ((char*)oldColumns) + oldVarLengthColumns[meta->m_varColumnCount];
		}
		inline void filledVardUpdatedColumn(uint16_t id, size_t size)
		{
			((uint32_t*)oldVarLengthColumns)[meta->m_realIndexInRowFormat[id]] = oldVarLengthColumns[meta->m_varColumnCount];
			((uint32_t*)oldVarLengthColumns)[meta->m_varColumnCount] += size;
			SET_BITMAP((uint8_t*)updatedNullBitmap, id);
		}
		inline void setVardUpdatedColumn(uint16_t id, const char* value, size_t size)
		{
			memcpy(((char*)oldColumns) + oldVarLengthColumns[meta->m_varColumnCount], value, size);
			filledVardUpdatedColumn(id, size);
			SET_BITMAP((uint8_t*)updatedBitmap, id);
		}

		inline void finishedSet()
		{
			if (oldColumns == nullptr)
				head->minHead.size = (columns + varLengthColumns[meta->m_varColumnCount]) - data;
			else
				head->minHead.size = (oldColumns + oldVarLengthColumns[meta->m_varColumnCount]) - data;
		}
		/*----------------------------------------------*/
		DMLRecord(const char* data, META::MetaDataBaseCollection* mc) ://for read
			Record(data)
		{
			const char* ptr = data + head->minHead.headSize;
			tableMetaID = *(uint64_t*)ptr;
			this->meta = mc->get(tableMetaID);
			if (this->meta == nullptr)
				return;
			_load(data, this->meta);
		}
		inline void load(const char* data, const META::TableMeta* meta)
		{
			this->data = data;
			head = (RecordHead*)data;
			tableMetaID = *(uint64_t*)(data + head->minHead.headSize);
			this->meta = meta;
			_load(data, meta);
		}
		inline void _load(const char* data, const META::TableMeta* meta)
		{
			const char* ptr = data + head->minHead.headSize;
			ptr += sizeof(tableMetaID);
			nullBitmap = (const uint8_t*)ptr;
			uint16_t bitmapSize = (meta->m_columnsCount >> 3) + (meta->m_columnsCount & 0x7f ? 1 : 0);
			ptr += bitmapSize;
			columns = ptr;
			ptr += meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount];
			varLengthColumns = (const uint32_t*)ptr;
			ptr = columns + varLengthColumns[meta->m_varColumnCount];
			if (head->minHead.type == static_cast<uint8_t>(RecordType::R_UPDATE) || head->minHead.type == static_cast<uint8_t>(RecordType::R_REPLACE))
			{
				updatedBitmap = (uint8_t*)ptr;
				updatedNullBitmap = updatedBitmap + bitmapSize;
				ptr += bitmapSize * 2;
				oldColumns = ptr;
				oldVarLengthColumns = (uint32_t*)(((const char*)oldColumns) + meta->m_fixedColumnOffsetsInRecord[meta->m_fixedColumnCount]);
				ptr = oldColumns + oldVarLengthColumns[meta->m_varColumnCount];
			}
			else
			{
				updatedBitmap = nullptr;
				updatedNullBitmap = nullptr;
				oldColumns = nullptr;
				oldVarLengthColumns = nullptr;
			}
			/*if version increased in future,add those code:
			  if(head->minHead.version >0)
			  {do some thing}
			  if(head->minHead.version >1)
			  {do some thing}
				 ...
			   */
		}
		static inline uint64_t tableId(const char* data)
		{
			return *(uint64_t*)(data + ((const RecordHead*)data)->minHead.headSize);
		}
		inline const char* _column(uint16_t index, const uint8_t* _nullBitmap, const char* _columns, const uint32_t* _varLengthColumns)const
		{
			if (!TEST_BITMAP(_nullBitmap, index))
				return nullptr;
			if (META::columnInfos[static_cast<int>(meta->m_columns[index].m_columnType)].fixed)
				return _columns + meta->m_fixedColumnOffsetsInRecord[meta->m_realIndexInRowFormat[index]];
			else
				return _columns + _varLengthColumns[meta->m_realIndexInRowFormat[index]];
		}
		inline const char* column(uint16_t index) const
		{
			return _column(index, nullBitmap, columns, varLengthColumns);
		}
		inline bool columnIsNull(uint16_t index)const
		{
			return !TEST_BITMAP(nullBitmap, index);
		}
		inline uint32_t _varColumnSize(uint16_t index, const uint8_t* _nullBitmap, const uint32_t* _varLengthColumns) const
		{
			if (!TEST_BITMAP(_nullBitmap, index))
				return 0;
			return _varLengthColumns[meta->m_realIndexInRowFormat[index] + 1] - varLengthColumns[meta->m_realIndexInRowFormat[index]];
		}
		inline uint32_t varColumnSize(uint16_t index)const
		{
			return _varColumnSize(index, nullBitmap, varLengthColumns);
		}
		inline const char* oldColumnOfUpdateType(uint16_t index) const
		{
			if (TEST_BITMAP(updatedBitmap, index))
				return _column(index, updatedNullBitmap, oldColumns, oldVarLengthColumns);
			else
				return _column(index, nullBitmap, columns, varLengthColumns);
		}
		inline uint32_t oldVarColumnSizeOfUpdateType(uint16_t index) const
		{
			if (TEST_BITMAP(updatedBitmap, index))
				return _varColumnSize(index, updatedNullBitmap, oldVarLengthColumns);
			else
				return _varColumnSize(index, nullBitmap, varLengthColumns);
		}
		inline bool oldColumnIsNull(uint16_t index)const
		{
			if (TEST_BITMAP(updatedBitmap, index))
			{
				return !TEST_BITMAP(updatedNullBitmap, index);
			}
			else
			{
				return !TEST_BITMAP(nullBitmap, index);
			}
		}
		inline bool isKeyUpdated(const META::UnionKeyMeta* key) const
		{
			for (uint16_t idx = 0; idx < key->columnCount; idx++)
			{
				if (TEST_BITMAP(updatedBitmap, key->columnInfo[idx].columnId))
					return true;
			}
			return false;
		}

		String columnValue(const char* value, uint32_t length, const META::ColumnMeta* column)
		{
			String str;
			if (value == nullptr)
				return "null\n";
			switch (column->m_columnType)
			{
			case META::COLUMN_TYPE::T_UINT8:
				str.append(*(const uint8_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_INT8:
				str.append(*value).append("\n");
				break;
			case META::COLUMN_TYPE::T_INT16:
			case META::COLUMN_TYPE::T_YEAR:
				str.append(*(const int16_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_UINT16:
				str.append(*(const uint16_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_INT32:
				str.append(*(const int32_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_UINT32:
				str.append(*(const uint32_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_INT64:
				str.append(*(const int64_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_UINT64:
				str.append(*(const uint64_t*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_STRING:
			case META::COLUMN_TYPE::T_DECIMAL:
			case META::COLUMN_TYPE::T_BIG_NUMBER:
			case META::COLUMN_TYPE::T_TEXT:
			case META::COLUMN_TYPE::T_JSON:
			case META::COLUMN_TYPE::T_XML:
				str.append(value, length).append("\n");
				break;
			case META::COLUMN_TYPE::T_FLOAT:
				str.append(*(float*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_DOUBLE:
				str.append(*(double*)value).append("\n");
				break;
			case META::COLUMN_TYPE::T_TIMESTAMP:
			{
				META::Timestamp t;
				t.time = *(const uint64_t*)value;
				char s[32] = { 0 };
				t.toString(s);
				str.append(s).append("\n");
			}
			break;
			case META::COLUMN_TYPE::T_DATETIME:
			case META::COLUMN_TYPE::T_DATETIME_ZERO_TZ:
			{
				META::DateTime d;
				d.time = *(const uint64_t*)value;
				char s[32] = { 0 };
				d.toString(s);
				str.append(s).append("\n");
			}
			break;
			case META::COLUMN_TYPE::T_DATE:
			{
				META::Date d;
				d.time = *(const uint32_t*)value;
				char s[32] = { 0 };
				d.toString(s);
				str.append(s).append("\n");
			}
			break;
			case META::COLUMN_TYPE::T_TIME:
			{
				META::Time t;
				t.time = *(const uint64_t*)value;
				char s[32] = { 0 };
				t.toString(s);
				str.append(s).append("\n");
			}
			break;
			case META::COLUMN_TYPE::T_BINARY:
			case META::COLUMN_TYPE::T_BLOB:
			case META::COLUMN_TYPE::T_GEOMETRY:
			{
				char* buf = new char[length * 2 + 2];
				for (uint32_t off = 0; off < length; off++)
				{
					uint8_t c = ((const uint8_t*)value)[off] >> 4;
					if (c < 10)
						buf[off * 2] = c + '0';
					else
						buf[off * 2] = c - 10 + 'A';
					c = ((const uint8_t*)value)[off] & 0xffu;
					if (c < 10)
						buf[off * 2 + 1] = c + '0';
					else
						buf[off * 2 + 1] = c - 10 + 'A';
				}
				buf[length * 2] = '\n';
				buf[length * 2 + 1] = '\0';
				str.append(buf);
				delete[]buf;
			}
			break;
			case META::COLUMN_TYPE::T_SET:
			{
				for (uint8_t idx = 0; idx < column->m_setAndEnumValueList.m_count; idx++)
				{
					if (TEST_BITMAP(value, idx))
					{
						str.append(column->m_setAndEnumValueList.m_array[idx]);
					}
				}
				str.append("\n");
			}
			break;
			case META::COLUMN_TYPE::T_ENUM:
			{
				uint16_t idx = *(const uint16_t*)value;
				assert(idx < column->m_setAndEnumValueList.m_count);
				str.append(column->m_setAndEnumValueList.m_array[idx]).append("\n");
			}
			break;
			default:
				str = String("unknown type:") << static_cast<int>(column->m_columnType) << "\n";
				break;
			}
			return str;
		}
		String toString()
		{
			String str = Record::toString();
			str.append("database:").append(meta->m_dbName).append("\ntable:").append(meta->m_tableName).append("\n");
			for (uint32_t idx = 0; idx < meta->m_columnsCount; idx++)
			{
				const META::ColumnMeta* c = meta->getColumn(idx);
				str.append(c->m_columnName).append(":\n");
				const char* value = column(idx);
				uint32_t valueLength = META::columnInfos[static_cast<int>(c->m_columnType)].fixed ? META::columnInfos[static_cast<int>(c->m_columnType)].columnTypeSize : varColumnSize(idx);
				String cv = columnValue(value, valueLength, c);
				str.append(cv.c_str());
				if (head->minHead.type == static_cast<uint8_t>(RecordType::R_UPDATE) || head->minHead.type == static_cast<uint8_t>(RecordType::R_REPLACE))
				{
					value = oldColumnOfUpdateType(idx);
					valueLength = META::columnInfos[static_cast<int>(c->m_columnType)].fixed ? META::columnInfos[static_cast<int>(c->m_columnType)].columnTypeSize : oldVarColumnSizeOfUpdateType(idx);
					str.append(columnValue(value, valueLength, c));
				}
			}
			return str;
		}
	};
	struct DDLRecord :public Record
	{
		/*--------------version 0----------------*/
		/*[32 bit sqlMode]
		 * [8 bit charset size][charset string + 1byte '\0']
		 * [8 bit database size][database string + 1byte '\0']
		 * [8 bit ddl size][ddl string + 1byte '\0']
		 * */
		uint64_t sqlMode;
		uint16_t charsetId;
		const char* database;
		const char* ddl;
		/*----------------------------------------------*/
		DDLRecord() {}
		inline void create(const char* data, const Checkpoint * ckp, const char* charset, uint64_t sqlMode, const char* database, const char* query, uint32_t querySize)
		{
			init(data, ckp);
			this->sqlMode = sqlMode;
			*(uint64_t*)(data + head->minHead.headSize) = sqlMode;
			charsetId = ascii;
			if (charset != nullptr)
			{
				const charsetInfo* ci = getCharset(charset);
				if (likely(ci != nullptr))
					charsetId = ci->id;
			}
			*(uint16_t*)(data + head->minHead.headSize + sizeof(uint64_t)) = charsetId;
			this->database = data + head->minHead.headSize + sizeof(sqlMode) + sizeof(charsetId) + 1;
			if (database != nullptr && strlen(database) > 0)
			{
				*(uint8_t*)(this->database - 1) = strlen(database) + 1;
				memcpy((char*)this->database, database, *(uint8_t*)(this->database - 1));
			}
			else
			{
				*(uint8_t*)(this->database - 1) = 0;
			}
			ddl = this->database + *(uint8_t*)(this->database - 1);
			memcpy((char*)ddl, query, querySize);
			if (((char*)ddl)[querySize - 1] != '\0')
			{
				((char*)ddl)[querySize] = '\0';
				head->minHead.size = ddl - data + querySize + 1;
			}
			else
				head->minHead.size = ddl - data + querySize;
			if (database == nullptr || *(uint8_t*)(this->database - 1) == 0)
				this->database = nullptr;
		}
		/*---------------------------------------------*/
		DDLRecord(const char* data)
		{
			load(data);
		}
		inline void load(const char* data)
		{
			this->data = data;
			head = (RecordHead*)data;
			const char* ptr = data;
			sqlMode = *(uint64_t*)(ptr + head->minHead.headSize);
			charsetId = *(uint16_t*)(ptr + head->minHead.headSize + sizeof(uint64_t));
			database = ptr + head->minHead.headSize + sizeof(sqlMode) + sizeof(charsetId) + 1;
			ddl = database + *(uint8_t*)(database - 1);
			/*if version increased in future,add those code:
			  if(head->minHead.version >0)
			  {do some thing}
			  if(head->minHead.version >1)
			  {do some thing}
				 ...
			   */
		}
		static inline uint32_t allocSize(uint32_t dataBaseSize, uint32_t ddlSize)
		{
			return sizeof(RecordHead) + sizeof(sqlMode) + sizeof(charsetId) + 1 + dataBaseSize + ddlSize + 2;
		}
		inline uint8_t databaseSize()const
		{
			return *(const uint8_t*)(database - 1);
		}
		inline uint32_t ddlSize()const
		{
			return head->minHead.size - (ddl - data);
		}
		String toString()
		{
			String str = Record::toString();
			str = str << "sqlMode:" << sqlMode << "\n" << "charset:" << charsets[charsetId].name << "\n";
			if (database != nullptr && *(const uint8_t*)(database - 1) > 0)
			{
				str.append("database:").append(database).append("\n");
			}
			str.append("query:").append(ddl).append("\n");
			return str;
		}
	};
	static Record* createRecord(const char* data, META::MetaDataBaseCollection* mc)
	{
		RecordHead* head = (RecordHead*)data;
		switch (static_cast<RecordType>(head->minHead.type))
		{
		case RecordType::R_INSERT:
		case RecordType::R_DELETE:
		case RecordType::R_UPDATE:
		case RecordType::R_REPLACE:
		{
			return new DMLRecord(data, mc);
		}
		case RecordType::R_DDL:
		{
			return new DDLRecord(data);
		}
		case RecordType::R_TABLE_META:
		{
			return new TableMetaMessage(data);
		}
		case RecordType::R_DATABASE_META:
		{
			return new DatabaseMetaMessage(data);
		}
		default:
			return new Record(data);
		}
	}
	static String getString(Record* r)
	{
		switch (static_cast<RecordType>(r->head->minHead.type))
		{
		case RecordType::R_INSERT:
		case RecordType::R_DELETE:
		case RecordType::R_UPDATE:
		case RecordType::R_REPLACE:
		{
			return static_cast<DMLRecord*>(r)->toString();
		}
		case RecordType::R_DDL:
		{
			return static_cast<DDLRecord*>(r)->toString();
		}
		default:
			return r->toString();
		}
	}
	struct LoginReq :public MinRecordHead
	{
	};
	struct LoginAck :public MinRecordHead
	{
		uint8_t scramble[128];
		uint32_t crc;
	};
	struct LoginMessage :public MinRecordHead
	{
		uint8_t user[32];
		uint8_t password[64];
		uint32_t crc;
	};
	struct LoginResult :public MinRecordHead
	{
		uint8_t success;
		uint32_t userId;
	};

#pragma pack()

}