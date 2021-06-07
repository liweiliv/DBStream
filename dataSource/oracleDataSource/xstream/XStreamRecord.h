#pragma once
namespace DATA_SOURCE
{
	struct lcrHeader
	{
		oratext* srcDbName;//Canonicalized source database name. Must be non-NULL.
		ub2 srcDbNameLen;//Length of the src_db_name string in bytes excluding the NULL terminator.
		oratext* cmdType;//cmd type ,like INSERT,UPDATE..
		ub2 cmdTypeLen;//Length of the cmd_type string in bytes excluding the NULL terminator.
		oratext* owner;//Canonicalized table owner name. Must be non-NULL. For procedure LCRs, the package or procedure owner is returned in owner.
		ub2 ownerLen;//Length of the owner string in bytes excluding the NULL terminator.
		oratext* oname;//Canonicalized table name. Must be non-NULL. For procedure LCRs, the procedure name is returned in oname.
		ub2 onameLen;//Length of the oname string in bytes excluding the NULL terminator.
		ub1* tag;//A binary tag that enables tracking of the LCR. For example, you can use this tag to determine the original source database of the DML statement if apply forwarding is used.
		ub2 tagLen;//Number of bytes in the tag.
		oratext* txid;//Transaction ID. Must be non-NULL
		ub2 txidLen;//Length of the string in bytes excluding the NULL terminator.
		OCIDate srcTime;//The time when the change was generated in the redo log file of the source database.
		ub2 oldColumns;//Number of columns in the OLD column list. Returns 0 if the input LCR is a DDL LCR. Optional.
		ub2 newColumns;//Number of columns in the NEW column list. Returns 0 if the input LCR is a DDL LCR. Optional.
		ub1* position;//Position for LCR.
		ub2 positionLen;//Length of position.
		oraub8 flag; //LCR flag.Possible flags are listed in Comments.
		/*
		*OCI_ROWLCR_HAS_ID_KEY_ONLY   only has ID key cols
		*OCI_ROWLCR_SEQ_LCR           sequence lcr
		*/
		lcrHeader()
		{
			reset();
		}
		inline void reset()
		{
			memset(&srcDbName, 0, sizeof(lcrHeader));
		}
	};
	constexpr static auto MAX_COLUMN_COUNT = 2048;
	struct columnValuesInfo
	{
		ub2 columnNumber; //Number of columns in the specified column array
		oratext* columnNames[MAX_COLUMN_COUNT];// An array of column name pointers.
		ub2 columnNamelengths[MAX_COLUMN_COUNT];//An array of column name lengths.
		ub2 columnDataTypes[MAX_COLUMN_COUNT]; //An array of column data types.Optional.Data is not returned if column_dtyp is NULL
		void* columnValues[MAX_COLUMN_COUNT];//An array of column data pointers.
		OCIInd columnIndicators[MAX_COLUMN_COUNT];//An array of indicators.
		ub2 columnValueLengths[MAX_COLUMN_COUNT];//An array of column lengths. Each returned element is the length in bytes.
		ub1 columnCharsets[MAX_COLUMN_COUNT];//An array of character set forms for the columns. Optional. Data is not returned if the argument is NULL.
		oraub8 columnFlags[MAX_COLUMN_COUNT];//An array of column flags.Optional.Data is not returned if the argument is NULL.See Comments for the values.
		ub2 columnCharsetIds[MAX_COLUMN_COUNT];//An array of character set IDs for the columns.
		columnValuesInfo()
		{
			reset();
		}
		inline void reset()
		{
			memset(&columnNumber, 0, sizeof(columnValuesInfo));
		}
	};

	// OCI_LCR_ATTR_THREAD_NO              "THREAD#"
	// OCI_LCR_ATTR_ROW_ID                 "ROW_ID"
	// OCI_LCR_ATTR_SESSION_NO             "SESSION#"
	// OCI_LCR_ATTR_SERIAL_NO              "SERIAL#"
	// OCI_LCR_ATTR_USERNAME               "USERNAME"
	// OCI_LCR_ATTR_TX_NAME                "TX_NAME"
	// OCI_LCR_ATTR_EDITION_NAME           "EDITION_NAME"
	// OCI_LCR_ATTR_MESSAGE_TRACKING_LABEL "MESSAGE_TRACKING_LABEL"
	// OCI_LCR_ATTR_CURRENT_USER           "CURRENT_USER"
	// OCI_LCR_ATTR_ROOT_NAME              "ROOT_NAME"
	constexpr static auto MAX_ATTR_COUNT = 20;
	struct lcrAttributes
	{
		ub2 attrsNumber;//Number of extra attributes.
		oratext* attrNames[MAX_ATTR_COUNT];//An array of extra attribute name pointers.
		ub2 attrNamesLens[MAX_ATTR_COUNT];//An array of extra attribute name lengths.
		ub2 attrDataTypes[MAX_ATTR_COUNT];//An array of extra attribute data types. Valid data types: see Comments.
		void* attrValues[MAX_ATTR_COUNT];//An array of extra attribute data value pointers.
		OCIInd attrIndp[MAX_ATTR_COUNT];//An indicator array. Each returned element is an OCIInd value (OCI_IND_NULL or OCI_IND_NOTNULL).
		ub2 attrValueLength[MAX_ATTR_COUNT];//An array of actual extra attribute data lengths. Each element in alensp is the length in bytes.
		lcrAttributes()
		{
			reset();
		}
		inline void reset()
		{
			memset(&attrsNumber, 0, sizeof(lcrAttributes));
		}
	};

	/*object type:
	CLUSTER
	FUNCTION
	INDEX
	OUTLINE
	PACKAGE
	PACKAGE BODY
	PROCEDURE
	SEQUENCE
	SYNONYM
	TABLE
	TRIGGER
	TYPE
	USER
	VIEW
	*/

	struct ddlInfo
	{
		oratext* objectType;//The type of object on which the DDL statement was executed.
		ub2 objectTypeLength;//Length of the object_type string without the NULL terminator.
		oratext* ddlText; //The text of the DDL statement
		ub4 ddlTextLength; //DDL text length in bytes without the NULL terminator.
		oratext* logonUser;//Canonicalized (follows a rule or procedure) name of the user whose session executed the DDL statement
		ub2 logonUserLen;//Length of the logon_user string without the NULL terminator.
		oratext* currentSchema;//The canonicalized schema name that is used if no schema is specified explicitly for the modified database objects in ddl_text
		ub2 currentSchemaLen;//Length of the current_schema string without the NULL terminator.
		oratext* baseTableOwner;//If the DDL statement is a table-related DDL (such as CREATE TABLE and ALTER TABLE), or if the DDL statement involves a table (such as creating a trigger on a table), then base_table_owner specifies the canonicalized owner of the table involved. Otherwise, base_table_owner is NULL. 
		ub2 baseTableOwnerLen;//Length of the base_table_owner string without the NULL terminator.
		oratext* baseTableName;//If the DDL statement is a table-related DDL (such as CREATE TABLE and ALTER TABLE), or if the DDL statement involves a table (such as creating a trigger on a table), then base_table_name specifies the canonicalized name of the table involved. Otherwise, base_table_name is NULL.
		ub2 baseTableNameLen;//Length of the base_table_name string without the NULL terminator.
		oraub8 flag;//DDL LCR flag. Optional. Data not returned if argument is NULL. Future extension not used currently.
		ddlInfo()
		{
			reset();
		}
		inline void reset()
		{
			memset(&objectType, 0, sizeof(ddlInfo));
		}
	};

	struct chunk
	{
		oratext* columnName;//Name of the column that has data.
		ub2 columnNameLen;//Length of the column name.
		ub2 columnDataType;//Column chunk data type (either SQLT_CHR or SQLT_BIN).
		oraub8 columnFlag;//Column flag. See Comments for valid flags.
		ub2 columnCharsetId;//Column character set ID. This is returned only for XMLType column, that is, column_flag has OCI_LCR_COLUMN_XML_DATA bit set.
		ub4 chunkBytes;//Number of bytes in the returned chunk.
		ub1* chunkData;//Pointer to the chunk data in the LCR. The client must not deallocate this buffer since the LCR and its contents are maintained by this function.
		oraub8 flag;//If OCI_XSTREAM_MORE_ROW_DATA (0x01) is set, then the current LCR has more chunks coming.
		chunk* next;
		chunk() :columnName(nullptr), columnNameLen(0), columnDataType(0), columnFlag(0), columnCharsetId(0), chunkBytes(0), chunkData(nullptr), flag(0), next(nullptr) {}
		inline void reset()
		{
			memset(&columnName, 0, sizeof(chunk));
		}
	};

	struct XStreamRecord
	{
		void* lcr;
		DATABASE_INCREASE::RecordType recordType;
		lcrHeader m_lcrHeader;
		columnValuesInfo m_newColumns;
		columnValuesInfo m_oldColumns;
		ddlInfo m_ddlInfo;
		lcrAttributes m_attrs;
		chunk m_chunkHead;
		chunk* m_lastChunk;
		int m_chunkCount;
	};
}