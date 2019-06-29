/*
 *  * BinaryLogEvent.h
 *   *
 *    *  Created on: 2018年7月3日
 *     *      Author: liwei
 *      */

#ifndef BINARYLOGEVENT_H_
#define BINARYLOGEVENT_H_
#include <stdint.h>
#include <string>
#include "mysql/my_byteorder.h"
namespace DATA_SOURCE {
#if !defined(le32toh)
	/**
	Converting a 32 bit integer from little-endian byte order to host byteorder

	@param x  32-bit integer in little endian byte order
	@return  32-bit integer in host byte order
	*/
	uint32_t inline le32toh(uint32_t x) {
#ifndef IS_BIG_ENDIAN
		return x;
#else
		return (((x >> 24) & 0xff) | ((x << 8) & 0xff0000) | ((x >> 8) & 0xff00) |
			((x << 24) & 0xff000000));
#endif
	}
#endif

#define LOG_EVENT_BINLOG_IN_USE_F       0x1
#define LOG_EVENT_ARTIFICIAL_F 0x20

	/**
	  Enumeration type for the different types of log events.
	*/
	enum Log_event_type {
		/**
		  Every time you add a type, you have to
		  - Assign it a number explicitly. Otherwise it will cause trouble
			if a event type before is deprecated and removed directly from
			the enum.
		  - Fix Format_description_event::Format_description_event().
		*/
		UNKNOWN_EVENT = 0,
		/*
		  Deprecated since mysql 8.0.2. It is just a placeholder,
		  should not be used anywhere else.
		*/
		START_EVENT_V3 = 1,
		QUERY_EVENT = 2,
		STOP_EVENT = 3,
		ROTATE_EVENT = 4,
		INTVAR_EVENT = 5,

		SLAVE_EVENT = 7,

		APPEND_BLOCK_EVENT = 9,
		DELETE_FILE_EVENT = 11,

		RAND_EVENT = 13,
		USER_VAR_EVENT = 14,
		FORMAT_DESCRIPTION_EVENT = 15,
		XID_EVENT = 16,
		BEGIN_LOAD_QUERY_EVENT = 17,
		EXECUTE_LOAD_QUERY_EVENT = 18,

		TABLE_MAP_EVENT = 19,

		/**
		  The V1 event numbers are used from 5.1.16 until mysql-5.6.
		*/
		WRITE_ROWS_EVENT_V1 = 23,
		UPDATE_ROWS_EVENT_V1 = 24,
		DELETE_ROWS_EVENT_V1 = 25,

		/**
		  Something out of the ordinary happened on the master
		 */
		INCIDENT_EVENT = 26,

		/**
		  Heartbeat event to be send by master at its idle time
		  to ensure master's online status to slave
		*/
		HEARTBEAT_LOG_EVENT = 27,

		/**
		  In some situations, it is necessary to send over ignorable
		  data to the slave: data that a slave can handle in case there
		  is code for handling it, but which can be ignored if it is not
		  recognized.
		*/
		IGNORABLE_LOG_EVENT = 28,
		ROWS_QUERY_LOG_EVENT = 29,

		/** Version 2 of the Row events */
		WRITE_ROWS_EVENT = 30,
		UPDATE_ROWS_EVENT = 31,
		DELETE_ROWS_EVENT = 32,

		GTID_LOG_EVENT = 33,
		ANONYMOUS_GTID_LOG_EVENT = 34,

		PREVIOUS_GTIDS_LOG_EVENT = 35,

		TRANSACTION_CONTEXT_EVENT = 36,

		VIEW_CHANGE_EVENT = 37,

		/* Prepared XA transaction terminal event similar to Xid */
		XA_PREPARE_LOG_EVENT = 38,

		/**
		  Extension of UPDATE_ROWS_EVENT, allowing partial values according
		  to binlog_row_value_options.
		*/
		PARTIAL_UPDATE_ROWS_EVENT = 39,

		/**
		  Add new events here - right above this comment!
		  Existing events (except ENUM_END_EVENT) should never change their numbers
		*/
		ENUM_END_EVENT /* end marker */
	};

#pragma pack(push,1)
	struct commonMysqlBinlogEventHeader_v4
	{
		uint32_t timestamp;
		uint8_t type;
		uint32_t serverId;
		uint32_t eventSize;
		uint32_t eventOffset;
		uint16_t flag;
	};
#pragma pack(pop)
	/**
	 *  The length of the array server_version, which is used to store the version
	 *   of MySQL server.
	 *    We could have used SERVER_VERSION_LENGTH, but this introduces an
	 *     obscure dependency - if somebody decided to change SERVER_VERSION_LENGTH
	 *      this would break the replication protocol
	 *       both of these are used to initialize the array server_version
	 *        SERVER_VERSION_LENGTH is used for global array server_version
	 *         and ST_SERVER_VER_LEN for the Start_event_v3 member server_version
	 *          */

#define ST_SERVER_VER_LEN 50

	 /*
	  *  Event header offsets;
	  *   these point to places inside the fixed header.
	  *    */
#define EVENT_TYPE_OFFSET    4
#define SERVER_ID_OFFSET     5
#define EVENT_LEN_OFFSET     9
#define LOG_POS_OFFSET       13
#define FLAGS_OFFSET         17

	  /** start event post-header (for v3 and v4) */
#define ST_BINLOG_VER_OFFSET  0
#define ST_SERVER_VER_OFFSET  2
#define ST_CREATED_OFFSET     (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
#define ST_COMMON_HEADER_LEN_OFFSET (ST_CREATED_OFFSET + 4)

#define LOG_EVENT_HEADER_LEN 19U    /* the fixed header length */
#define OLD_HEADER_LEN       13U    /* the fixed header length in 3.23 */
#define EVENT_TYPE_PERMUTATION_NUM 23
#define LOG_EVENT_TYPES (ENUM_END_EVENT - 1)
/**
 *  The lengths for the fixed data part of each event.
 *   This is an enum that provides post-header lengths for all events.
 *    */
	enum enum_post_header_length
	{
		/* where 3.23, 4.x and 5.0 agree*/
		QUERY_HEADER_MINIMAL_LEN = (4 + 4 + 1 + 2),
		/* where 5.0 differs: 2 for length of N-bytes vars.*/
		QUERY_HEADER_LEN = (QUERY_HEADER_MINIMAL_LEN + 2),
		STOP_HEADER_LEN = 0,
		LOAD_HEADER_LEN = (4 + 4 + 4 + 1 + 1 + 4),
		START_V3_HEADER_LEN = (2 + ST_SERVER_VER_LEN + 4),
		/* this is FROZEN (the Rotate post-header is frozen)*/
		ROTATE_HEADER_LEN = 8,
		INTVAR_HEADER_LEN = 0,
		CREATE_FILE_HEADER_LEN = 4,
		APPEND_BLOCK_HEADER_LEN = 4,
		EXEC_LOAD_HEADER_LEN = 4,
		DELETE_FILE_HEADER_LEN = 4,
		NEW_LOAD_HEADER_LEN = LOAD_HEADER_LEN,
		RAND_HEADER_LEN = 0,
		USER_VAR_HEADER_LEN = 0,
		FORMAT_DESCRIPTION_HEADER_LEN = (START_V3_HEADER_LEN + 1 + LOG_EVENT_TYPES),
		XID_HEADER_LEN = 0,
		BEGIN_LOAD_QUERY_HEADER_LEN = APPEND_BLOCK_HEADER_LEN,
		ROWS_HEADER_LEN_V1 = 8,
		TABLE_MAP_HEADER_LEN = 8,
		EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN = (4 + 4 + 4 + 1),
		EXECUTE_LOAD_QUERY_HEADER_LEN = (QUERY_HEADER_LEN
		+ \
			EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN),
		INCIDENT_HEADER_LEN = 2,
		HEARTBEAT_HEADER_LEN = 0,
		IGNORABLE_HEADER_LEN = 0,
		ROWS_HEADER_LEN_V2 = 10,
		TRANSACTION_CONTEXT_HEADER_LEN = 18,
		VIEW_CHANGE_HEADER_LEN = 52,
		XA_PREPARE_HEADER_LEN = 0
	};
	/* end enum_post_header_length*/
	/**
	 *  Enumeration spcifying checksum algorithm used to encode a binary log event
	 *   */
	enum enum_binlog_checksum_alg
	{
		/**
	 * 	 Events are without checksum though its generator is checksum-capable
	 * 	 	 New Master (NM).
	 * 	 	 	 */
		BINLOG_CHECKSUM_ALG_OFF = 0,
		/** CRC32 of zlib algorithm */
		BINLOG_CHECKSUM_ALG_CRC32 = 1,
		/** the cut line: valid alg range is [1, 0x7f] */
		BINLOG_CHECKSUM_ALG_ENUM_END,
		/**
	 * 	 Special value to tag undetermined yet checksum or events from
	 * 	 	 checksum-unaware servers
	 * 	 	 	 */
		BINLOG_CHECKSUM_ALG_UNDEF = 255
	};

#define CHECKSUM_CRC32_SIGNATURE_LEN 4

	/**
	 *  defined statically while there is just one alg implemented
	 *   */
#define BINLOG_CHECKSUM_LEN CHECKSUM_CRC32_SIGNATURE_LEN
#define BINLOG_CHECKSUM_ALG_DESC_LEN 1  /* 1 byte checksum alg descriptor */
#define LOG_EVENT_MINIMAL_HEADER_LEN 19u

	class formatEvent
	{
	public:
		uint8_t post_header_len[ENUM_END_EVENT];
		uint8_t number_of_event_types;
		uint8_t common_header_len;
		char server_version[ST_SERVER_VER_LEN];
		unsigned char server_version_split[3];
		enum_binlog_checksum_alg alg;
		commonMysqlBinlogEventHeader_v4 head;
		formatEvent(const char* buf, uint32_t size);
		formatEvent(uint8_t binlog_ver, const char* server_ver);
		static void createV4FmtEvent(const char* server_ver, char* buf, enum_binlog_checksum_alg alg);
	};
	class RotateEvent
	{
	public:
		commonMysqlBinlogEventHeader_v4 head;
		char fileName[512];
		RotateEvent(const char* buf, size_t size, const formatEvent* fmt);
	};

	class QueryEvent
	{
	public:
#define QUERY_HEADER_MINIMAL_LEN (4 + 4 + 1 + 2)
#define MAX_DBS_IN_EVENT_MTS 16
#define MAX_SIZE_LOG_EVENT_STATUS (1U + 4          /* type, flags2 */   + \
                                     1U + 8          /* type, sql_mode */ + \
                                     1U + 1 + 255    /* type, length, catalog */ + \
                                     1U + 4          /* type, auto_increment */ + \
                                     1U + 6          /* type, charset */ + \
                                     1U + 1 + 255    /* type, length, time_zone */ + \
                                     1U + 2          /* type, lc_time_names_number */ + \
                                     1U + 2          /* type, charset_database_number */ + \
                                     1U + 8          /* type, table_map_for_update */ + \
                                     1U + 4          /* type, master_data_written */ + \
                                                   /* type, db_1, db_2, ... */  \
                                   1U + (MAX_DBS_IN_EVENT_MTS * (1 + NAME_LEN)) + \
                                   3U +            /* type, microseconds */ + \
                                   1U + 32*3 + 1 + 60 \
                                   /* type, user_len, user, host_len, host */)
		/**
	 * 	 When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS
	 * 	 	 the value of OVER_MAX_DBS_IN_EVENT_MTS is is put into the
	 * 	 	 	 mts_accessed_dbs status.
	 * 	 	 	 	 */
#define OVER_MAX_DBS_IN_EVENT_MTS 254
		enum Query_event_post_header_offset
		{
			Q_THREAD_ID_OFFSET = 0,
			Q_EXEC_TIME_OFFSET = 4,
			Q_DB_LEN_OFFSET = 8,
			Q_ERR_CODE_OFFSET = 9,
			Q_STATUS_VARS_LEN_OFFSET = 11,
			Q_DATA_OFFSET = 13
		};
		enum Query_event_status_vars
		{
			Q_FLAGS2_CODE = 0,
			Q_SQL_MODE_CODE,
			/*
	 * 		 Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
	 * 		 		 5.0.x where 0<=x<=3. We have to keep it to be able to replicate these
	 * 		 		 		 old masters.
	 * 		 		 		 		 */
			Q_CATALOG_CODE,
			Q_AUTO_INCREMENT,
			Q_CHARSET_CODE,
			Q_TIME_ZONE_CODE,
			/*
	 * 		 Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
	 * 		 		 5.0.x where x>=4. Saves one byte in every Query_event in binlog,
	 * 		 		 		 compared to Q_CATALOG_CODE. The reason we didn't simply re-use
	 * 		 		 		 		 Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4)
	 * 		 		 		 		 		 master would crash (segfault etc) because it would expect a 0 when there
	 * 		 		 		 		 		 		 is none.
	 * 		 		 		 		 		 		 		 */
			Q_CATALOG_NZ_CODE,
			Q_LC_TIME_NAMES_CODE,
			Q_CHARSET_DATABASE_CODE,
			Q_TABLE_MAP_FOR_UPDATE_CODE,
			Q_MASTER_DATA_WRITTEN_CODE,
			Q_INVOKER,
			/*
	 * 		 Q_UPDATED_DB_NAMES status variable collects information of accessed
	 * 		 		 databases i.e. the total number and the names to be propagated to the
	 * 		 		 		 slave in order to facilitate the parallel applying of the Query events.
	 * 		 		 		 		 */
			Q_UPDATED_DB_NAMES,
			Q_MICROSECONDS,
			/*
	 * 		 A old (unused now) code for Query_log_event status similar to G_COMMIT_TS.
	 * 		 		 */
			Q_COMMIT_TS,
			/*
	 * 		 A code for Query_log_event status, similar to G_COMMIT_TS2.
	 * 		 		 */
			Q_COMMIT_TS2,
			/*
			 *The master connection @@session.explicit_defaults_for_timestamp which
			*is recorded for queries, CREATE and ALTER table that is defined with
			 * a TIMESTAMP column, that are dependent on that feature.
			* For pre-WL6292 master's the associated with this code value is zero.
			* 		 		 		 		 		 */
			Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP
		};
		commonMysqlBinlogEventHeader_v4 head;
		std::string query;
		std::string db;
		uint32_t thread_id;
		uint64_t sql_mode;
		uint32_t query_exec_time;
		uint16_t error_code;
		uint16_t status_vars_len;
		bool flags2_inited;
		uint32_t flags2;
		bool sql_mode_inited;
		size_t catalog_len;
		const char* catalog;
		uint16_t auto_increment_increment, auto_increment_offset;
		bool charset_inited;
		char charset[6];
		size_t time_zone_len; /* 0 means uninited */
		const char* time_zone_str;
		uint16_t lc_time_names_number; /* 0 means en_US */
		uint16_t charset_database_number;
		uint32_t tv_usec;
		size_t user_len;
		const char* user;
		size_t host_len;
		const char* host;
		unsigned char mts_accessed_dbs;
		char mts_accessed_db_names[MAX_DBS_IN_EVENT_MTS][64 * 3];
		/*
		* The following member gets set to OFF or ON value when the
		* Query-log-event is marked as dependent on
		* @@explicit_defaults_for_timestamp. That is the member is relevant
		* to queries that declare TIMESTAMP column attribute, like CREATE
		* and ALTER.
		* The value is set to @c TERNARY_OFF when @@explicit_defaults_for_timestamp
		* encoded value is zero, otherwise TERNARY_ON.
		* 	 	 	 	 	 	 	 	 */
		enum enum_ternary
		{
			TERNARY_UNSET, TERNARY_OFF, TERNARY_ON
		} explicit_defaults_ts;
		/*
		 * map for tables that will be updated for a multi-table update query
		 * statement, for other query statements, this will be zero.
		 * */
		uint64_t table_map_for_update;
		size_t master_data_written;
		QueryEvent(const char* buf, size_t size, const formatEvent* fmt);
		static int getQuery(const char* data, size_t size,
			const formatEvent* fmt, const char*& query, uint32_t& querySize); //for parser check if query is BEGIN,COMMIT  fastly

	};
	/**
   Get the length of next field.
   Change parameter to point at fieldstart.

   @param  packet pointer to a buffer containing the field in a row.
   @return pos    length of the next field
   */
	static inline unsigned long get_field_length(unsigned char** packet)
	{
		unsigned char* pos = *packet;
		uint32_t temp = 0;
		if (*pos < 251)
		{
			(*packet)++;
			return *pos;
		}
		if (*pos == 251)
		{
			(*packet)++;
			return ((unsigned long)~0); //NULL_LENGTH;
		}
		if (*pos == 252)
		{
			(*packet) += 3;
			memcpy(&temp, pos + 1, 2);
			temp = le32toh(temp);
			return (unsigned long)temp;
		}
		if (*pos == 253)
		{
			(*packet) += 4;
			memcpy(&temp, pos + 1, 3);
			temp = le32toh(temp);
			return (unsigned long)temp;
		}
		(*packet) += 9; /* Must be 254 when here */
		memcpy(&temp, pos + 1, 4);
		temp = le32toh(temp);
		return (unsigned long)temp;
	}
	struct tableMap
	{
		uint64_t tableID;
		const char* dbName;
		const char* tableName;
		uint32_t columnCount;
		const uint8_t* types;
		const uint8_t* metaInfo;
		uint32_t metaInfoSize;
		const char* metaData;
		inline void init(
			const formatEvent* description_event,
			const char* metaData_, size_t size)
		{
			assert(size > 16);
			tableID = 0;
			metaData = metaData_ + description_event->common_header_len;
			const char* pos = metaData;
			if (description_event->post_header_len[TABLE_MAP_EVENT - 1]
				== 6)
			{
				tableID = uint4korr(metaData);
				pos += 4 + 2; //4byte tableID+2byte flag
			}
			else
			{
				tableID = uint6korr(metaData);
				pos += 6 + 2; //6byte tableID+2byte flag
			}

			dbName = pos + 1;
			pos += 1 + 1 + (uint8_t)pos[0]; //1byte dbName length + dbName length +1 byte '/0' end of string
			tableName = pos + 1;
			pos += 1 + 1 + (uint8_t)pos[0]; //1byte tableName length + tableName length +1 byte '/0' end of string
			columnCount = get_field_length((uint8_t * *)& pos);
			types = (const uint8_t*)pos;
			pos += columnCount;
			if (pos - metaData_ < size)
			{
				metaInfoSize = get_field_length((uint8_t * *)& pos);
				assert(metaInfoSize <= columnCount * sizeof(uint16_t));
				metaInfo = (uint8_t*)pos;
			}
			else
			{
				metaInfo = NULL;
			}
		}
		tableMap() {}
		tableMap(const formatEvent* description_event,
			const char* metaData_, size_t size)
		{
			init(description_event, metaData_, size);
		}
	};
#define NULL_LENGTH ((unsigned long)~0) /**< For ::net_store_length() */
	/* Get the length of next field. Change parameter to point at fieldstart */
	static inline uint32_t  net_field_length(uint8_t** packet) {
		const uint8_t* pos = *packet;
		if (*pos < 251) {
			(*packet)++;
			return (uint32_t)* pos;
		}
		if (*pos == 251) {
			(*packet)++;
			return NULL_LENGTH;
		}
		if (*pos == 252) {
			(*packet) += 3;
			return (uint32_t)uint2korr(pos + 1);
		}
		if (*pos == 253) {
			(*packet) += 4;
			return (uint32_t)uint3korr(pos + 1);
		}
		(*packet) += 9; /* Must be 254 when here */
		return (uint32_t)uint4korr(pos + 1);
	}

}
#endif /* BINARYLOGEVENT_H_ */


