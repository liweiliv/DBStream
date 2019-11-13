/*
 *  * BinaryLogEvent.cpp
 *   *
 *    *  Created on: 2018年7月2日
 *     *      Author: liwei
 *      */
#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "mysql.h" 
#include "BinaryLogEvent.h"
namespace DATA_SOURCE
{
	static uint8_t server_event_header_length[] =
	{ START_V3_HEADER_LEN, QUERY_HEADER_LEN, STOP_HEADER_LEN, ROTATE_HEADER_LEN,
			INTVAR_HEADER_LEN, LOAD_HEADER_LEN,
		/*
		 *               Unused because the code for Slave log event was removed.
		 *                               (15th Oct. 2010)
		 *                                               */
		0, CREATE_FILE_HEADER_LEN, APPEND_BLOCK_HEADER_LEN,
		EXEC_LOAD_HEADER_LEN, DELETE_FILE_HEADER_LEN, NEW_LOAD_HEADER_LEN,
		RAND_HEADER_LEN, USER_VAR_HEADER_LEN, FORMAT_DESCRIPTION_HEADER_LEN,
		XID_HEADER_LEN, BEGIN_LOAD_QUERY_HEADER_LEN,
		EXECUTE_LOAD_QUERY_HEADER_LEN, TABLE_MAP_HEADER_LEN,
		/*
		 *               The PRE_GA events are never be written to any binlog, but
		 *                               their lengths are included in Format_description_log_event.
		 *                                               Hence, we need to be assign some value here, to avoid reading
		 *                                                               uninitialized memory when the array is written to disk.
		 *                                                                               */
		0, /* PRE_GA_WRITE_ROWS_EVENT */
		0, /* PRE_GA_UPDATE_ROWS_EVENT*/
		0, /* PRE_GA_DELETE_ROWS_EVENT*/
		ROWS_HEADER_LEN_V1, /* WRITE_ROWS_EVENT_V1*/
		ROWS_HEADER_LEN_V1, /* UPDATE_ROWS_EVENT_V1*/
		ROWS_HEADER_LEN_V1, /* DELETE_ROWS_EVENT_V1*/
		INCIDENT_HEADER_LEN, 0, /* HEARTBEAT_LOG_EVENT*/
		IGNORABLE_HEADER_LEN, IGNORABLE_HEADER_LEN, ROWS_HEADER_LEN_V2,
		ROWS_HEADER_LEN_V2, ROWS_HEADER_LEN_V2, 42, /*GTID_EVENT*/
		42, /*ANONYMOUS_GTID_EVENT*/
		IGNORABLE_HEADER_LEN, TRANSACTION_CONTEXT_HEADER_LEN,
		VIEW_CHANGE_HEADER_LEN, XA_PREPARE_HEADER_LEN };
#pragma pack(push,1)

	struct fmtPayloadV4
	{
		uint16_t binlogversion;
		char serverVersion[50];
		uint32_t createTimestamp;
		uint8_t eventHeadLength;
		uint8_t eventHeaderLength[sizeof(server_event_header_length)];
		uint8_t checksumAlg;
		uint32_t checksum;
	};
#pragma pack(pop)
	const static unsigned char checksum_version_split[3] =
	{ 5, 6, 1 };
	const static unsigned long checksum_version_product = (checksum_version_split[0]
		* 256 + checksum_version_split[1]) * 256 + checksum_version_split[2];
	/**
	 Splits server 'version' string into three numeric pieces stored
	 into 'split_versions':
	 X.Y.Zabc (X,Y,Z numbers, a not a digit) -> {X,Y,Z}
	 X.Yabc -> {X,Y,0}

	 @param version        String representing server version
	 @param split_versions Array with each element containing one split of the
	 input version string
	 */
	static inline void do_server_version_split(const char* version,
		unsigned char split_versions[3])
	{
		const char* p = version;
		char* r;
		unsigned long number;
		for (unsigned int i = 0; i <= 2; i++)
		{
			number = strtoul(p, &r, 10);
			/*
			 * 		 It is an invalid version if any version number greater than 255 or
			 * 		 		 first number is not followed by '.'.
			 * 		 		 		 */
			if (number < 256 && (*r == '.' || i != 0))
				split_versions[i] = static_cast<unsigned char>(number);
			else
			{
				split_versions[0] = 0;
				split_versions[1] = 0;
				split_versions[2] = 0;
				break;
			}

			p = r;
			if (*r == '.')
				p++; // skip the dot
		}
	}
	/**
	 *  Calculate the version product from the numeric pieces representing the server
	 *   version:
	 *    For a server version X.Y.Zabc (X,Y,Z numbers, a not a digit), the input is
	 *     {X,Y,Z}. This is converted to XYZ in bit representation.
	 *
	 *      @param  version_split Array containing the version information of the server
	 *       @return               The version product of the server
	 *        */
	static inline unsigned long version_product(const unsigned char* version_split)
	{
		return ((version_split[0] * 256 + version_split[1]) * 256 + version_split[2]);
	}
	/**
	 *  The method returns the checksum algorithm used to checksum the binary log.
	 *   For MySQL server versions < 5.6, the algorithm is undefined. For the higher
	 *    versions, the type is decoded from the FORMAT_DESCRIPTION_EVENT.
	 *
	 *     @param buf buffer holding serialized FD event
	 *      @param len netto (possible checksum is stripped off) length of the event buf
	 *
	 *       @return  the version-safe checksum alg descriptor where zero
	 *        designates no checksum, 255 - the orginator is
	 *         checksum-unaware (effectively no checksum) and the actuall
	 *          [1-254] range alg descriptor.
	 *           */
	static enum_binlog_checksum_alg get_checksum_alg(const char* buf,
		unsigned long len)
	{
		enum_binlog_checksum_alg ret;
		char version[ST_SERVER_VER_LEN];
		unsigned char version_split[3];
		assert(buf[EVENT_TYPE_OFFSET] == FORMAT_DESCRIPTION_EVENT);
		memcpy(version,
			buf
			+ buf[LOG_EVENT_MINIMAL_HEADER_LEN
			+ ST_COMMON_HEADER_LEN_OFFSET]
			+ ST_SERVER_VER_OFFSET, ST_SERVER_VER_LEN);
		version[ST_SERVER_VER_LEN - 1] = 0;

		do_server_version_split(version, version_split);
		if (version_product(version_split) < checksum_version_product)
			ret = BINLOG_CHECKSUM_ALG_UNDEF;
		else
			ret = static_cast<enum_binlog_checksum_alg>(*(buf + len -
				BINLOG_CHECKSUM_LEN -
				BINLOG_CHECKSUM_ALG_DESC_LEN));
		assert(
			ret == BINLOG_CHECKSUM_ALG_OFF || ret == BINLOG_CHECKSUM_ALG_UNDEF
			|| ret == BINLOG_CHECKSUM_ALG_CRC32);
		return ret;
	}

	formatEvent::formatEvent(const char* buf, uint32_t size)
	{
		number_of_event_types = 0;
		assert(size > LOG_EVENT_MINIMAL_HEADER_LEN);
		memcpy(&head, buf, sizeof(head));
		alg = get_checksum_alg(buf, size);
		if (alg != BINLOG_CHECKSUM_ALG_UNDEF)
			size = size - BINLOG_CHECKSUM_LEN;
		memcpy(server_version,
			buf
			+ buf[LOG_EVENT_MINIMAL_HEADER_LEN
			+ ST_COMMON_HEADER_LEN_OFFSET]
			+ ST_SERVER_VER_OFFSET, ST_SERVER_VER_LEN);
		server_version[ST_SERVER_VER_LEN - 1] = 0;
		do_server_version_split(server_version, server_version_split);
		memset(post_header_len, 0, sizeof(post_header_len));
		assert(size >= LOG_EVENT_MINIMAL_HEADER_LEN);
		const char* data = buf + LOG_EVENT_MINIMAL_HEADER_LEN;
		if ((common_header_len = data[ST_COMMON_HEADER_LEN_OFFSET]) < OLD_HEADER_LEN)
		{
			return; /* sanity check */
		}
		number_of_event_types = size
			- (LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_HEADER_LEN_OFFSET + 1);
		if (version_product(server_version_split) >= checksum_version_product)
		{
			number_of_event_types -= BINLOG_CHECKSUM_ALG_DESC_LEN;
			assert(
				version_product(server_version_split)
				!= checksum_version_product
				|| number_of_event_types == ENUM_END_EVENT - 1);
		}
		memcpy(post_header_len, data + ST_COMMON_HEADER_LEN_OFFSET + 1,
			number_of_event_types);

		/*
		 * 	 In some previous versions, the events were given other event type
		 * 	 id numbers than in the present version. When replicating from such
		 * 	 a version, we therefore set up an array that maps those id numbers
		 * 	 to the id numbers of the present server.
		 * 	 If post_header_len is null, it means malloc failed, and in the mysql-server
		 * 	 code the variable *is_valid* will be set to false, so there is no need to do
		 * 	 anything.
		 *  	 The trees in which events have wrong id's are:
		 * 	 mysql-5.1-wl1012.old mysql-5.1-wl2325-5.0-drop6p13-alpha
		 * 	 mysql-5.1-wl2325-5.0-drop6 mysql-5.1-wl2325-5.0
		 * 	 mysql-5.1-wl2325-no-dd
		 * 	 	 	 	 	 	 	 	 	 	 	 	 (this was found by grepping for two lines in sequence where the
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 first matches "FORMAT_DESCRIPTION_EVENT," and the second matches
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 "TABLE_MAP_EVENT," in log_event.h in all trees)
		 *
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 In these trees, the following server_versions existed since
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 TABLE_MAP_EVENT was introduced:
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.1-a_drop5p3   5.1.1-a_drop5p4        5.1.1-alpha
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p10  5.1.2-a_drop5p11       5.1.2-a_drop5p12
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p13  5.1.2-a_drop5p14       5.1.2-a_drop5p15
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p16  5.1.2-a_drop5p16b      5.1.2-a_drop5p16c
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p17  5.1.2-a_drop5p4        5.1.2-a_drop5p5
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p6   5.1.2-a_drop5p7        5.1.2-a_drop5p8
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.2-a_drop5p9   5.1.3-a_drop5p17       5.1.3-a_drop5p17b
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.3-a_drop5p17c 5.1.4-a_drop5p18       5.1.4-a_drop5p19
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.4-a_drop5p20  5.1.4-a_drop6p0        5.1.4-a_drop6p1
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.1.4-a_drop6p2   5.1.5-a_drop5p20       5.2.0-a_drop6p3
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.2.0-a_drop6p4   5.2.0-a_drop6p5        5.2.0-a_drop6p6
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.2.1-a_drop6p10  5.2.1-a_drop6p11       5.2.1-a_drop6p12
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.2.1-a_drop6p6   5.2.1-a_drop6p7        5.2.1-a_drop6p8
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.2.2-a_drop6p13  5.2.2-a_drop6p13-alpha 5.2.2-a_drop6p13b
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5.2.2-a_drop6p13c
		 *
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 (this was found by grepping for "mysql," in all historical
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 versions of configure.in in the trees listed above).
		 *
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 There are 5.1.1-alpha versions that use the new event id's, so we
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 do not test that version string.  So replication from 5.1.1-alpha
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 with the other event id's to a new version does not work.
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 Moreover, we can safely ignore the part after drop[56].  This
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 allows us to simplify the big list above to the following regexes:
		 *
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5\.1\.[1-5]-a_drop5.*
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5\.1\.4-a_drop6.*
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 5\.2\.[0-2]-a_drop6.*
		 *
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 This is what we test for in the 'if' below.
		 * 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 */
		if ((!(number_of_event_types > 0)) && server_version[0] == '5'
			&& server_version[1] == '.' && server_version[3] == '.'
			&& strncmp(server_version + 5, "-a_drop", 7) == 0
			&& ((server_version[2] == '1' && server_version[4] >= '1'
				&& server_version[4] <= '5' && server_version[12] == '5')
				|| (server_version[2] == '1' && server_version[4] == '4'
					&& server_version[12] == '6')
				|| (server_version[2] == '2' && server_version[4] >= '0'
					&& server_version[4] <= '2'
					&& server_version[12] == '6')))
		{
			/* 22= No of events used in the mysql version mentioned above in the comments*/
			if (number_of_event_types != 22)
			{
				number_of_event_types = 0;
				memset(post_header_len, 0, sizeof(post_header_len));
				return;
			}
			static const uint8_t perm[EVENT_TYPE_PERMUTATION_NUM] =
			{ UNKNOWN_EVENT, START_EVENT_V3, QUERY_EVENT, STOP_EVENT, ROTATE_EVENT,
					INTVAR_EVENT, LOAD_EVENT, SLAVE_EVENT, CREATE_FILE_EVENT,
					APPEND_BLOCK_EVENT, EXEC_LOAD_EVENT, DELETE_FILE_EVENT,
					NEW_LOAD_EVENT, RAND_EVENT, USER_VAR_EVENT,
					FORMAT_DESCRIPTION_EVENT, TABLE_MAP_EVENT,
					PRE_GA_WRITE_ROWS_EVENT, PRE_GA_UPDATE_ROWS_EVENT,
					PRE_GA_DELETE_ROWS_EVENT, XID_EVENT, BEGIN_LOAD_QUERY_EVENT,
					EXECUTE_LOAD_QUERY_EVENT, };
			/*
			 * 		 Since we use (permuted) event id's to index the post_header_len
			 * 		 		 array, we need to permute the post_header_len array too.
			 * 		 		 		 */
			uint8_t post_header_len_temp[EVENT_TYPE_PERMUTATION_NUM];
			for (unsigned int i = 1; i < EVENT_TYPE_PERMUTATION_NUM; i++)
				post_header_len_temp[perm[i] - 1] = post_header_len[i - 1];
			for (unsigned int i = 0; i < EVENT_TYPE_PERMUTATION_NUM - 1; i++)
				post_header_len[i] = post_header_len_temp[i];
		}
	}
	void formatEvent::createV4FmtEvent(const char* server_ver, char* buf,
		enum_binlog_checksum_alg alg)
	{
		commonMysqlBinlogEventHeader_v4* _head =
			(commonMysqlBinlogEventHeader_v4*)buf;
		_head->eventOffset = 4;
		_head->timestamp = 0;
		_head->serverId = 0;
		_head->type = FORMAT_DESCRIPTION_EVENT;
		_head->flag = 0;
		_head->eventSize = sizeof(commonMysqlBinlogEventHeader_v4)
			+ sizeof(fmtPayloadV4);
		fmtPayloadV4* payLoad = (fmtPayloadV4*)(buf
			+ sizeof(commonMysqlBinlogEventHeader_v4));
		payLoad->binlogversion = 4;
		strncpy(payLoad->serverVersion, server_ver, 49);
		payLoad->eventHeadLength = 19;
		payLoad->createTimestamp = 0;
		memcpy(payLoad->eventHeaderLength, DATA_SOURCE::server_event_header_length,
			sizeof(DATA_SOURCE::server_event_header_length));
		payLoad->checksumAlg = alg;
		payLoad->checksum = 0; //todo;
	}
	formatEvent::formatEvent(uint8_t binlog_ver, const char* server_ver) :
		alg(BINLOG_CHECKSUM_ALG_OFF)
	{
		memset(&head, 0, sizeof(head));
		switch (binlog_ver)
		{
		case 4: /* MySQL 5.0 and above*/
		{
			number_of_event_types = LOG_EVENT_TYPES;
			/**
			 * 		 This will be used to initialze the post_header_len,
			 * 		 		 for binlog version 4.
			 * 		 		 		 */

			 /*
			  * 		 Allows us to sanity-check that all events initialized their
			  * 		 		 events (see the end of this 'if' block).
			  * 		 		 		 */
			memcpy(post_header_len, server_event_header_length,
					number_of_event_types);
			/*
			 * 		 As we are copying from a char * it might be the case at times that some
			 * 		 		 part of the array server_version remains uninitialized so memset will help
			 * 		 		 		 in getting rid of the valgrind errors.
			 * 		 		 		 		 */
			memset(server_version, 0, ST_SERVER_VER_LEN);
			strncpy(server_version, server_ver, ST_SERVER_VER_LEN);
			common_header_len = LOG_EVENT_HEADER_LEN;
			break;
		}
		case 1: /* 3.23 */
		case 3: /* 4.0.x x >= 2 */
		{
			/*
			 * 		 We build an artificial (i.e. not sent by the master) event, which
			 * 		 		 describes what those old master versions send.
			 * 		 		 		 */
			if (binlog_ver == 1)
				strcpy(server_version, server_ver ? server_ver : "3.23");
			else
				strcpy(server_version, server_ver ? server_ver : "4.0");
			common_header_len = binlog_ver == 1 ? OLD_HEADER_LEN :
				LOG_EVENT_MINIMAL_HEADER_LEN;
			/*
			 * 		 The first new event in binlog version 4 is Format_desc. So any event type
			 * 		 		 after that does not exist in older versions. We use the events known by
			 * 		 		 		 version 3, even if version 1 had only a subset of them (this is not a
			 * 		 		 		 		 problem: it uses a few bytes for nothing but unifies code; it does not
			 * 		 		 		 		 		 make the slave detect less corruptions).
			 * 		 		 		 		 		 		 */
			number_of_event_types = FORMAT_DESCRIPTION_EVENT - 1;
			/**
			 * 		 This will be used to initialze the post_header_len, for binlog version
			 * 		 		 1 and 3
			 * 		 		 		 */
			static uint8_t server_event_header_length_ver_1_3[] =
			{ START_V3_HEADER_LEN, QUERY_HEADER_MINIMAL_LEN, STOP_HEADER_LEN,
					uint8_t(binlog_ver == 1 ? 0 : ROTATE_HEADER_LEN),
					INTVAR_HEADER_LEN, LOAD_HEADER_LEN,
				/*
				 * 				 Unused because the code for Slave log event was removed.
				 * 				 				 (15th Oct. 2010)
				 * 				 				 				 */
				0, CREATE_FILE_HEADER_LEN, APPEND_BLOCK_HEADER_LEN,
				EXEC_LOAD_HEADER_LEN, DELETE_FILE_HEADER_LEN,
				NEW_LOAD_HEADER_LEN, RAND_HEADER_LEN, USER_VAR_HEADER_LEN };
			memcpy(post_header_len, server_event_header_length_ver_1_3,
				number_of_event_types);
			break;
		}
		default: /* Includes binlog version 2 i.e. 4.0.x x<=1 */
			/*
			 * 		 Will make the mysql-server variable *is_valid* defined in class Log_event
			 * 		 		 to be set to false.
			 * 		 		 		 */
			break;
		}
	}
	RotateEvent::RotateEvent(const char* buf, size_t size, const formatEvent* fmt)
	{
		if (fmt->alg == BINLOG_CHECKSUM_ALG_CRC32)
			size -= BINLOG_CHECKSUM_LEN;
		size_t header_size = fmt->common_header_len;
		assert(size > LOG_EVENT_MINIMAL_HEADER_LEN);
		memcpy(&head, buf, sizeof(head));
		uint8_t post_header_len = fmt->post_header_len[ROTATE_EVENT - 1];
		unsigned int ident_offset;
		if (size < header_size)
			return;
		int ident_len = size - (header_size + post_header_len);
		ident_offset = post_header_len;
		if (ident_len > 512 - 1)
			ident_len = 512 - 1;

		const char* dot = strchr(buf + LOG_EVENT_MINIMAL_HEADER_LEN + ident_offset,
			'.');
		if (dot == NULL
			|| ident_len
			- (dot - (buf + LOG_EVENT_MINIMAL_HEADER_LEN + ident_offset))
			- 1 != 6)
			abort();
		memcpy(fileName, buf + LOG_EVENT_MINIMAL_HEADER_LEN + ident_offset,
			ident_len);
		fileName[ident_len] = '\0';
	}

	/**
	 *  Macro to check that there is enough space to read from memory.
	 *
	 *   @param PTR Pointer to memory
	 *    @param END End of memory
	 *     @param CNT Number of bytes that should be read.
	 *      */
#define CHECK_SPACE(PTR,END,CNT)                      \
  do {                                                \
    assert((PTR) + (CNT) <= (END));              \
    if ((PTR) + (CNT) > (END)) {                      \
      query.clear();                                       \
      return;                               \
    }                                                 \
  } while (0)

	QueryEvent::QueryEvent(const char* buf, size_t size, const formatEvent* fmt) :
		thread_id(0), sql_mode(0), query_exec_time(0), error_code(0), status_vars_len(
			0), flags2_inited(false), flags2(0), sql_mode_inited(false), catalog_len(
				0), catalog(NULL), auto_increment_increment(0), auto_increment_offset(
					0), charset_inited(false), time_zone_len(0), time_zone_str(
						NULL), lc_time_names_number(0), charset_database_number(0), tv_usec(0), user_len(
							0), user(NULL), host_len(0), host(NULL), mts_accessed_dbs(0)
	{
		/*buf is advanced in Binary_log_event constructor to point to
		 * 	beginning of post-header*/
		if (fmt->alg == BINLOG_CHECKSUM_ALG_CRC32)
			size -= 4;
		uint32_t tmp;
		uint8_t common_header_len, post_header_len;
		uint8_t* start;
		const uint8_t* end;

		uint64_t query_data_written = 0;
		assert(size > LOG_EVENT_MINIMAL_HEADER_LEN);
		memcpy(&head, buf, sizeof(head));

		common_header_len = fmt->common_header_len;
		post_header_len = fmt->post_header_len[QUERY_EVENT - 1];

		/*
		 * 	 We test if the event's length is sensible, and if so we compute data_len.
		 * 	 	 We cannot rely on QUERY_HEADER_LEN here as it would not be format-tolerant.
		 * 	 	 	 We use QUERY_HEADER_MINIMAL_LEN which is the same for 3.23, 4.0 & 5.0.
		 * 	 	 	 	 */
		if (size < (unsigned int)(common_header_len + post_header_len))
			abort();
		buf += common_header_len;
		uint64_t data_len = size - (common_header_len + post_header_len);

		memcpy(&thread_id, buf + Q_THREAD_ID_OFFSET, sizeof(thread_id));
		thread_id = le32toh(thread_id);
		memcpy(&query_exec_time, buf + Q_EXEC_TIME_OFFSET, sizeof(query_exec_time));
		query_exec_time = le32toh(query_exec_time);

		uint8_t db_len = (unsigned char)buf[Q_DB_LEN_OFFSET];
		/* TODO: add a check of all *_len vars*/
		memcpy(&error_code, buf + Q_ERR_CODE_OFFSET, sizeof(error_code));
		error_code = le16toh(error_code);

		/*
		 * 	 5.0 format starts here.
		 * 	 	 Depending on the format, we may or not have affected/warnings etc
		 * 	 	 	 The remnent post-header to be parsed has length:
		 * 	 	 	 	 */
		tmp = post_header_len - QUERY_HEADER_MINIMAL_LEN;
		if (tmp)
		{
			memcpy(&status_vars_len, buf + Q_STATUS_VARS_LEN_OFFSET,
				sizeof(status_vars_len));
			status_vars_len = le16toh(status_vars_len);
			/*
			 * 		 Check if status variable length is corrupt and will lead to very
			 * 		 		 wrong data. We could be even more strict and require data_len to
			 * 		 		 		 be even bigger, but this will suffice to catch most corruption
			 * 		 		 		 		 errors that can lead to a crash.
			 * 		 		 		 		 		 */
			if (status_vars_len > (data_len< MAX_SIZE_LOG_EVENT_STATUS? data_len : MAX_SIZE_LOG_EVENT_STATUS))
			{
				query.clear();
				return;
			}
			data_len -= status_vars_len;
			tmp -= 2;
		}
		else
		{
			/*
			 * 		 server version < 5.0 / binlog_version < 4 master's event is
			 * 		 		 relay-logged with storing the original size of the event in
			 * 		 		 		 Q_MASTER_DATA_WRITTEN_CODE status variable.
			 * 		 		 		 		 The size is to be restored at reading Q_MASTER_DATA_WRITTEN_CODE-marked
			 * 		 		 		 		 		 event from the relay log.
			 * 		 		 		 		 		 		 */
			abort();		  //not support mysql 4.x
		}
		/*
		 * 	 We have parsed everything we know in the post header for QUERY_EVENT,
		 * 	 	 the rest of post header is either comes from older version MySQL or
		 * 	 	 	 dedicated to derived events (e.g. Execute_load_query...)
		 * 	 	 	 	 */

		 /* variable-part: the status vars; only in MySQL 5.0  */
		start = (uint8_t*)(buf + post_header_len);
		end = (const uint8_t*)(start + status_vars_len);
		for (const uint8_t* pos = start; pos < end;)
		{
			switch (*pos++)
			{
			case Q_FLAGS2_CODE:
				CHECK_SPACE(pos, end, 4);
				flags2_inited = 1;
				memcpy(&flags2, pos, sizeof(flags2));
				flags2 = le32toh(flags2);
				pos += 4;
				break;
			case Q_SQL_MODE_CODE:
			{
				CHECK_SPACE(pos, end, 8);
				sql_mode_inited = 1;
				memcpy(&sql_mode, pos, sizeof(sql_mode));
				sql_mode = le64toh(sql_mode);
				pos += 8;
				break;
			}
			case Q_CATALOG_NZ_CODE:
				if ((catalog_len = *pos))
					catalog = (const char*)(pos + 1);
				CHECK_SPACE(pos, end, catalog_len + 1);
				pos += catalog_len + 1;
				break;
			case Q_AUTO_INCREMENT:
				CHECK_SPACE(pos, end, 4);
				memcpy(&auto_increment_increment, pos,
					sizeof(auto_increment_increment));
				auto_increment_increment = le16toh(auto_increment_increment);
				memcpy(&auto_increment_offset, pos + 2,
					sizeof(auto_increment_offset));
				auto_increment_offset = le16toh(auto_increment_offset);
				pos += 4;
				break;
			case Q_CHARSET_CODE:
			{
				CHECK_SPACE(pos, end, 6);
				charset_inited = 1;
				memcpy(charset, pos, 6);
				pos += 6;
				break;
			}
			case Q_TIME_ZONE_CODE:
			{
				if ((time_zone_len = *pos))
					time_zone_str = (const char*)(pos + 1);
				pos += time_zone_len + 1;
				break;
			}
			case Q_CATALOG_CODE: /* for 5.0.x where 0<=x<=3 masters */
				CHECK_SPACE(pos, end, 1);
				if ((catalog_len = *pos))
					catalog = (const char*)(pos + 1);
				CHECK_SPACE(pos, end, catalog_len + 2);
				pos += catalog_len + 2; // leap over end 0
				break;
			case Q_LC_TIME_NAMES_CODE:
				CHECK_SPACE(pos, end, 2);
				memcpy(&lc_time_names_number, pos, sizeof(lc_time_names_number));
				lc_time_names_number = le16toh(lc_time_names_number);
				pos += 2;
				break;
			case Q_CHARSET_DATABASE_CODE:
				CHECK_SPACE(pos, end, 2);
				memcpy(&charset_database_number, pos, sizeof(lc_time_names_number));
				charset_database_number = le16toh(charset_database_number);
				pos += 2;
				break;
			case Q_TABLE_MAP_FOR_UPDATE_CODE:
				CHECK_SPACE(pos, end, 8);
				memcpy(&table_map_for_update, pos, sizeof(table_map_for_update));
				table_map_for_update = le64toh(table_map_for_update);
				pos += 8;
				break;
			case Q_MASTER_DATA_WRITTEN_CODE:
				CHECK_SPACE(pos, end, 4);
				memcpy(&master_data_written, pos, sizeof(master_data_written));
				master_data_written = le32toh(
					static_cast<uint32_t>(master_data_written));
				pos += 4;
				break;
			case Q_MICROSECONDS:
			{
				CHECK_SPACE(pos, end, 3);
				uint32_t temp_usec = 0;
				memcpy(&temp_usec, pos, 3);
				tv_usec = le32toh(temp_usec);
				pos += 3;
				break;
			}
			case Q_INVOKER:
			{
				CHECK_SPACE(pos, end, 1);
				user_len = *pos++;
				CHECK_SPACE(pos, end, user_len);
				user = (const char*)pos;
				if (user_len == 0)
					user = (const char*) "";
				pos += user_len;

				CHECK_SPACE(pos, end, 1);
				host_len = *pos++;
				CHECK_SPACE(pos, end, host_len);
				host = (const char*)pos;
				if (host_len == 0)
					host = (const char*) "";
				pos += host_len;
				break;
			}
			case Q_UPDATED_DB_NAMES:
			{
				unsigned char i = 0;
				CHECK_SPACE(pos, end, 1);
				mts_accessed_dbs = *pos++;
				/*
				 * 			 Notice, the following check is positive also in case of
				 * 			 			 the master's MAX_DBS_IN_EVENT_MTS > the slave's one and the event
				 * 			 			 			 contains e.g the master's MAX_DBS_IN_EVENT_MTS db:s.
				 * 			 			 			 			 */
				if (mts_accessed_dbs > MAX_DBS_IN_EVENT_MTS)
				{
					mts_accessed_dbs = OVER_MAX_DBS_IN_EVENT_MTS;
					break;
				}

				assert(mts_accessed_dbs != 0);

				for (i = 0; i < mts_accessed_dbs && pos < start + status_vars_len;
					i++)
				{
					strncpy(mts_accessed_db_names[i], (char*)pos,
						(NAME_LEN<start + status_vars_len - pos? NAME_LEN: start + status_vars_len - pos));
					mts_accessed_db_names[i][NAME_LEN - 1] = 0;
					pos += 1 + strlen((const char*)pos);
				}
				if (i != mts_accessed_dbs)
					return;
				break;
			}
			case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
			{
				CHECK_SPACE(pos, end, 1);
				explicit_defaults_ts = *pos++ == 0 ? TERNARY_OFF : TERNARY_ON;
				break;
			}
			default:
				/* That's why you must write status vars in growing order of code */
				pos = (const unsigned char*)end;         // Break loop
			}
		}
		if (catalog_len)                             // If catalog is given
			query_data_written += catalog_len + 1;
		if (time_zone_len)
			query_data_written += time_zone_len + 1;
		if (user_len > 0)
			query_data_written += user_len + 1;
		if (host_len > 0)
			query_data_written += host_len + 1;

		/*
		 * 	 if time_zone_len or catalog_len are 0, then time_zone and catalog
		 * 	 	 are uninitialized at this point.  shouldn't they point to the
		 * 	 	 	 zero-length null-terminated strings we allocated space for in the
		 * 	 	 	 	 my_alloc call above? /sven
		 * 	 	 	 	 	 */

		 /* A 2nd variable part; this is common to all versions */
		query_data_written += data_len + 1;
		db = std::string((const char*)end, db_len);
		query = std::string((const char*)end + db_len + 1, data_len - db_len - 1);
		return;
	}
	int QueryEvent::getQuery(const char* data, size_t size, const formatEvent* fmt,
		const char*& query, uint32_t& querySize) //for parser check if query is BEGIN,COMMIT  fastly
	{
		if (fmt->alg == BINLOG_CHECKSUM_ALG_CRC32)
			size -= 4;
		assert(
			size
					> fmt->common_header_len
			+ fmt->post_header_len[QUERY_EVENT - 1]);
		data += fmt->common_header_len;
		uint8_t post_header_len = fmt->post_header_len[QUERY_EVENT - 1];
		uint16_t status_vars_len = 0;
		if (post_header_len - QUERY_HEADER_MINIMAL_LEN > 0)
		{
			memcpy(&status_vars_len, data + Q_STATUS_VARS_LEN_OFFSET,
				sizeof(status_vars_len));
			status_vars_len = le16toh(status_vars_len);
			/*
			 * 		 Check if status variable length is corrupt and will lead to very
			 * 		 		 wrong data. We could be even more strict and require data_len to
			 * 		 		 		 be even bigger, but this will suffice to catch most corruption
			 * 		 		 		 		 errors that can lead to a crash.
			 * 		 		 		 		 		 */
			if (status_vars_len
				> (
					size - (fmt->common_header_len + post_header_len)<MAX_SIZE_LOG_EVENT_STATUS? size - (fmt->common_header_len + post_header_len): MAX_SIZE_LOG_EVENT_STATUS))
			{
				return -1;
			}
		}
		const char* end = (data + fmt->post_header_len[QUERY_EVENT - 1])
			+ status_vars_len;
		query = end + (unsigned char)data[Q_DB_LEN_OFFSET] + 1;
		querySize = size - (query - (data - fmt->common_header_len));
		return 0;
	}
}
