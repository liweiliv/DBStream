#pragma once
#include <stdint.h>
#include <glog/logging.h>
#include "util/status.h"
#include "util/String.h"
#include "util/hex.h"
#include "util/sparsepp/spp.h"
#include "dataSource/localLogFileCache/logEntry.h"
#include "mysql_com.h"
namespace DATA_SOURCE
{
	constexpr static int UUID_STR_SIZE = 36;
#pragma pack(1)
	struct ServerUUID
	{
		union {
			struct
			{
				uint32_t a;
				uint16_t b;
				uint16_t c;
			};
			uint64_t up;
		};
		union {
			struct
			{
				uint16_t d;
				uint64_t e : 48;
			};
			uint64_t low;
		};
		std::string toString() const
		{
			String s;
			s.appendHex(a).append("-").appendHex(b).append("-").appendHex(c).append("-").appendHex(d).append("-").appendHex(e, 6);
			return s;
		}


		DS fromString(const char* uuid)
		{
			const char* start = uuid;
			const char* pos = strchr(uuid, '-');
			if (pos == nullptr || pos - start != 2 * sizeof(a))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);
			if (!hex::hex2Int(start, 2 * sizeof(a), a))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);

			start = pos + 1;
			if ((pos = strchr(start, '-')) == nullptr || pos - start != 2 * sizeof(b))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);
			if (!hex::hex2Int(start, 2 * sizeof(b), b))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);

			start = pos + 1;
			if ((pos = strchr(start, '-')) == nullptr || pos - start != 2 * sizeof(c))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);
			if (!hex::hex2Int(start, 2 * sizeof(c), c))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);

			start = pos + 1;
			if ((pos = strchr(start, '-')) == nullptr || pos - start != 2 * sizeof(d))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);
			if (!hex::hex2Int(start, 2 * sizeof(d), d))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);

			start = pos + 1;
			uint64_t _e;
			if (strlen(start) < 6 * 2 || !hex::hex2Int(start, 6 * 2, _e))
				dsFailedAndLogIt(1, "uuid " << uuid << " is illegal", ERROR);
			dsOk();
		}
		ServerUUID() :up(0), low(0) {}
		ServerUUID(const ServerUUID& uuid) :up(uuid.up), low(uuid.low) {}
		ServerUUID& operator =(const ServerUUID& uuid)
		{
			this->up = uuid.up;
			this->low = uuid.low;
			return *this;
		}
	};


	struct GtidInfo
	{
		ServerUUID serverid;
		uint64_t groupNumber;
		GtidInfo() : groupNumber(0) {}
		GtidInfo(const GtidInfo& gtid) :serverid(gtid.serverid), groupNumber(gtid.groupNumber) {}
		GtidInfo& operator =(const GtidInfo& gtid)
		{
			serverid = gtid.serverid;
			groupNumber = gtid.groupNumber;
			return *this;
		}
		std::string toString() const
		{
			String str = serverid.toString();
			str.append(":").append(groupNumber);
			return str;
		}
	};

	struct gtidInfoHash
	{
		inline size_t operator()(const ServerUUID& uuid)
		{
			return uuid.up ^ uuid.low;
		}
		inline bool operator()(const ServerUUID& src, const ServerUUID& dest)
		{
			return src.up == dest.up && src.low == dest.low;
		}
	};

	typedef spp::sparse_hash_map<ServerUUID, GtidInfo, gtidInfoHash, gtidInfoHash> gtidSet;
	class GtidSet
	{
	private:
		gtidSet m_gtids;
	public:
		DS getGtidFromCheckpoint(const RPC::Checkpoint* ckp)
		{
			m_gtids.clear();
			if (ckp->externSize == 0)
				dsOk();
			if ((ckp->externSize - sizeof(uint16_t)) % sizeof(GtidInfo) != 0)
				dsFailedAndLogIt(1, "gtid size is illegal", ERROR);
			uint16_t gtidServerCount = (ckp->externSize - sizeof(uint16_t)) / sizeof(GtidInfo);
			for (uint16_t i = 0; i < gtidServerCount; i++)
			{
				GtidInfo& gtid = ((GtidInfo*)ckp->externInfo)[i];
				m_gtids.insert(std::pair<ServerUUID, GtidInfo>(gtid.serverid, gtid));
			}
			dsOk();
		}

		inline void addGtid(const GtidInfo& gtid)
		{
			auto& iter = m_gtids.find(gtid.serverid);
			if (iter == m_gtids.end())
				m_gtids.insert(std::pair<ServerUUID, GtidInfo>(gtid.serverid, gtid));
			else
				iter->second.groupNumber = gtid.groupNumber;
		}

		inline void toCheckpoint(RPC::Checkpoint &ckp)
		{
			ckp.externSize = sizeof(GtidInfo) * m_gtids.size();
			GtidInfo* p = (GtidInfo*)&ckp.externInfo[0];
			for (auto& iter : m_gtids)
				*p++ = iter.second;
		}

		inline GtidInfo* getGtid(const ServerUUID& uuid)
		{
			auto& iter = m_gtids.find(uuid);
			if (iter == m_gtids.end())
				return nullptr;
			return &iter->second;
		}
	};




	static std::string gtidSetToString(const gtidSet& gtids)
	{
		std::string str;
		bool isFirst = true;
		for (auto& iter : gtids)
		{
			if (isFirst)
				isFirst = false;
			else
				str.append(",");
			str.append(iter.second.toString());
		}
		return str;
	}

	constexpr static int LOGICAL_TIMESTAMP_TYPECODE = 2;
	constexpr static int ENCODED_COMMIT_TIMESTAMP_LENGTH = 55;
	constexpr static int IMMEDIATE_SERVER_VERSION_LENGTH = 4;
	constexpr static int LT_TTYPE_OFFSET = 25;
	constexpr static int IMD_TIME_OFFSET = 48;
	constexpr static int IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7;
	constexpr static int ORIGINAL_COMMIT_TIMESTAMP_LENGTH = 7;
	constexpr static int ENCODED_SERVER_VERSION_LENGTH = 31;
	constexpr static int ORIGINAL_SERVER_VERSION_LENGTH = 4;

	struct gtidEvent
	{
		uint8_t flag;
		GtidInfo gtid;
		uint8_t ltType;
		uint64_t lastCommit;
		uint64_t sequenceNumber;
		uint64_t immediateCommitTimestamp;
		uint64_t originalCommitTimestamp;
		uint64_t transactionLength;
		uint32_t immediateServerVersion;
		uint32_t originalServerVersion;
		static inline const GtidInfo& getGtidFromEvent(const char* buf)
		{
			return ((const gtidEvent*)buf)->gtid;
		}
		DS parse(const char* buf, uint32_t size)
		{
			memset(this, 0, sizeof(gtidEvent));
			if (size <= LT_TTYPE_OFFSET)
			{
				memcpy(&flag, buf, size);
				dsOk();
			}
			uint8_t offset;
			memcpy(&flag, buf, IMD_TIME_OFFSET);
			if (size - IMD_TIME_OFFSET >= IMMEDIATE_COMMIT_TIMESTAMP_LENGTH)
			{
				memcpy(&immediateCommitTimestamp, buf + IMD_TIME_OFFSET, IMMEDIATE_COMMIT_TIMESTAMP_LENGTH);
				if ((immediateCommitTimestamp & (1ULL << ENCODED_COMMIT_TIMESTAMP_LENGTH)) != 0)
				{
					immediateCommitTimestamp &= ~(1ULL << ENCODED_COMMIT_TIMESTAMP_LENGTH);
					memcpy(&originalCommitTimestamp, buf + IMD_TIME_OFFSET + IMMEDIATE_COMMIT_TIMESTAMP_LENGTH, ORIGINAL_COMMIT_TIMESTAMP_LENGTH);
					offset = IMD_TIME_OFFSET + IMMEDIATE_COMMIT_TIMESTAMP_LENGTH, ORIGINAL_COMMIT_TIMESTAMP_LENGTH;
				}
				else
				{
					offset = IMD_TIME_OFFSET + IMMEDIATE_COMMIT_TIMESTAMP_LENGTH;
					originalCommitTimestamp = immediateCommitTimestamp;
				}
			}
			if (size == offset)
				dsOk();
			unsigned char* pos = (unsigned char*)buf + offset;
			transactionLength = net_field_length_ll(&pos);
			if (size - IMMEDIATE_SERVER_VERSION_LENGTH >= (const char*)pos - buf)
			{
				immediateServerVersion = *(uint32_t*)pos;
				if ((immediateServerVersion & (1ULL << ENCODED_SERVER_VERSION_LENGTH)) != 0)
				{
					immediateServerVersion &= ~(1ULL << ENCODED_SERVER_VERSION_LENGTH);  // Clear MSB
					originalServerVersion = *(uint32_t*)(pos + IMMEDIATE_SERVER_VERSION_LENGTH);
				}
				else
				{
					originalServerVersion = immediateServerVersion;
				}
			}
			dsOk();
		}
	};

	struct gnoPair
	{
		uint64_t start;
		uint64_t end;
	};

	struct serverGtidSets
	{
		ServerUUID uuid;
		uint64_t intervNum;
		gnoPair gnoList[1];
		inline uint32_t length()
		{
			return sizeof(uuid) + sizeof(intervNum) + intervNum * sizeof(gnoPair);
		}
	};

	constexpr static uint32_t DEFAULT_GTID_BUF_SIZE = 256;
	struct previousGtidsEvent
	{
		uint64_t serverCount;
		serverGtidSets** gtids;
		serverGtidSets* defaultGtidBuf[DEFAULT_GTID_BUF_SIZE];
		previousGtidsEvent() :serverCount(0), gtids(nullptr)
		{
			memset(defaultGtidBuf, 0, sizeof(defaultGtidBuf));
		}
		previousGtidsEvent(const char* buf, uint32_t size)
		{
			parse(buf, size);
		}
		~previousGtidsEvent()
		{
			if (gtids != nullptr && gtids != defaultGtidBuf)
				delete[]gtids;
		}
		DS parse(const char* buf, uint32_t size)
		{
			serverCount = *(uint64_t*)(buf);
			if (serverCount > DEFAULT_GTID_BUF_SIZE)
				gtids = new serverGtidSets * [serverCount];
			else
				gtids = defaultGtidBuf;
			const char* pos = buf + sizeof(uint64_t);
			for (uint64_t i = 0; i < serverCount; i++)
			{
				if (pos - buf >= size)
					dsFailedAndLogIt(1, "illegal previousGtidsEvent", ERROR);
				gtids[i] = (serverGtidSets*)pos;
				pos += gtids[i]->length();
			}
			dsOk();
		}
	};
#pragma pack()
}