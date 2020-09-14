#pragma once
#include <stdint.h>
namespace CLUSTER {
	constexpr static uint8_t VERSION = 1;
#pragma pack(1)
	struct logIndexInfo {
		uint64_t term;
		uint64_t logIndex;
		inline bool operator<(const logIndexInfo& dest) const
		{
			return term < dest.term || (term == dest.term && logIndex < dest.logIndex);
		}
		inline bool operator<= (const logIndexInfo& dest) const
		{
			return term < dest.term || (term == dest.term && logIndex <= dest.logIndex);
		}
		inline bool operator>(const logIndexInfo& dest)const
		{
			return term > dest.term || (term == dest.term && logIndex > dest.logIndex);
		}
		inline bool operator>=(const logIndexInfo& dest)const
		{
			return term > dest.term || (term == dest.term && logIndex >= dest.logIndex);
		}
		inline bool operator==(const logIndexInfo& dest)const
		{
			return term == dest.term && logIndex == dest.logIndex;
		}
		inline bool operator!=(const logIndexInfo& dest)const
		{
			return term != dest.term || logIndex != dest.logIndex;
		}
		logIndexInfo() :term(0), logIndex(0) {}
		logIndexInfo(const logIndexInfo& index) :term(index.term), logIndex(index.logIndex) {}
		logIndexInfo(uint64_t term, uint64_t logIndex) :term(term), logIndex(logIndex) {}
	};

	struct authReq {
		uint32_t size;
		uint32_t crc;
		int32_t migicNum;
		uint32_t clusterId;
		uint32_t nodeId;
		uint16_t port;
		char host[1];
	};
	struct authRsp {
		uint32_t size;
		uint32_t crc;
		int32_t migicNum;
		int8_t success;
		uint32_t nodeId;
		char msg[1];
	};
	enum class rpcType {
		baseLogEntry,
		logEntryRsp,
		nodeInfoLogEntry,
		processorLogEntry,
		endPointrLogEntry,
		checkpointLogEntry,
		voteReq,
		voteRsq,
		endOfFile
	};
	struct raftRpcHead {
		uint32_t size;
		uint8_t recordType;
		uint8_t version;
	};
	struct logEntryRpcBase :public raftRpcHead {
		logIndexInfo logIndex;
		logIndexInfo prevRecordLogIndex;
		uint64_t leaderCommitIndex;
	};
	struct logEntryResponse :public raftRpcHead {
		uint8_t success;
		uint64_t term;
		uint64_t logIndex;
	};

	struct nodeInfoLogEntry :public logEntryRpcBase
	{
		uint8_t opt;//0 add node,1 update node,2 delete node
		uint8_t reserver;
		uint16_t role;
		uint32_t nodeId;
		uint16_t port;
		char host[1];
	};
	struct processorInfoLogEntry :public logEntryRpcBase
	{
		uint8_t opt;//0 add processor,1 update processor,2 delete processor
		uint8_t processorType;
		uint32_t nodeId;
		uint32_t processorId;
		uint32_t endPointId;
		char conf[1];
	};
	struct endPointLogEntry :public logEntryRpcBase {
		uint8_t opt;//0 add endPoint,1 update endPoint,2 delete endPoint
		uint8_t endPointType;
		uint32_t endPointId;
		char detail[1];//in json foramt
	};
	struct checkpointLogEntry :public logEntryRpcBase
	{
		uint8_t type;//0 processor,1 client
		uint64_t logOffset;
		uint64_t timestamp;
		char gtid[1];
	};
	struct voteRequest :public raftRpcHead {
		uint32_t term;
		uint32_t nodeId;
		logIndexInfo lastLogIndex;
	};
	struct voteResponse : public raftRpcHead {
		uint32_t term;
		uint8_t voteGranted;
	};
#pragma pack()

}
