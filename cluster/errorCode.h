#pragma once
namespace CLUSTER
{
	enum errorCode {
		ok,
		ioError,
		netError,
		endOfFile,
		full,
		emptyLogFile,
		prevNotMatch,
		rollback,
		logIndexNotFound,
		illegalLogEntry,
		rollbackCommited,
		rollbackTooEarlier,
		missingLogFile
	};
}