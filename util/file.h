#pragma once
#include <string>
struct fileInfo
{
	std::string fileName;
	int64_t size;
	int64_t timestamp;
	fileInfo() :size(0), timestamp(0)
	{}
	fileInfo(const fileInfo& file) :fileName(file.fileName), size(file.size), timestamp(file.timestamp)
	{}
	fileInfo& operator=(const fileInfo& file)
	{
		fileName = file.fileName;
		size = file.size;
		timestamp = file.timestamp;
		return *this;
	}
};
#ifdef OS_WIN
#include "fileOptWindows.h"
static inline const char* basename(const char* path)
{
	const char* backSlant = strchr(path, '\\');
	if (backSlant == nullptr)
		return path;
	for (const char* nextBackSlant = strchr(backSlant + 1, '\\'); nextBackSlant != nullptr; nextBackSlant = strchr(backSlant + 1, '\\'))
		backSlant = nextBackSlant;
	return backSlant + 1;
}
#endif
#if defined OS_LINUX
#include "fileOptUnix.h"
#endif 
static inline uint64_t getFileId(const char* file)
{
	uint64_t id = 0;
	const char* ptr = file;
	const char* dot = nullptr;
	while (*ptr != '\0')
	{
		if (*ptr == '.')
			dot = ptr;
		ptr++;
	}
	if (dot == nullptr)
		ptr = file;
	else
		ptr = dot + 1;
	while (*ptr != '\0')
	{
		if (*ptr >= '0' && *ptr <= '9')
			id = id * 10 + *ptr - '0';
		else
			break;
	}
	return id;
}
