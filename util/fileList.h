#pragma once
#include <string>
#include <map>
#include <string.h>
#include <vector>
#include <stdint.h>
#include "file.h"
#include "winDll.h"
#include "status.h"
DLL_EXPORT class fileList
{
public:
private:
	std::string dirPath;
	std::string prefix;
	std::map< uint64_t, fileInfo> files;
public:
	DLL_EXPORT fileList(const char* dirPath, const char* prefix) :dirPath(dirPath), prefix(prefix)
	{
	}
	DLL_EXPORT ~fileList()
	{
		clean();
	}
	DLL_EXPORT void clean();
	DLL_EXPORT int load();
	DLL_EXPORT int update();
	DLL_EXPORT const std::map< uint64_t, fileInfo>& get()
	{
		return files;
	}
	DLL_EXPORT static DS getFileList(const std::string &filePath, std::vector<std::string>& files);
};
