#include "fileList.h"
#include "glog/logging.h"
#ifdef OS_WIN
#include <io.h>
#endif
#ifdef OS_LINUX
#include <dirent.h>
#include <unistd.h>
#include <sys/types.h>  
#include <sys/stat.h>  
#endif
DLL_EXPORT void fileList::clean()
{
	files.clear();
}
DLL_EXPORT int fileList::load()
{
#ifdef OS_WIN
	WIN32_FIND_DATA findFileData;
	std::string findString(dirPath);
	findString.append("\\").append(prefix).append(".*");
	HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
	if (INVALID_HANDLE_VALUE == hFind)
	{
		LOG(ERROR) << "open data dir:" << dirPath << " failed,errno:" << errno << "," << strerror(errno);
		return -1;
	}
	do
	{
		if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
			continue;
		const char* fileName = findFileData.cFileName;
#endif
#ifdef OS_LINUX
		DIR* dir = opendir(dirPath.c_str());
		if (dir == nullptr)
		{
			LOG(ERROR) << "open data dir:" << dirPath << " failed,errno:" << errno << "," << strerror(errno);
			return -1;
		}
		dirent* file;
		while ((file = readdir(dir)) != nullptr)
		{
			if (file->d_type != 8)
				continue;
			const char* fileName = file->d_name;
#endif
			if (strncmp(fileName, prefix.c_str(), prefix.size()) != 0)
				continue;
			if (fileName[prefix.size()] != '.')
				continue;
			const char* pos = fileName + prefix.size() + 1;
			uint64_t id = 0;
			while (*pos <= '9' && *pos >= '0')
			{
				id = id * 10 + *pos - '0';
				pos++;
			}
			if (*pos == '\0')
			{
				fileInfo file;
				file.fileName = fileName;
				getFileSizeAndTimestamp((dirPath + "/" + "fileName").c_str(), &file.size, &file.timestamp);
				files.insert(std::pair<uint64_t, fileInfo>(id, file));
			}
#ifdef OS_WIN
	} while (FindNextFile(hFind, &findFileData));
	FindClose(hFind);
#endif
#ifdef OS_LINUX
	}
	closedir(dir);
#endif
	return 0;
}
DLL_EXPORT int fileList::update()
{
	clean();
	return load();
}
#ifdef OS_LINUX
DLL_EXPORT DS fileList::getFileList(const std::string& dirPath, std::vector<std::string>& files)
{
	DIR* dir = opendir(dirPath.c_str());
	if (dir == nullptr)
		dsFailedAndLogIt(errno, "open data dir:" << dirPath << " failed, errno:" << errno << "," << strerror(errno), ERROR);
	dirent* file;
	while ((file = readdir(dir)) != nullptr)
	{
		if (file->d_type != 8)
			continue;
		files.push_back(file->d_name);
	}
	closedir(dir);
	dsOk();
}
#endif

#ifdef OS_WIN
DLL_EXPORT DS fileList::getFileList(const std::string& dirPath, std::vector<std::string>& files)
{
	WIN32_FIND_DATA findFileData;
	HANDLE hFind = FindFirstFile((dirPath + "\\*").c_str(), &findFileData);
	if (INVALID_HANDLE_VALUE == hFind)
		dsFailedAndLogIt(errno, "open data dir:" << dirPath << " failed, errno:" << errno << "," << strerror(errno), ERROR);
	do
	{
		if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
			continue;
		files.push_back(findFileData.cFileName);
	} while (FindNextFile(hFind, &findFileData));
	FindClose(hFind);
}
#endif

