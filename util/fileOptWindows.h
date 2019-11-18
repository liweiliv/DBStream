#pragma once
#include <windows.h>
#include <stdint.h>
#include <io.h>
#define fileHandle HANDLE 

static fileHandle openFile(const char *file,bool read,bool write,bool create)
{
	uint64_t flag = 0;
	if (read)
		flag |= GENERIC_READ;
	if (write)
		flag |= GENERIC_WRITE;
	fileHandle fd = CreateFile(file, flag, 0, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (fd == INVALID_HANDLE_VALUE)
	{
		uint64_t errCode = GetLastError();
		errno = errCode;
		if (errCode == ERROR_FILE_NOT_FOUND)//no such file
		{
			if (create)
			{
				fd = CreateFile(file, flag, 0, nullptr, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, nullptr);
				return fd;
			}
		}
		return INVALID_HANDLE_VALUE;
	}
	else
		return fd;
}
static int64_t seekFile(fileHandle fd, int64_t position, int seekType)
{
	LARGE_INTEGER li;
	li.QuadPart = position;
	li.LowPart = SetFilePointer(fd,li.LowPart,&li.HighPart,seekType);
	if (li.LowPart == INVALID_SET_FILE_POINTER && GetLastError()
		!= NO_ERROR)
	{
		li.QuadPart = -1;
	}
	return li.QuadPart;
}
static int truncateFile(fileHandle fd, uint64_t offset)
{
	if (offset != seekFile(fd, offset, SEEK_SET))
		return -1;
	return SetEndOfFile(fd)?0:-1;
}
static bool fileHandleValid(fileHandle fd)
{
	return fd != INVALID_HANDLE_VALUE;
}


static int64_t writeFile(fileHandle fd, const char* data, uint64_t size)
{
	DWORD writed = 0;
	uint64_t remain = size;
	while (WriteFile(fd, data+(size- remain), size, &writed, nullptr))
	{
		if (writed > 0)
		{
			if (0 == (remain -= writed))
				break;
			writed = 0;
		}
		else
		{
			if (remain == size)//write no thing
				return -1;
			else//rollback
			{
				if (seekFile(fd, remain - size, SEEK_CUR) < 0)
				{
					return -2;
				}
			}
		}
	}
	return size - remain;
}
static int64_t readFile(fileHandle fd, char *buf, uint64_t size)
{
	DWORD readed = 0;
	uint64_t remain = size;
	while (ReadFile(fd, buf+(size-remain), size, &readed, nullptr))
	{
		if (readed > 0)
		{
			if(0==(remain -= readed))
				break;
			readed = 0;
		}
		else
		{
			if (remain == size)//read no thing
				return -1;
			else//rollback
			{
				if (seekFile(fd, remain - size, SEEK_CUR) < 0)
				{
					return -2;
				}
			}
		}
	}
	return size - remain;
}
static int closeFile(fileHandle fd)
{
	return CloseHandle(fd);
}
static int fsync(fileHandle fd)
{
	return FlushFileBuffers(fd)?0:-1;
}
static inline  int checkFileExist(const char* filename,int mode)
{
	return _access(filename, mode);
}
static long getFileTime(const char * file)
{
	FILETIME creationTime, lastAccessTime, lastWriteTime;
	fileHandle fd = openFile(file, true, false, false);
	if (fd == INVALID_HANDLE_VALUE)
		return -1;
	GetFileTime(fd, &creationTime, &lastAccessTime, &lastWriteTime);
	closeFile(fd);
	return lastWriteTime.dwLowDateTime;
}
static int64_t getFileSize(const char* fileName)
{
	fileHandle fd = openFile(fileName, true, false, false);
	if (fd == INVALID_HANDLE_VALUE)
		return -1;
	DWORD high = 0,size;
	size = GetFileSize(fd, &high);
	if (size == INVALID_FILE_SIZE)
	{
		closeFile(fd);
		return -1;
	}
	closeFile(fd);
	return (high<<32)+size;
}
static int getFileSizeAndTimestamp(const char* fileName, int64_t* size, int64_t* timestamp)
{
	*size = *timestamp = 0;
	fileHandle fd = openFile(fileName, true, false, false);
	if (fd == INVALID_HANDLE_VALUE)
		return -1;
	DWORD high = 0;
	*size = GetFileSize(fd, &high);
	if (*size == INVALID_FILE_SIZE)
	{
		closeFile(fd);
		return -1;
	}
	*size = (high << 32) + *size;
	FILETIME creationTime, lastAccessTime, lastWriteTime;
	GetFileTime(fd, &creationTime, &lastAccessTime, &lastWriteTime);
	*timestamp = lastWriteTime.dwLowDateTime;
	closeFile(fd);
	return 0;
}
static int removeDir(const char* dir)
{
	WIN32_FIND_DATA findFileData;
	std::string findString(dir);
	findString.append("\\").append("*");
	HANDLE hFind = FindFirstFile(findString.c_str(), &findFileData);
	if (INVALID_HANDLE_VALUE == hFind && errno != 0)
		return -1;
	if (INVALID_HANDLE_VALUE != hFind)
	{
		do
		{
			if (findFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
			{
				if (strcmp(findFileData.cFileName, ".") == 0|| strcmp(findFileData.cFileName, "..") == 0)
					continue;
				if (0 != removeDir(std::string(dir).append("\\").append(findFileData.cFileName).c_str()))
					return -1;
			}
			else
				remove(std::string(dir).append("\\").append(findFileData.cFileName).c_str());
		} while (FindNextFile(hFind, &findFileData));
		FindClose(hFind);
	}
	return RemoveDirectory(dir) ? 0 : -1;
}