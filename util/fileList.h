 once
#include <string>
#include <map>
#include <string.h>
#include <stdint.h>
#include "file.h"
class fileList
{
public:
private:
	std::string dirPath;
	std::string prefix;
	std::map< uint64_t, fileInfo> files;
public:
	fileList(const char* dirPath, const char* prefix) :dirPath(dirPath), prefix(prefix)
	{
	}
	~fileList()
	{
		clean();
	}
	void clean();
	int load();
	int update();
	const std::map< uint64_t, fileInfo>& get()
	{
		return files;
	}

};
