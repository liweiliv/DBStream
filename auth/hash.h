#pragma once
#include <string>
namespace AUTH
{
	class hashWrap
	{
	public:
		inline bool equal(const std::string& s, const std::string& d) const
		{
			return s == d;
		}
		inline size_t hash(const std::string& s) const
		{
			std::hash<std::string> h;
			return h(s);
		}
	};
}