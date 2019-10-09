#pragma once
#include <string.h>
#include "util/winString.h"
namespace META {
	class nameCompare {
	public:
		bool caseSensitive;
		nameCompare(bool caseSensitive=false):caseSensitive(caseSensitive){}
		nameCompare(const nameCompare &c) :caseSensitive(c.caseSensitive) {}
		nameCompare& operator=(const nameCompare c) { caseSensitive = c.caseSensitive; return *this; }
		inline int compare(const char* src, const char* dest)const
		{
			if (caseSensitive)
			{
				return strcmp(src, dest);
			}
			else
			{
				return strcasecmp(src, dest);
			}
		}
		inline uint32_t hash(const char* s)const
		{
			uint32_t hash = 1315423911;
			if (caseSensitive)
			{
				while (*s)
					hash ^= ((hash << 5) + (*s++) + (hash >> 2));
			}
			else
			{
				while (*s)
				{
					if (*s >= 'A' && *s <= 'Z')
						hash ^= ((hash << 5) + (*s++) + ('a' - 'A') + (hash >> 2));
					else
						hash ^= ((hash << 5) + (*s++) + (hash >> 2));
				}
			}
			return (hash & 0x7FFFFFFF);
		}
		//hash
		inline uint32_t operator()(const char* name)const
		{
			return hash(name);
		}
		inline bool operator()(const char* src, const char* dest)const
		{
			return compare(src, dest) == 0;
		}
	};
}