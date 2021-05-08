#pragma once
#include <stdint.h>
#include <string.h>
#include "util/winString.h"
namespace SQL_PARSER
{
	struct str {
		const char* pos;
		uint32_t size;
		str() :pos(nullptr), size(0) {}
		str(const char* pos, uint32_t size) :pos(pos), size(size) {}
		str(const str& s) :pos(s.pos), size(s.size) {}
		str(const char* s) :pos(s), size(strlen(s)) {}
		str& operator=(const str& s)
		{
			pos = s.pos;
			size = s.size;
			return *this;
		}
		inline void assign(const char* pos, uint32_t size)
		{
			this->pos = pos;
			this->size = size;
		}
		inline void assign(const str& s)
		{
			this->pos = s.pos;
			this->size = s.size;
		}
		inline int compare(const str& s)
		{
			if (pos == s.pos && size == s.size)
				return true;
			if (size < s.size)
			{
				if (memcmp(pos, s.pos, size) <= 0)
					return -1;
				else
					return 1;
			}
			else if (size == s.size)
			{
				return memcmp(pos, s.pos, size);
			}
			else
			{
				if (memcmp(pos, s.pos, size) > 0)
					return 1;
				else
					return -1;
			}
		}
		inline std::string toString()
		{
			if (pos != nullptr)
				return std::string(pos, size);
			else
				return "";
		}
	};
	class strCompare {
	public:
		bool caseSensitive;
		strCompare(bool caseSensitive = false) :caseSensitive(caseSensitive) {}
		strCompare(const strCompare& c) :caseSensitive(c.caseSensitive) {}
		strCompare& operator=(const strCompare c) { caseSensitive = c.caseSensitive; return *this; }
		inline uint32_t hash(const str* v)const
		{
			uint32_t hash = 1315423911;
			const char* s = v->pos, * e = v->pos + v->size;
			if (caseSensitive)
			{
				while (s < e)
					hash ^= ((hash << 5) + (*s++) + (hash >> 2));
			}
			else
			{
				while (s < e)
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
		inline uint32_t operator()(const str* word)const
		{
			return hash(word);
		}
		inline bool operator()(const str* src, const str* dest)const
		{
			if (src->size != dest->size)
				return false;
			if (caseSensitive)
				return memcmp(src->pos, dest->pos, src->size) == 0;
			else
				return strncasecmp(src->pos, dest->pos, src->size) == 0;
		}
	};

	class strCompareCaseSensitive :public strCompare {
	public:
		strCompareCaseSensitive() :strCompare(true) {}
	};
}