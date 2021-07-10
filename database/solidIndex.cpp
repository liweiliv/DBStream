/*
 * solidIndex.cpp
 *
 *  Created on: 2019年11月10日
 *      Author: liwei
 */

#include "solidIndex.h"
namespace DATABASE
{
	template<>
	DLL_EXPORT int varSolidIndex::find(const META::BinaryType& d, bool equalOrGreater)const
	{
		int32_t s = 0, e = head->keyCount - 1, m = 0;
		while (s <= e)
		{
			m = (s + e) >> 1;
			const char* idx = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[m];
			META::BinaryType _m(idx + sizeof(uint16_t), *(uint16_t*)idx);
			int c = d.compare(_m);
			if (c > 0)
			{
				s = m + 1;
			}
			else if (c < 0)
			{
				e = m - 1;
			}
			else
			{
				return m;
			}
		}
		if (equalOrGreater)
			return -1;
		if (e < 0)
			return 0;
		if (s < (int32_t)head->keyCount)
			return s;
		else
			return m;
	}
	template<>
	DLL_EXPORT int varSolidIndex::find(const META::unionKey& d, bool equalOrGreater)const
	{
		int32_t s = 0, e = (int32_t)head->keyCount - 1, m = 0;
		while (s <= e)
		{
			m = (s + e) >> 1;
			META::unionKey _m((const char*)getKey(m), d.meta);
			int c = d.compare(_m);
			if (c > 0)
			{
				s = m + 1;
			}
			else if (c < 0)
			{
				e = m - 1;
			}
			else
			{
				return m;
			}
		}
		if (equalOrGreater)
			return -1;
		if (e < 0)
			return 0;
		if (s < (int32_t)head->keyCount)
			return s;
		else
			return m;
	}
	template<>
	DLL_EXPORT int fixedSolidIndex::find(const META::unionKey& d, bool equalOrGreater)const
	{
		int32_t s = 0, e = head->keyCount - 1, m = 0;
		while (s <= e)
		{
			m = (s + e) >> 1;
			META::unionKey _m((const char*)getKey(m), d.meta);
			int c = d.compare(_m);
			if (c > 0)
			{
				s = m + 1;
			}
			else if (c < 0)
			{
				e = m - 1;
			}
			else
			{
				return m;
			}
		}
		if (equalOrGreater)
			return -1;
		if (e < 0)
			return 0;
		if (s < (int32_t)head->keyCount)
			return s;
		else
			return m;
	}
	template<>
	DLL_EXPORT int fixedSolidIndex::find(const float& d, bool equalOrGreater)const
	{
		int32_t s = 0, e = head->keyCount - 1, m = 0;
		while (s <= e)
		{
			m = (s + e) >> 1;
			float _m = *(const float*)getKey(m);
			if (d - _m > 0.00001f)
			{
				s = m + 1;
			}
			else if (_m - d > 0.00001f)
			{
				e = m - 1;
			}
			else
			{
				return m;
			}
		}
		if (equalOrGreater)
			return -1;
		if (e < 0)
			return 0;
		if (s < (int32_t)head->keyCount)
			return s;
		else
			return m;
	}
	template<>
	DLL_EXPORT int fixedSolidIndex::find(const double& d, bool equalOrGreater)const
	{
		int32_t s = 0, e = head->keyCount - 1, m = 0;
		while (s <= e)
		{
			m = (s + e) >> 1;
			double _m = *(const double*)getKey(m);
			if (d - _m > 0.0000000001f)
			{
				s = m + 1;
			}
			else if (_m - d > 0.0000000001f)
			{
				e = m - 1;
			}
			else
			{
				return m;
			}
		}
		if (equalOrGreater)
			return -1;
		if (e < 0)
			return 0;
		if (s < (int32_t)head->keyCount)
			return s;
		else
			return m;
	}
}


