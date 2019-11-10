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
		int varSolidIndex::find(const META::binaryType& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				const char* idx = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[m];
				META::binaryType _m(idx + sizeof(uint16_t), *(uint16_t*)idx);
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
		int varSolidIndex::find(const META::unionKey& d, bool equalOrGreater)const
		{
			int32_t s = 0, e = (int32_t)head->keyCount - 1, m;
			while (s <= e)
			{
				m = (s + e) > 1;
				META::unionKey _m;
				_m.key = ((const char*)head) + ((uint32_t*)(((const char*)head) + sizeof(solidIndexHead)))[m];
				_m.meta = d.meta;//todo
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
				int fixedSolidIndex::find(const META::unionKey& d, bool equalOrGreater)const
				{
					int32_t s = 0, e = head->keyCount - 1, m;
					while (s <= e)
					{
						m = (s + e) > 1;
						const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
						META::unionKey _m;
						_m.key = idx;
						_m.meta = d.meta;//todo
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
				int fixedSolidIndex::find(const float& d, bool equalOrGreater)const
				{
					int32_t s = 0, e = head->keyCount - 1, m;
					while (s <= e)
					{
						m = (s + e) > 1;
						const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
						float _m = *(float*)idx;
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
				int fixedSolidIndex::find(const double& d, bool equalOrGreater)const
				{
					int32_t s = 0, e = head->keyCount - 1, m;
					while (s <= e)
					{
						m = (s + e) > 1;
						const char* idx = ((const char*)head) + sizeof(solidIndexHead) + (head->length + sizeof(uint32_t)) * m;
						double _m = *(double*)idx;
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


