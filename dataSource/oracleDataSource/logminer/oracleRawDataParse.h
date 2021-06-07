#pragma once
#include "occi.h"
#include <stdint.h>
namespace DATA_SOURCE
{
	inline bool oracleNumberToInt64(const uint8_t* ociNumber,int64_t &value,bool force)
	{
		value = 0;
		if (ociNumber[0] > 0x80)
		{
			int8_t intLength = ociNumber[0] - 0xc0;
			if (intLength > 19 && !force)//greater than max int64_t
				return false;
			for (uint8_t idx = 1; idx < 22&&ociNumber[idx] != 0; idx++, intLength--)
			{
				if (intLength <= 0)
					return force;
				if (ociNumber[idx] == 0)
					break;
				if (idx >= 18)
				{
					uint64_t tmp = value * 100 + ociNumber[idx] - 0x01;
					if (tmp < value)//overflow
					{
						value = LLONG_MAX;
						return force;
					}
					value = tmp;
				}
				else
				{
					value *= 100;
					value += ociNumber[idx] - 0x01;
				}

			}
			for (uint8_t idx = 0; idx < intLength; idx++)
			{
				if (value * 100 < value)
				{
					value = LLONG_MAX;
					return force;
				}
				value *= 100;
			}
			return true;
		}
		else
		{
			int8_t intLength = 0x3f-ociNumber[0];
			if (intLength > 19 && !force)//less than min int64_t
				return false;
			for (uint8_t idx = 1; idx < 22 && ociNumber[idx] != 0&& ociNumber[idx] != 0x66; idx++, intLength--)
			{
				if (intLength <= 0)
				{
					value = -value;
					return force;
				}
				if (ociNumber[idx] == 0)
					break;
				if (idx >= 18)
				{
					uint64_t tmp = value * 100 + 0x65 - ociNumber[idx];
					if (tmp < value)//overflow
					{
						value = LLONG_MIN;
						return force;
					}
					value = tmp;
				}
				else
				{
					value *= 100;
					value += 0x65 - ociNumber[idx];
				}
			}
			for (uint8_t idx = 0; idx < intLength; idx++)
			{
				if (value * 100 < value)
				{
					value = LLONG_MIN;
					return force;
				}
				value *= 100;
			}
			value = -value;
			return true;
		}
	}
	inline bool oracleNumberToUInt64(const uint8_t* ociNumber, uint64_t& value, bool force)
	{
		value = 0;
		if (ociNumber[0] > 0x80)
		{
			int8_t intLength = ociNumber[0] - 0xc0;
			if (intLength > 19 && !force)//greater than max int64_t
				return false;
			for (uint8_t idx = 1; idx < 22 && ociNumber[idx] != 0; idx++, intLength--)
			{
				if (intLength <= 0)
					return force;
				if (ociNumber[idx] == 0)
					break;
				if (idx >= 18)
				{
					uint64_t tmp = value * 100 + ociNumber[idx] - 0x01;
					if (tmp < value)//overflow
					{
						value = LLONG_MAX;
						return force;
					}
					value = tmp;
				}
				else
				{
					value *= 100;
					value += ociNumber[idx] - 0x01;
				}

			}
			for (uint8_t idx = 0; idx < intLength; idx++)
			{
				if (value * 100 < value)
				{
					value = LLONG_MAX;
					return force;
				}
				value *= 100;
			}
			return true;
		}
		else
		{
			return false;
		}
	}
	void int64ToOracleNumber(OCINumber* number, int64_t value)
	{
		uint8_t length = 1;
		if (value >= 0)
		{
			int64_t v = value;
			while (v != 0) {
				uint8_t c = v % 100;
				v /= 100;
				number->OCINumberPart[length++] = v + 0x01;
			}
			number->OCINumberPart[0] = 0xc0 + length - 1;
		}
		else
		{
			int64_t v = -value;
			while (v != 0) {
				uint8_t c = v % 100;
				v /= 100;
				number->OCINumberPart[length++] = 0x65 - v;
			}
			number->OCINumberPart[0] = 0x3f - (length - 1);
		}
	}
	void uint64ToOracleNumber(OCINumber* number, uint64_t value)
	{
		uint8_t length = 1;
		uint64_t v = value;
		while (v != 0) {
			uint8_t c = v % 100;
			v /= 100;
			number->OCINumberPart[length++] = v + 0x01;
		}
		number->OCINumberPart[0] = 0xc0 + length - 1;
	}
}