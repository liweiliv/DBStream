#pragma once
#ifndef MY_BYTEORDER_INCLUDED
#define MY_BYTEORDER_INCLUDED

/* Copyright (c) 2001, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

   /**
	 @file include/my_byteorder.h
	 Functions for reading and storing in machine-independent format.
	 The little-endian variants are 'korr' (assume 'corrector') variants
	 for integer types, but 'get' (assume 'getter') for floating point types.
   */

#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#ifdef OS_LINUX
#include <arpa/inet.h>
#endif
#if defined(_MSC_VER)
#include <stdlib.h>
#endif

#if defined(_WIN32) && defined(WIN32_LEAN_AND_MEAN)
#include <winsock2.h>
#endif

#ifdef WORDS_BIGENDIAN
#include "big_endian.h"  // IWYU pragma: export
#else
#include "little_endian.h"  // IWYU pragma: export
#endif

static inline int32_t sint3korr(const uint8_t* A) {
	return ((int32_t)(((A[2]) & 128)
		? (((uint32_t)255L << 24) | (((uint32_t)A[2]) << 16) |
		(((uint32_t)A[1]) << 8) | ((uint32_t)A[0]))
		: (((uint32_t)A[2]) << 16) | (((uint32_t)A[1]) << 8) |
		((uint32_t)A[0])));
}

static inline uint32_t uint3korr(const uint8_t* A) {
	return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
		(((uint32_t)(A[2])) << 16));
}

static inline uint64_t uint5korr(const uint8_t* A) {
	return ((uint64_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
		(((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24)) +
		(((uint64_t)(A[4])) << 32));
}

static inline uint64_t uint6korr(const uint8_t* A) {
	return ((uint64_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
		(((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24)) +
		(((uint64_t)(A[4])) << 32) + (((uint64_t)(A[5])) << 40));
}

/**
  int3store

  Stores an unsinged integer in a platform independent way

  @param T  The destination buffer. Must be at least 3 bytes long
  @param A  The integer to store.

  _Example:_
  A @ref a_protocol_type_int3 "int \<3\>" with the value 1 is stored as:
  ~~~~~~~~~~~~~~~~~~~~~
  01 00 00
  ~~~~~~~~~~~~~~~~~~~~~
*/
static inline void int3store(uint8_t* T, uint32_t A) {
	*(T) = (uint8_t)(A);
	*(T + 1) = (uint8_t)(A >> 8);
	*(T + 2) = (uint8_t)(A >> 16);
}

static inline void int5store(uint8_t* T, uint64_t A) {
	*(T) = (uint8_t)(A);
	*(T + 1) = (uint8_t)(A >> 8);
	*(T + 2) = (uint8_t)(A >> 16);
	*(T + 3) = (uint8_t)(A >> 24);
	*(T + 4) = (uint8_t)(A >> 32);
}

static inline void int6store(uint8_t* T, uint64_t A) {
	*(T) = (uint8_t)(A);
	*(T + 1) = (uint8_t)(A >> 8);
	*(T + 2) = (uint8_t)(A >> 16);
	*(T + 3) = (uint8_t)(A >> 24);
	*(T + 4) = (uint8_t)(A >> 32);
	*(T + 5) = (uint8_t)(A >> 40);
}

#ifdef __cplusplus

static inline int16_t sint2korr(const char* pT) {
	return sint2korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline uint16_t uint2korr(const char* pT) {
	return uint2korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline uint32_t uint3korr(const char* pT) {
	return uint3korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline int32_t sint3korr(const char* pT) {
	return sint3korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline uint32_t uint4korr(const char* pT) {
	return uint4korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline int32_t sint4korr(const char* pT) {
	return sint4korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline uint64_t uint6korr(const char* pT) {
	return uint6korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline uint64_t uint8korr(const char* pT) {
	return uint8korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline int64_t sint8korr(const char* pT) {
	return sint8korr(static_cast<const uint8_t*>(static_cast<const void*>(pT)));
}

static inline void int2store(char* pT, uint16_t A) {
	int2store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

static inline void int3store(char* pT, uint32_t A) {
	int3store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

static inline void int4store(char* pT, uint32_t A) {
	int4store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

static inline void int5store(char* pT, uint64_t A) {
	int5store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

static inline void int6store(char* pT, uint64_t A) {
	int6store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

static inline void int8store(char* pT, uint64_t A) {
	int8store(static_cast<uint8_t*>(static_cast<void*>(pT)), A);
}

/*
  Functions for reading and storing in machine format from/to
  short/long to/from some place in memory V should be a variable
  and M a pointer to byte.
*/

static inline void float4store(char* V, float M) {
	float4store(static_cast<uint8_t*>(static_cast<void*>(V)), M);
}

static inline void float8get(double* V, const char* M) {
	float8get(V, static_cast<const uint8_t*>(static_cast<const void*>(M)));
}

static inline void float8store(char* V, double M) {
	float8store(static_cast<uint8_t*>(static_cast<void*>(V)), M);
}

/*
 Functions for big-endian loads and stores. These are safe to use
 no matter what the compiler, CPU or alignment, and also with -fstrict-aliasing.

 The stores return a pointer just past the value that was written.
*/

static inline uint16_t load16be(const char* ptr) {
	uint16_t val;
	memcpy(&val, ptr, sizeof(val));
	return ntohs(val);
}

static inline uint32_t load32be(const char* ptr) {
	uint32_t val;
	memcpy(&val, ptr, sizeof(val));
	return ntohl(val);
}

static inline char* store16be(char* ptr, uint16_t val) {
#if defined(_MSC_VER)
	// _byteswap_ushort is an intrinsic on MSVC, but htons is not.
	val = _byteswap_ushort(val);
#else
	val = htons(val);
#endif
	memcpy(ptr, &val, sizeof(val));
	return ptr + sizeof(val);
}

static inline char* store32be(char* ptr, uint32_t val) {
	val = htonl(val);
	memcpy(ptr, &val, sizeof(val));
	return ptr + sizeof(val);
}

// Adapters for using uint8_t * instead of char *.

static inline uint16_t load16be(const uint8_t* ptr) {
	return load16be((const char*)(ptr));
}

static inline uint32_t load32be(const uint8_t* ptr) {
	return load32be((const char*)(ptr));
}

static inline uint8_t* store16be(uint8_t* ptr, uint16_t val) {
	return (uint8_t*)(store16be((char*)(ptr), val));
}

static inline uint8_t* store32be(uint8_t* ptr, uint32_t val) {
	return (uint8_t*)(store32be((char*)(ptr), val));
}



	static inline uint32_t mi_uint4korr(const uint8_t* A) {
		return (uint32_t)((uint32_t)A[3] + ((uint32_t)A[2] << 8) + ((uint32_t)A[1] << 16) +
			((uint32_t)A[0] << 24));
	}
	static inline uint16_t mi_uint2korr(const uint8_t* A) {
		return (uint16_t)((uint16_t)A[1]) + ((uint16_t)A[0] << 8);
	}

	static inline char mi_sint1korr(const uint8_t* A) { return *A; }
	static inline int16_t mi_sint2korr(const uint8_t* A) {
		return (int16_t)((uint32_t)(A[1]) + ((uint32_t)(A[0]) << 8));
	}
	static inline int32_t mi_sint3korr(const uint8_t* A) {
		return (int32_t)((A[0] & 128) ? ((255U << 24) | ((uint32_t)(A[0]) << 16) |
			((uint32_t)(A[1]) << 8) | ((uint32_t)A[2]))
			: (((uint32_t)(A[0]) << 16) |
			((uint32_t)(A[1]) << 8) | ((uint32_t)(A[2]))));
	}
	static inline uint32_t mi_uint3korr(const uint8_t* A) {
		 return (uint32_t)((uint32_t)A[2] + ((uint32_t)A[1] << 8) + ((uint32_t)A[0] << 16));
	}

	static inline int32_t mi_sint4korr(const uint8_t* A) {
		return (int32_t)((uint32_t)(A[3]) + ((uint32_t)(A[2]) << 8) +
			((uint32_t)(A[1]) << 16) + ((uint32_t)(A[0]) << 24));
	}

	static inline uint64_t mi_uint5korr(const uint8_t* A) {
		return (uint64_t)((uint32_t)A[4] + ((uint32_t)A[3] << 8) + ((uint32_t)A[2] << 16) +
			((uint32_t)A[1] << 24)) +
			((uint64_t)A[0] << 32);
	}
	static inline uint64_t mi_uint6korr(const uint8_t* A) {
		return (uint64_t)((uint32_t)A[5] + ((uint32_t)A[4] << 8) + ((uint32_t)A[3] << 16) +
			((uint32_t)A[2] << 24)) +
			(((uint64_t)((uint32_t)A[1] + ((uint32_t)A[0] << 8))) << 32);
	}






/*
  Methods for reading and storing in machine independent
  format (low byte first).
*/


#if !defined(le16toh)
/**
  Converting a 16 bit integer from little-endian byte order to host byteorder

  @param x  16-bit integer in little endian byte order
  @return  16-bit integer in host byte order
*/
uint16_t inline le16toh(uint16_t x) {
#ifndef IS_BIG_ENDIAN
	return x;
#else
	return ((x >> 8) | (x << 8));
#endif
}
#endif

#if !defined(le32toh)
/**
  Converting a 32 bit integer from little-endian byte order to host byteorder

  @param x  32-bit integer in little endian byte order
  @return  32-bit integer in host byte order
*/
uint32_t inline le32toh(uint32_t x) {
#ifndef IS_BIG_ENDIAN
	return x;
#else
	return (((x >> 24) & 0xff) | ((x << 8) & 0xff0000) | ((x >> 8) & 0xff00) |
		((x << 24) & 0xff000000));
#endif
}
#endif

#if !defined(be32toh)
/**
  Converting a 32 bit integer from big-endian byte order to host byteorder

  @param x  32-bit integer in big endian byte order
  @return  32-bit integer in host byte order
*/
uint32_t inline be32toh(uint32_t x) {
#ifndef IS_BIG_ENDIAN
	return (((x >> 24) & 0xff) | ((x << 8) & 0xff0000) | ((x >> 8) & 0xff00) |
		((x << 24) & 0xff000000));
#else
	return x;
#endif
}
#endif

#if !defined(le64toh)
/**
  Converting a 64 bit integer from little-endian byte order to host byteorder

  @param x  64-bit integer in little endian byte order
  @return  64-bit integer in host byte order
*/
uint64_t inline le64toh(uint64_t x) {
#ifndef IS_BIG_ENDIAN
	return x;
#else
	x = ((x << 8) & 0xff00ff00ff00ff00ULL) | ((x >> 8) & 0x00ff00ff00ff00ffULL);
	x = ((x << 16) & 0xffff0000ffff0000ULL) | ((x >> 16) & 0x0000ffff0000ffffULL);
	return (x << 32) | (x >> 32);
#endif
}
#endif

#endif /* __cplusplus */

#endif /* MY_BYTEORDER_INCLUDED */

