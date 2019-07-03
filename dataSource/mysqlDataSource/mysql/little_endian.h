#pragma once
#ifndef LITTLE_ENDIAN_INCLUDED
#define LITTLE_ENDIAN_INCLUDED
/* Copyright (c) 2012, 2017, Oracle and/or its affiliates. All rights reserved.

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
	 @file include/little_endian.h
	 Data in little-endian format.
   */

   // IWYU pragma: private, include "my_byteorder.h"

#ifndef MY_BYTEORDER_INCLUDED
#error This file should never be #included directly; use my_byteorder.h.
#endif

#include <string.h>
#include <stdint.h>
/*
  Since the pointers may be misaligned, we cannot do a straight read out of
  them. (It usually works-by-accident on x86 and on modern ARM, but not always
  when the compiler chooses unusual instruction for the read, e.g. LDM on ARM
  or most SIMD instructions on x86.) memcpy is safe and gets optimized to a
  single operation, since the size is small and constant.
*/

static inline int16_t sint2korr(const uint8_t* A) {
	int16_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline int32_t sint4korr(const uint8_t* A) {
	int32_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint16_t uint2korr(const uint8_t* A) {
	uint16_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint32_t uint4korr(const uint8_t* A) {
	uint32_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline uint64_t uint8korr(const uint8_t* A) {
	uint64_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline int64_t sint8korr(const uint8_t* A) {
	int64_t ret;
	memcpy(&ret, A, sizeof(ret));
	return ret;
}

static inline void int2store(uint8_t* T, uint16_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int4store(uint8_t* T, uint32_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int7store(uint8_t* T, uint64_t A) { memcpy(T, &A, 7); }

static inline void int8store(uint8_t* T, uint64_t A) {
	memcpy(T, &A, sizeof(A));
}

static inline void float4get(float* V, const uint8_t* M) {
	memcpy(V, (M), sizeof(float));
}

static inline void float4store(uint8_t* V, float M) {
	memcpy(V, (&M), sizeof(float));
}

static inline void float8get(double* V, const uint8_t* M) {
	memcpy(V, M, sizeof(double));
}

static inline void float8store(uint8_t* V, double M) {
	memcpy(V, &M, sizeof(double));
}

static inline void floatget(float* V, const uint8_t* M) { float4get(V, M); }
static inline void floatstore(uint8_t* V, float M) { float4store(V, M); }

static inline void doublestore(uint8_t* T, double V) {
	memcpy(T, &V, sizeof(double));
}
static inline void doubleget(double* V, const uint8_t* M) {
	memcpy(V, M, sizeof(double));
}

static inline void ushortget(uint16_t* V, const uint8_t* pM) { *V = uint2korr(pM); }
static inline void shortget(int16_t* V, const uint8_t* pM) { *V = sint2korr(pM); }
static inline void longget(int32_t* V, const uint8_t* pM) { *V = sint4korr(pM); }
static inline void ulongget(uint32_t* V, const uint8_t* pM) { *V = uint4korr(pM); }
static inline void shortstore(uint8_t* T, int16_t V) { int2store(T, V); }
static inline void longstore(uint8_t* T, int32_t V) { int4store(T, V); }

static inline void longlongget(int64_t* V, const uint8_t* M) {
	memcpy(V, (M), sizeof(uint64_t));
}
static inline void longlongstore(uint8_t* T, int64_t V) {
	memcpy((T), &V, sizeof(uint64_t));
}

#endif /* LITTLE_ENDIAN_INCLUDED */

