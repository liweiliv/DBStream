#pragma once
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
	 @file include/big_endian.h

	 Endianness-independent definitions (little_endian.h contains optimized
	 versions if you know you are on a little-endian platform).
   */

   // IWYU pragma: private, include "my_byteorder.h"

#ifndef MY_BYTEORDER_INCLUDED
#error This file should never be #included directly; use my_byteorder.h.
#endif

#include <string.h>
#include <stdint.h>

static inline int16_t sint2korr(const uint8_t* A) {
	return (int16_t)(((int16_t)(A[0])) + ((int16_t)(A[1]) << 8));
}

static inline int32_t sint4korr(const uint8_t* A) {
	return (int32_t)(((int32_t)(A[0])) + (((int32_t)(A[1]) << 8)) +
		(((int32_t)(A[2]) << 16)) + (((int32_t)(A[3]) << 24)));
}

static inline uint16_t uint2korr(const uint8_t* A) {
	return (uint16_t)(((uint16_t)(A[0])) + ((uint16_t)(A[1]) << 8));
}

static inline uint32_t uint4korr(const uint8_t* A) {
	return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
		(((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24));
}

static inline uint64_t uint8korr(const uint8_t* A) {
	return ((uint64_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
		(((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24)) +
		(((uint64_t)(((uint32_t)(A[4])) + (((uint32_t)(A[5])) << 8) +
		(((uint32_t)(A[6])) << 16) + (((uint32_t)(A[7])) << 24)))
			<< 32));
}

static inline int64_t sint8korr(const uint8_t* A) {
	return (int64_t)uint8korr(A);
}

static inline void int2store(uint8_t* T, uint16_t A) {
	uint32_t def_temp = A;
	*(T) = (uint8_t)(def_temp);
	*(T + 1) = (uint8_t)(def_temp >> 8);
}

static inline void int4store(uint8_t* T, uint32_t A) {
	*(T) = (uint8_t)(A);
	*(T + 1) = (uint8_t)(A >> 8);
	*(T + 2) = (uint8_t)(A >> 16);
	*(T + 3) = (uint8_t)(A >> 24);
}

static inline void int7store(uint8_t* T, uint64_t A) {
	*(T) = (uint8_t)(A);
	*(T + 1) = (uint8_t)(A >> 8);
	*(T + 2) = (uint8_t)(A >> 16);
	*(T + 3) = (uint8_t)(A >> 24);
	*(T + 4) = (uint8_t)(A >> 32);
	*(T + 5) = (uint8_t)(A >> 40);
	*(T + 6) = (uint8_t)(A >> 48);
}

static inline void int8store(uint8_t* T, uint64_t A) {
	uint32_t def_temp = (uint32_t)A, def_temp2 = (uint32_t)(A >> 32);
	int4store(T, def_temp);
	int4store(T + 4, def_temp2);
}

/*
  Data in big-endian format.
*/
static inline void float4store(uint8_t* T, float A) {
	*(T) = ((uint8_t*)& A)[3];
	*((T)+1) = (char)((uint8_t*)& A)[2];
	*((T)+2) = (char)((uint8_t*)& A)[1];
	*((T)+3) = (char)((uint8_t*)& A)[0];
}

static inline void float4get(float* V, const uint8_t* M) {
	float def_temp;
	((uint8_t*)& def_temp)[0] = (M)[3];
	((uint8_t*)& def_temp)[1] = (M)[2];
	((uint8_t*)& def_temp)[2] = (M)[1];
	((uint8_t*)& def_temp)[3] = (M)[0];
	(*V) = def_temp;
}

static inline void float8store(uint8_t* T, double V) {
	*(T) = ((uint8_t*)& V)[7];
	*((T)+1) = (char)((uint8_t*)& V)[6];
	*((T)+2) = (char)((uint8_t*)& V)[5];
	*((T)+3) = (char)((uint8_t*)& V)[4];
	*((T)+4) = (char)((uint8_t*)& V)[3];
	*((T)+5) = (char)((uint8_t*)& V)[2];
	*((T)+6) = (char)((uint8_t*)& V)[1];
	*((T)+7) = (char)((uint8_t*)& V)[0];
}

static inline void float8get(double* V, const uint8_t* M) {
	double def_temp;
	((uint8_t*)& def_temp)[0] = (M)[7];
	((uint8_t*)& def_temp)[1] = (M)[6];
	((uint8_t*)& def_temp)[2] = (M)[5];
	((uint8_t*)& def_temp)[3] = (M)[4];
	((uint8_t*)& def_temp)[4] = (M)[3];
	((uint8_t*)& def_temp)[5] = (M)[2];
	((uint8_t*)& def_temp)[6] = (M)[1];
	((uint8_t*)& def_temp)[7] = (M)[0];
	(*V) = def_temp;
}

static inline void ushortget(uint16_t*V, const uint8_t* pM) {
	*V = (uint16_t)(((uint16_t)((uint8_t)(pM)[1])) + ((uint16_t)((uint16_t)(pM)[0]) << 8));
}
static inline void shortget(int16_t*V, const uint8_t* pM) {
	*V = (short)(((short)((uint8_t)(pM)[1])) + ((short)((short)(pM)[0]) << 8));
}
static inline void longget(int32_t* V, const uint8_t* pM) {
	int32_t def_temp;
	((uint8_t*)& def_temp)[0] = (pM)[0];
	((uint8_t*)& def_temp)[1] = (pM)[1];
	((uint8_t*)& def_temp)[2] = (pM)[2];
	((uint8_t*)& def_temp)[3] = (pM)[3];
	(*V) = def_temp;
}
static inline void ulongget(uint32_t* V, const uint8_t* pM) {
	uint32_t def_temp;
	((uint8_t*)& def_temp)[0] = (pM)[0];
	((uint8_t*)& def_temp)[1] = (pM)[1];
	((uint8_t*)& def_temp)[2] = (pM)[2];
	((uint8_t*)& def_temp)[3] = (pM)[3];
	(*V) = def_temp;
}
static inline void shortstore(uint8_t* T, int16_t A) {
	uint32_t def_temp = (uint)(A);
	*(((char*)T) + 1) = (char)(def_temp);
	*(((char*)T) + 0) = (char)(def_temp >> 8);
}
static inline void longstore(uint8_t* T, int32_t A) {
	*(((char*)T) + 3) = ((A));
	*(((char*)T) + 2) = (((A) >> 8));
	*(((char*)T) + 1) = (((A) >> 16));
	*(((char*)T) + 0) = (((A) >> 24));
}

static inline void floatget(float* V, const uint8_t* M) {
	memcpy(V, (M), sizeof(float));
}

static inline void floatstore(uint8_t* T, float V) {
	memcpy((T), (&V), sizeof(float));
}

static inline void doubleget(double* V, const uint8_t* M) {
	memcpy(V, (M), sizeof(double));
}

static inline void doublestore(uint8_t* T, double V) {
	memcpy((T), &V, sizeof(double));
}

static inline void longlongget(int64_t* V, const uint8_t* M) {
	memcpy(V, (M), sizeof(uint64_t));
}
static inline void longlongstore(uint8_t* T, int64_t V) {
	memcpy((T), &V, sizeof(uint64_t));
}

