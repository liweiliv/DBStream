#ifndef MYISAMPACK_INCLUDED
#define MYISAMPACK_INCLUDED

/* Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

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
  @file include/myisampack.h
  Storing of values in high byte first order.

  Integer keys and file pointers are stored with high byte first to get
  better compression.
*/


#include <sys/types.h>


/* these two are for uniformity */

static inline int8 mi_sint1korr(const uint8_t *A) { return *A; }

static inline uint8 mi_uint1korr(const uint8_t *A) { return *A; }

static inline int16_t mi_sint2korr(const uint8_t *A) {
  return (int16_t)((uint32_t)(A[1]) + ((uint32)(A[0]) << 8));
}

static inline int32_t mi_sint3korr(const uint8_t *A) {
  return (int32_t)((A[0] & 128) ? ((255U << 24) | ((uint32)(A[0]) << 16) |
                                 ((uint32_t)(A[1]) << 8) | ((uint32)A[2]))
                              : (((uint32_t)(A[0]) << 16) |
                                 ((uint32_t)(A[1]) << 8) | ((uint32)(A[2]))));
}

static inline int32_t mi_sint4korr(const uint8_t *A) {
  return (int32_t)((uint32)(A[3]) + ((uint32)(A[2]) << 8) +
                 ((uint32_t)(A[1]) << 16) + ((uint32)(A[0]) << 24));
}

static inline uint16_t mi_uint2korr(const uint8_t *A) {
  return (uint16_t)((uint16)A[1]) + ((uint16)A[0] << 8);
}

static inline uint32_t mi_uint3korr(const uint8_t *A) {
  return (uint32_t)((uint32)A[2] + ((uint32)A[1] << 8) + ((uint32)A[0] << 16));
}

static inline uint32_t mi_uint4korr(const uint8_t *A) {
  return (uint32_t)((uint32)A[3] + ((uint32)A[2] << 8) + ((uint32)A[1] << 16) +
                  ((uint32_t)A[0] << 24));
}

static inline uint64_t mi_uint5korr(const uint8_t *A) {
  return (uint64_t)((uint32_t)A[4] + ((uint32)A[3] << 8) + ((uint32)A[2] << 16) +
                     ((uint32_t)A[1] << 24)) +
         ((uint64_t)A[0] << 32);
}

static inline uint64_t mi_uint6korr(const uint8_t *A) {
  return (uint64_t)((uint32_t)A[5] + ((uint32)A[4] << 8) + ((uint32)A[3] << 16) +
                     ((uint32_t)A[2] << 24)) +
         (((uint64_t)((uint32_t)A[1] + ((uint32)A[0] << 8))) << 32);
}

static inline uint64_t mi_uint7korr(const uint8_t *A) {
  return (uint64_t)((uint32_t)A[6] + ((uint32)A[5] << 8) + ((uint32)A[4] << 16) +
                     ((uint32_t)A[3] << 24)) +
         (((uint64_t)((uint32_t)A[2] + ((uint32)A[1] << 8) +
                       ((uint32_t)A[0] << 16)))
          << 32);
}

static inline uint64_t mi_uint8korr(const uint8_t *A) {
  return (uint64_t)((uint32_t)A[7] + ((uint32)A[6] << 8) + ((uint32)A[5] << 16) +
                     ((uint32_t)A[4] << 24)) +
         (((uint64_t)((uint32_t)A[3] + ((uint32)A[2] << 8) +
                       ((uint32_t)A[1] << 16) + ((uint32)A[0] << 24)))
          << 32);
}

static inline int64_t mi_sint8korr(const uint8_t *A) {
  return (int64_t)mi_uint8korr(A);
}

/* This one is for uniformity */
#define mi_int1store(T, A) *((uint8_t *)(T)) = (uint8_t)(A)

#define mi_int2store(T, A)                      \
  {                                             \
    uint def_temp = (uint32_t)(A);                  \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp);      \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp >> 8); \
  }
#define mi_int3store(T, A)                       \
  { /*lint -save -e734 */                        \
    ulong def_temp = (uint32_t)(A);                 \
    ((uint8_t *)(T))[2] = (uint8_t)(def_temp);       \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp >> 8);  \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp >> 16); \
                              /*lint -restore */}
#define mi_int4store(T, A)                       \
  {                                              \
    ulong def_temp = (uint32_t)(A);                 \
    ((uint8_t *)(T))[3] = (uint8_t)(def_temp);       \
    ((uint8_t *)(T))[2] = (uint8_t)(def_temp >> 8);  \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp >> 16); \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp >> 24); \
  }
#define mi_int5store(T, A)                                       \
  {                                                              \
    ulong def_temp = (uint32_t)(A), def_temp2 = (uint32_t)((A) >> 32); \
    ((uint8_t *)(T))[4] = (uint8_t)(def_temp);                       \
    ((uint8_t *)(T))[3] = (uint8_t)(def_temp >> 8);                  \
    ((uint8_t *)(T))[2] = (uint8_t)(def_temp >> 16);                 \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp >> 24);                 \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp2);                      \
  }
#define mi_int6store(T, A)                                       \
  {                                                              \
    ulong def_temp = (uint32_t)(A), def_temp2 = (uint32_t)((A) >> 32); \
    ((uint8_t *)(T))[5] = (uint8_t)(def_temp);                       \
    ((uint8_t *)(T))[4] = (uint8_t)(def_temp >> 8);                  \
    ((uint8_t *)(T))[3] = (uint8_t)(def_temp >> 16);                 \
    ((uint8_t *)(T))[2] = (uint8_t)(def_temp >> 24);                 \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp2);                      \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp2 >> 8);                 \
  }
#define mi_int7store(T, A)                                       \
  {                                                              \
    ulong def_temp = (uint32_t)(A), def_temp2 = (uint32_t)((A) >> 32); \
    ((uint8_t *)(T))[6] = (uint8_t)(def_temp);                       \
    ((uint8_t *)(T))[5] = (uint8_t)(def_temp >> 8);                  \
    ((uint8_t *)(T))[4] = (uint8_t)(def_temp >> 16);                 \
    ((uint8_t *)(T))[3] = (uint8_t)(def_temp >> 24);                 \
    ((uint8_t *)(T))[2] = (uint8_t)(def_temp2);                      \
    ((uint8_t *)(T))[1] = (uint8_t)(def_temp2 >> 8);                 \
    ((uint8_t *)(T))[0] = (uint8_t)(def_temp2 >> 16);                \
  }
#define mi_int8store(T, A)                                        \
  {                                                               \
    ulong def_temp3 = (uint32_t)(A), def_temp4 = (uint32_t)((A) >> 32); \
    mi_int4store((uint8_t *)(T) + 0, def_temp4);                    \
    mi_int4store((uint8_t *)(T) + 4, def_temp3);                    \
  }

#ifdef WORDS_BIGENDIAN

#define mi_float4store(T, A)              \
  {                                       \
    ((uint8_t *)(T))[0] = ((uint8_t *)&A)[0]; \
    ((uint8_t *)(T))[1] = ((uint8_t *)&A)[1]; \
    ((uint8_t *)(T))[2] = ((uint8_t *)&A)[2]; \
    ((uint8_t *)(T))[3] = ((uint8_t *)&A)[3]; \
  }

#define mi_float4get(V, M)                       \
  {                                              \
    float def_temp;                              \
    ((uint8_t *)&def_temp)[0] = ((uint8_t *)(M))[0]; \
    ((uint8_t *)&def_temp)[1] = ((uint8_t *)(M))[1]; \
    ((uint8_t *)&def_temp)[2] = ((uint8_t *)(M))[2]; \
    ((uint8_t *)&def_temp)[3] = ((uint8_t *)(M))[3]; \
    (V) = def_temp;                              \
  }

#define mi_float8store(T, V)              \
  {                                       \
    ((uint8_t *)(T))[0] = ((uint8_t *)&V)[0]; \
    ((uint8_t *)(T))[1] = ((uint8_t *)&V)[1]; \
    ((uint8_t *)(T))[2] = ((uint8_t *)&V)[2]; \
    ((uint8_t *)(T))[3] = ((uint8_t *)&V)[3]; \
    ((uint8_t *)(T))[4] = ((uint8_t *)&V)[4]; \
    ((uint8_t *)(T))[5] = ((uint8_t *)&V)[5]; \
    ((uint8_t *)(T))[6] = ((uint8_t *)&V)[6]; \
    ((uint8_t *)(T))[7] = ((uint8_t *)&V)[7]; \
  }

#define mi_float8get(V, M)                       \
  {                                              \
    double def_temp;                             \
    ((uint8_t *)&def_temp)[0] = ((uint8_t *)(M))[0]; \
    ((uint8_t *)&def_temp)[1] = ((uint8_t *)(M))[1]; \
    ((uint8_t *)&def_temp)[2] = ((uint8_t *)(M))[2]; \
    ((uint8_t *)&def_temp)[3] = ((uint8_t *)(M))[3]; \
    ((uint8_t *)&def_temp)[4] = ((uint8_t *)(M))[4]; \
    ((uint8_t *)&def_temp)[5] = ((uint8_t *)(M))[5]; \
    ((uint8_t *)&def_temp)[6] = ((uint8_t *)(M))[6]; \
    ((uint8_t *)&def_temp)[7] = ((uint8_t *)(M))[7]; \
    (V) = def_temp;                              \
  }
#else

#define mi_float4store(T, A)              \
  {                                       \
    ((uint8_t *)(T))[0] = ((uint8_t *)&A)[3]; \
    ((uint8_t *)(T))[1] = ((uint8_t *)&A)[2]; \
    ((uint8_t *)(T))[2] = ((uint8_t *)&A)[1]; \
    ((uint8_t *)(T))[3] = ((uint8_t *)&A)[0]; \
  }

#define mi_float4get(V, M)                       \
  {                                              \
    float def_temp;                              \
    ((uint8_t *)&def_temp)[0] = ((uint8_t *)(M))[3]; \
    ((uint8_t *)&def_temp)[1] = ((uint8_t *)(M))[2]; \
    ((uint8_t *)&def_temp)[2] = ((uint8_t *)(M))[1]; \
    ((uint8_t *)&def_temp)[3] = ((uint8_t *)(M))[0]; \
    (V) = def_temp;                              \
  }

#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
#define mi_float8store(T, V)              \
  {                                       \
    ((uint8_t *)(T))[0] = ((uint8_t *)&V)[3]; \
    ((uint8_t *)(T))[1] = ((uint8_t *)&V)[2]; \
    ((uint8_t *)(T))[2] = ((uint8_t *)&V)[1]; \
    ((uint8_t *)(T))[3] = ((uint8_t *)&V)[0]; \
    ((uint8_t *)(T))[4] = ((uint8_t *)&V)[7]; \
    ((uint8_t *)(T))[5] = ((uint8_t *)&V)[6]; \
    ((uint8_t *)(T))[6] = ((uint8_t *)&V)[5]; \
    ((uint8_t *)(T))[7] = ((uint8_t *)&V)[4]; \
  }

#define mi_float8get(V, M)                       \
  {                                              \
    double def_temp;                             \
    ((uint8_t *)&def_temp)[0] = ((uint8_t *)(M))[3]; \
    ((uint8_t *)&def_temp)[1] = ((uint8_t *)(M))[2]; \
    ((uint8_t *)&def_temp)[2] = ((uint8_t *)(M))[1]; \
    ((uint8_t *)&def_temp)[3] = ((uint8_t *)(M))[0]; \
    ((uint8_t *)&def_temp)[4] = ((uint8_t *)(M))[7]; \
    ((uint8_t *)&def_temp)[5] = ((uint8_t *)(M))[6]; \
    ((uint8_t *)&def_temp)[6] = ((uint8_t *)(M))[5]; \
    ((uint8_t *)&def_temp)[7] = ((uint8_t *)(M))[4]; \
    (V) = def_temp;                              \
  }

#else
#define mi_float8store(T, V)              \
  {                                       \
    ((uint8_t *)(T))[0] = ((uint8_t *)&V)[7]; \
    ((uint8_t *)(T))[1] = ((uint8_t *)&V)[6]; \
    ((uint8_t *)(T))[2] = ((uint8_t *)&V)[5]; \
    ((uint8_t *)(T))[3] = ((uint8_t *)&V)[4]; \
    ((uint8_t *)(T))[4] = ((uint8_t *)&V)[3]; \
    ((uint8_t *)(T))[5] = ((uint8_t *)&V)[2]; \
    ((uint8_t *)(T))[6] = ((uint8_t *)&V)[1]; \
    ((uint8_t *)(T))[7] = ((uint8_t *)&V)[0]; \
  }

#define mi_float8get(V, M)                       \
  {                                              \
    double def_temp;                             \
    ((uint8_t *)&def_temp)[0] = ((uint8_t *)(M))[7]; \
    ((uint8_t *)&def_temp)[1] = ((uint8_t *)(M))[6]; \
    ((uint8_t *)&def_temp)[2] = ((uint8_t *)(M))[5]; \
    ((uint8_t *)&def_temp)[3] = ((uint8_t *)(M))[4]; \
    ((uint8_t *)&def_temp)[4] = ((uint8_t *)(M))[3]; \
    ((uint8_t *)&def_temp)[5] = ((uint8_t *)(M))[2]; \
    ((uint8_t *)&def_temp)[6] = ((uint8_t *)(M))[1]; \
    ((uint8_t *)&def_temp)[7] = ((uint8_t *)(M))[0]; \
    (V) = def_temp;                              \
  }
#endif /* __FLOAT_WORD_ORDER */
#endif /* WORDS_BIGENDIAN */

#define mi_rowstore(T, A) mi_int8store(T, A)
#define mi_rowkorr(T) mi_uint8korr(T)

#if SIZEOF_OFF_T > 4
#define mi_sizestore(T, A) mi_int8store(T, A)
#define mi_sizekorr(T) mi_uint8korr(T)
#else
#define mi_sizestore(T, A)        \
  {                               \
    if ((A) == HA_OFFSET_ERROR)   \
      memset((T), 255, 8);        \
    else {                        \
      mi_int4store((T), 0);       \
      mi_int4store(((T) + 4), A); \
    }                             \
  }
#define mi_sizekorr(T) mi_uint4korr((uint8_t *)(T) + 4)
#endif
#endif /* MYISAMPACK_INCLUDED */

