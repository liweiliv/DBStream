#pragma once
#include <stdint.h>
#include "my_byteorder.h"
#include "../../../util/likely.h"
namespace DATA_SOURCE {
	/** maximum length of buffer in our big digits (uint32). */
#define DECIMAL_BUFF_LENGTH 9

/* the number of digits that my_decimal can possibly contain */
#define DECIMAL_MAX_POSSIBLE_PRECISION (DECIMAL_BUFF_LENGTH * 9)

#define DECIMAL_MAX_STR_LENGTH (DECIMAL_MAX_POSSIBLE_PRECISION + 2)

#define DATETIMEF_INT_OFS 0x8000000000LL
	static inline uint64_t my_packed_time_make_int(int64_t i) {
		return (static_cast<uint64_t>(i) << 24);
	}
	static inline int64_t my_packed_time_make(int64_t i, int64_t f) {
		return (static_cast<uint64_t>(i) << 24) + f;
	}
	typedef int32_t decimal_digit_t;

	typedef decimal_digit_t dec1;
	typedef int64_t dec2;

#define E_DEC_OK 0
#define E_DEC_TRUNCATED 1
#define E_DEC_OVERFLOW 2
#define E_DEC_DIV_ZERO 4
#define E_DEC_BAD_NUM 8
#define E_DEC_OOM 16

#define E_DEC_ERROR 31
#define E_DEC_FATAL_ERROR 30

#define NOT_FIXED_DEC 31

#define DIG_PER_DEC1 9
#define DIG_MASK 100000000
#define DIG_BASE 1000000000
#define DIG_MAX (DIG_BASE - 1)
#define ROUND_UP(X) (((X) + DIG_PER_DEC1 - 1) / DIG_PER_DEC1)
	static const dec1 powers10[DIG_PER_DEC1 + 1] = {
		1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };
	static const int dig2bytes[DIG_PER_DEC1 + 1] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };

	/*
	Returns the size of array to hold a binary representation of a decimal

	RETURN VALUE
	  size in bytes
	*/
	inline static int decimal_bin_size_inline(int precision, int scale) {
		int intg = precision - scale, intg0 = intg / DIG_PER_DEC1,
			frac0 = scale / DIG_PER_DEC1, intg0x = intg - intg0 * DIG_PER_DEC1,
			frac0x = scale - frac0 * DIG_PER_DEC1;
		return intg0 * sizeof(dec1) + dig2bytes[intg0x] + frac0 * sizeof(dec1) +
			dig2bytes[frac0x];
	}
	typedef enum {
		TRUNCATE = 0,
		HALF_EVEN,
		HALF_UP,
		CEILING,
		FLOOR
	} decimal_round_mode;

	/**
		intg is the number of *decimal* digits (NOT number of decimal_digit_t's !)
			 before the point
		frac is the number of decimal digits after the point
		len  is the length of buf (length of allocated space) in decimal_digit_t's,
			 not in bytes
		sign false means positive, true means negative
		buf  is an array of decimal_digit_t's
	 */
	struct decimal_t {
		int intg, frac, len;
		bool sign;
		decimal_digit_t* buf;
	};
	/* set a decimal_t to zero */
	static inline void decimal_make_zero(decimal_t* dec) {
		dec->buf[0] = 0;
		dec->intg = 1;
		dec->frac = 0;
		dec->sign = 0;
	}
#define FIX_INTG_FRAC_ERROR(len, intg1, frac1, error) \
  do {                                                \
    if (unlikely(intg1 + frac1 > (len))) {            \
      if (unlikely(intg1 > (len))) {                  \
        intg1 = (len);                                \
        frac1 = 0;                                    \
        error = E_DEC_OVERFLOW;                       \
      } else {                                        \
        frac1 = (len)-intg1;                          \
        error = E_DEC_TRUNCATED;                      \
      }                                               \
    } else                                            \
      error = E_DEC_OK;                               \
  } while (0)
#define MY_TEST(a) ((a) ? 1 : 0)

	/*
	Restores decimal from its binary fixed-length representation

	SYNOPSIS
	  bin2decimal()
		from    - value to convert
		to      - result
		precision/scale - see decimal_bin_size() below

	NOTE
	  see decimal2bin()
	  the buffer is assumed to be of the size decimal_bin_size(precision, scale)

	RETURN VALUE
	  E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW
	*/

	int bin2decimal(const uint8_t* from, decimal_t* to, int precision, int scale) {
		int error = E_DEC_OK, intg = precision - scale, intg0 = intg / DIG_PER_DEC1,
			frac0 = scale / DIG_PER_DEC1, intg0x = intg - intg0 * DIG_PER_DEC1,
			frac0x = scale - frac0 * DIG_PER_DEC1, intg1 = intg0 + (intg0x > 0),
			frac1 = frac0 + (frac0x > 0);
		dec1* buf = to->buf, mask = (*from & 0x80) ? 0 : -1;
		const uint8_t* stop;
		uint8_t* d_copy;
		int bin_size = decimal_bin_size_inline(precision, scale);

		((uint8_t*)from)[0] ^= 0x80;
		FIX_INTG_FRAC_ERROR(to->len, intg1, frac1, error);
		if (unlikely(error)) {
			if (intg1 < intg0 + (intg0x > 0)) {
				from += dig2bytes[intg0x] + sizeof(dec1) * (intg0 - intg1);
				frac0 = frac0x = intg0x = 0;
				intg0 = intg1;
			}
			else {
				frac0x = 0;
				frac0 = frac1;
			}
		}

		to->sign = (mask != 0);
		to->intg = intg0 * DIG_PER_DEC1 + intg0x;
		to->frac = frac0 * DIG_PER_DEC1 + frac0x;

		if (intg0x) {
			int i = dig2bytes[intg0x];
			dec1 x = 0;
			switch (i) {
			case 1:
				x = mi_sint1korr(from);
				break;
			case 2:
				x = mi_sint2korr(from);
				break;
			case 3:
				x = mi_sint3korr(from);
				break;
			case 4:
				x = mi_sint4korr(from);
				break;
			default:
				abort();
			}
			from += i;
			*buf = x ^ mask;
			if (((uint64_t)* buf) >= (uint64_t)powers10[intg0x + 1]) goto err;
			if (buf > to->buf || *buf != 0)
				buf++;
			else
				to->intg -= intg0x;
		}
		for (stop = from + intg0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
			*buf = mi_sint4korr(from) ^ mask;
			if (((uint32_t)* buf) > DIG_MAX) goto err;
			if (buf > to->buf || *buf != 0)
				buf++;
			else
				to->intg -= DIG_PER_DEC1;
		}
		for (stop = from + frac0 * sizeof(dec1); from < stop; from += sizeof(dec1)) {
			*buf = mi_sint4korr(from) ^ mask;
			if (((uint32_t)* buf) > DIG_MAX) goto err;
			buf++;
		}
		if (frac0x) {
			int i = dig2bytes[frac0x];
			dec1 x = 0;
			switch (i) {
			case 1:
				x = mi_sint1korr(from);
				break;
			case 2:
				x = mi_sint2korr(from);
				break;
			case 3:
				x = mi_sint3korr(from);
				break;
			case 4:
				x = mi_sint4korr(from);
				break;
			default:
				abort();
			}
			*buf = (x ^ mask) * powers10[DIG_PER_DEC1 - frac0x];
			if (((uint32_t)* buf) > DIG_MAX) goto err;
			buf++;
		}

		/*
		  No digits? We have read the number zero, of unspecified precision.
		  Make it a proper zero, with non-zero precision.
		*/
		if (to->intg == 0 && to->frac == 0) decimal_make_zero(to);
		return error;

	err:
		decimal_make_zero(to);
		return (E_DEC_BAD_NUM);
	}
	/*
	This is a direct loop unrolling of code that used to look like this:
	for (; *buf_beg < powers10[i--]; start++) ;

	@param   i    start index
	@param   val  value to compare against list of powers of 10

	@retval  Number of leading zeroes that can be removed from fraction.

	@note Why unroll? To get rid of lots of compiler warnings [-Warray-bounds]
		  Nice bonus: unrolled code is significantly faster.
	*/
	static inline int count_leading_zeroes(int i, dec1 val) {
		int ret = 0;
		switch (i) {
			/* @note Intentional fallthrough in all case labels */
		case 9:
			if (val >= 1000000000) break;
			++ret;  // Fall through.
		case 8:
			if (val >= 100000000) break;
			++ret;  // Fall through.
		case 7:
			if (val >= 10000000) break;
			++ret;  // Fall through.
		case 6:
			if (val >= 1000000) break;
			++ret;  // Fall through.
		case 5:
			if (val >= 100000) break;
			++ret;  // Fall through.
		case 4:
			if (val >= 10000) break;
			++ret;  // Fall through.
		case 3:
			if (val >= 1000) break;
			++ret;  // Fall through.
		case 2:
			if (val >= 100) break;
			++ret;  // Fall through.
		case 1:
			if (val >= 10) break;
			++ret;  // Fall through.
		case 0:
			if (val >= 1) break;
			++ret;  // Fall through.
		default: { abort(); }
		}
		return ret;
	}

	static inline dec1* remove_leading_zeroes(const decimal_t* from,
		int* intg_result) {
		int intg = from->intg, i;
		dec1* buf0 = from->buf;
		i = ((intg - 1) % DIG_PER_DEC1) + 1;
		while (intg > 0 && *buf0 == 0) {
			intg -= i;
			i = DIG_PER_DEC1;
			buf0++;
		}
		if (intg > 0) {
			intg -= count_leading_zeroes((intg - 1) % DIG_PER_DEC1, *buf0);
		}
		else
			intg = 0;
		*intg_result = intg;
		return buf0;
	}

	/*
	Convert decimal to its printable string representation

	SYNOPSIS
	  decimal2string()
		from            - value to convert
		to              - points to buffer where string representation
						  should be stored
		*to_len         - in:  size of to buffer (incl. terminating '\0')
						  out: length of the actually written string (excl. '\0')
		fixed_precision - 0 if representation can be variable length and
						  fixed_decimals will not be checked in this case.
						  Put number as with fixed point position with this
						  number of digits (sign counted and decimal point is
						  counted)
		fixed_decimals  - number digits after point.
		filler          - character to fill gaps in case of fixed_precision > 0

	RETURN VALUE
	  E_DEC_OK/E_DEC_TRUNCATED/E_DEC_OVERFLOW
	*/

	int decimal2string(const decimal_t* from, char* to, int* to_len,
		int fixed_precision, int fixed_decimals, char filler) {
		/* {intg_len, frac_len} output widths; {intg, frac} places in input */
		int len, intg, frac = from->frac, i, intg_len, frac_len, fill;
		/* number digits before decimal point */
		int fixed_intg = (fixed_precision ? (fixed_precision - fixed_decimals) : 0);
		int error = E_DEC_OK;
		char* s = to;
		dec1* buf, * buf0 = from->buf, tmp;


		/* removing leading zeroes */
		buf0 = remove_leading_zeroes(from, &intg);
		if (unlikely(intg + frac == 0)) {
			intg = 1;
			tmp = 0;
			buf0 = &tmp;
		}

		if (!(intg_len = fixed_precision ? fixed_intg : intg)) intg_len = 1;
		frac_len = fixed_precision ? fixed_decimals : frac;
		len = from->sign + intg_len + MY_TEST(frac) + frac_len;
		if (fixed_precision) {
			if (frac > fixed_decimals) {
				error = E_DEC_TRUNCATED;
				frac = fixed_decimals;
			}
			if (intg > fixed_intg) {
				error = E_DEC_OVERFLOW;
				intg = fixed_intg;
			}
		}
		else if (unlikely(len > -- * to_len)) /* reserve one byte for \0 */
		{
			int j = len - *to_len; /* excess printable chars */
			error = (frac && j <= frac + 1) ? E_DEC_TRUNCATED : E_DEC_OVERFLOW;

			/*
			  If we need to cut more places than frac is wide, we'll end up
			  dropping the decimal point as well.  Account for this.
			*/
			if (frac && j >= frac + 1) j--;

			if (j > frac) {
				intg_len = intg -= j - frac;
				frac = 0;
			}
			else
				frac -= j;
			frac_len = frac;
			len = from->sign + intg_len + MY_TEST(frac) + frac_len;
		}
		*to_len = len;
		s[len] = 0;

		if (from->sign)* s++ = '-';

		if (frac) {
			char* s1 = s + intg_len;
			fill = frac_len - frac;
			buf = buf0 + ROUND_UP(intg);
			*s1++ = '.';
			for (; frac > 0; frac -= DIG_PER_DEC1) {
				dec1 x = *buf++;
				for (i = frac < DIG_PER_DEC1 ? frac : DIG_PER_DEC1; i; i--) {
					dec1 y = x / DIG_MASK;
					*s1++ = '0' + (uint8_t)y;
					x -= y * DIG_MASK;
					x *= 10;
				}
			}
			for (; fill > 0; fill--)* s1++ = filler;
		}

		fill = intg_len - intg;
		if (intg == 0) fill--; /* symbol 0 before digital point */
		for (; fill > 0; fill--)* s++ = filler;
		if (intg) {
			s += intg;
			for (buf = buf0 + ROUND_UP(intg); intg > 0; intg -= DIG_PER_DEC1) {
				dec1 x = *--buf;
				for (i = intg < DIG_PER_DEC1 ? intg : DIG_PER_DEC1; i; i--) {
					dec1 y = x / 10;
					*--s = '0' + (uint8_t)(x - y * 10);
					x = y;
				}
			}
		}
		else
			*s = '0';

		return error;
	}

}
