/* ==========================================================================
 * spf.rl - "spf.c", a Sender Policy Framework library.
 * --------------------------------------------------------------------------
 * Copyright (c) 2009, 2010  William Ahern
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */
#include <stddef.h>	/* size_t */
#include <stdarg.h>	/* va_list va_start() va_arg() va_end() */
#include <stdint.h>	/* intptr_t */
#include <stdlib.h>	/* malloc(3) free(3) abs(3) labs(3) */
#include <stdio.h>	/* FILE */

#include <limits.h>	/* UCHAR_MAX USHRT_MAX */ 

#include <ctype.h>	/* isgraph(3) isdigit(3) tolower(3) */

#include <string.h>	/* memcpy(3) strlen(3) strsep(3) strcmp(3) */

#include <errno.h>	/* EINVAL EFAULT ENAMETOOLONG E2BIG errno */

#include <assert.h>	/* assert(3) */

#include <time.h>	/* time(3) */

#include <setjmp.h>	/* jmp_buf _setjmp(3) _longjmp(3) */

#include <sys/socket.h>	/* AF_INET AF_INET6 */

#include <unistd.h>	/* gethostname(3) */

#include <netinet/in.h>	/* struct in_addr struct in6_addr */

#include "dns.h"
#include "spf.h"


#define SPF_DEFEXP "%{i} is not one of %{d}'s designated mail servers."


/*
 * C O M P I L E R  A N N O T A T I O N S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#if __GNUC__
#define SPF_NOTUSED __attribute__((unused))
#else
#define SPF_NOTUSED
#endif

#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Winitializer-overrides"
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
#elif (__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Woverride-init"
#pragma GCC diagnostic ignored "-Wstrict-aliasing" /* IN6_IS_ADDR_V4COMPAT, ... */
#endif


/*
 * D E B U G  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int spf_debug = 0;

#if SPF_DEBUG
#include <stdio.h> /* stderr fprintf(3) */

#undef SPF_DEBUG
#define SPF_DEBUG spf_debug

#define SPF_SAY_(fmt, ...) \
	do { if (spf_unlikely(SPF_DEBUG > 0)) fprintf(stderr, fmt "%.1s", __func__, __LINE__, __VA_ARGS__); } while (0)
#define SPF_SAY(...) SPF_SAY_(">>>> (%s:%d) " __VA_ARGS__, "\n")
#define SPF_HAI SPF_SAY("HAI")

#else /* !SPF_DEBUG */

#undef SPF_DEBUG
#define SPF_DEBUG 0

#define SPF_SAY(...)
#define SPF_HAI

#endif /* SPF_DEBUG */


/*
 * M I S C .  M A C R O S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#if __GNUC__ >= 3
#define spf_likely(e)	__builtin_expect((e), 1)
#define spf_unlikely(e)	__builtin_expect((e), 0)
#else
#define spf_likely(e)	(e)
#define spf_unlikely(e)	(e)
#endif

/** static assert */
#define spf_verify_true(R) (!!sizeof (struct { unsigned int constraint: (R)? 1 : -1; }))
#define spf_verify(R) extern int (*spf_contraint (void))[spf_verify_true(R)]

#define spf_lengthof(a) (sizeof (a) / sizeof (a)[0])
#define spf_endof(a) (&(a)[spf_lengthof((a))])

#define SPF_PASTE(x, y) a##b
#define SPF_XPASTE(x, y) SPF_PASTE(a, b)
#define SPF_STRINGIFY_(x) #x
#define SPF_STRINGIFY(x) SPF_STRINGIFY_(x)


/*
 * V E R S I O N  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

const char *spf_vendor(void) {
	return SPF_VENDOR;
} /* spf_vendor() */


int spf_v_rel(void) {
	return SPF_V_REL;
} /* spf_v_rel() */


int spf_v_abi(void) {
	return SPF_V_ABI;
} /* spf_v_abi() */


int spf_v_api(void) {
	return SPF_V_API;
} /* spf_v_api() */


/*
 * E R R O R  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

const char *spf_strerror(int error) {
	switch (error) {
	case SPF_EQUERYLIMIT:
		return "SPF query limit reached";
	case SPF_ENOPOLICY:
		return "No SPF policy found";
	case SPF_EBADPOLICY:
		return "Invalid SPF policy";
	case SPF_ESERVFAIL:
		return "Query failure";
	case SPF_EVMFAULT:
		return "Virtual machine fault";
	default:
		return dns_strerror(error);
	} /* switch() */
} /* spf_strerror() */


/*
 * S T R I N G  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

static size_t spf_itoa(char *dst, size_t lim, unsigned i) {
	unsigned r, d = 1000000000, p = 0;
	size_t dp = 0;

	if (i) {
		do {
			if ((r = i / d) || p) {
				i -= r * d;

				p++;

				if (dp < lim)
					dst[dp] = '0' + r;
				dp++;
			}
		} while (d /= 10);
	} else {
		if (dp < lim)
			dst[dp] = '0';
		dp++;
	}

	if (lim)
		dst[SPF_MIN(dp, lim - 1)] = '\0';

	return dp;
} /* spf_itoa() */


unsigned long spf_atoi(const char *src) {
	unsigned long i = 0;

	while (isdigit((unsigned char)*src)) {
		i *= 10;
		i += *src++ - '0';
	}

	return i;
} /* spf_atoi() */


unsigned spf_xtoi(const char *src) {
	static const unsigned char tobase[256] =
		{ ['0'] = 0x10, ['1'] = 0x11, ['2'] = 0x12, ['3'] = 0x13, ['4'] = 0x14,
		  ['5'] = 0x15, ['6'] = 0x16, ['7'] = 0x17, ['8'] = 0x18, ['9'] = 0x19,
		  ['a'] = 0x1a, ['b'] = 0x1b, ['c'] = 0x1c, ['d'] = 0x1d, ['e'] = 0x1e, ['f'] = 0x1f,
		  ['A'] = 0x1A, ['B'] = 0x1B, ['C'] = 0x1C, ['D'] = 0x1D, ['E'] = 0x1E, ['F'] = 0x1F };
	unsigned n, i = 0;

	while ((0xf0 & (n = tobase[0xff & (unsigned char)*src++]))) {
		i <<= 4;
		i |= 0x0f & n;
	}

	return i;
} /* spf_xtoi() */


size_t spf_itox(char *dst, size_t lim, unsigned i) {
	static const char tohex[] = "0123456789abcdef";
	unsigned r, d = 0x10000000, p = 0;
	size_t dp = 0;

	if (i) {
		do {
			if ((r = i / d) || p) {
				i -= r * d;

				p++;

				if (dp < lim)
					dst[dp] = tohex[r];
				dp++;
			}
		} while (d /= 16);
	} else {
		if (dp < lim)
			dst[dp] = '0';

		dp++;
	}

	if (lim)
		dst[SPF_MIN(dp, lim - 1)] = '\0';

	return dp;
} /* spf_itox() */


static size_t spf_strlcpy(char *dst, const char *src, size_t lim) {
	char *dp = dst; char *de = &dst[lim]; const char *sp = src;

	if (dp < de) {
		do {
			if ('\0' == (*dp++ = *sp++))
				return sp - src - 1;
		} while (dp < de);

		dp[-1]	= '\0';
	}

	while (*sp++ != '\0')
		;;

	return sp - src - 1;
} /* spf_strlcpy() */


static unsigned spf_split(unsigned max, char **argv, char *src, const char *delim, _Bool empty) {
	unsigned argc = 0;
	char *arg;

	do {
		if ((arg = strsep(&src, delim)) && (*arg || empty)) {
			if (argc < max)
				argv[argc] = arg;

			argc++;
		}
	} while (arg);

	if (max)
		argv[SPF_MIN(argc, max - 1)] = 0;

	return argc;
} /* spf_split() */


char *spf_tolower(char *src) {
	unsigned char *p = (unsigned char *)src;

	while (*p) {
		*p = tolower(*p);
		++p;
	}

	return src;
} /* spf_tolower() */


static size_t spf_rtrim(char *str, const char *trim) {
	int p = strlen(str);

	while (--p >= 0 && strchr(trim, str[p]))
		str[p] = 0;

	return p + 1;
} /* spf_rtrim() */


/** domain normalization */

#define SPF_DN_CHOMP  1	/* discard root zone, if any */
#define SPF_DN_ANCHOR 2 /* add root zone, if none */
#define SPF_DN_TRUNC  4 /* discard sub-domain(s) if copy overflows */
#define SPF_DN_SUPER  8 /* discard sub-domain */

size_t spf_fixdn(char *dst, const char *src, size_t lim, int flags) {
	size_t op, dp, sp;
	int lc;

	sp = 0;
fixdn:
	op = sp;
	dp = 0;
	lc = 0;

	/* trim any leading dot(s) */
	while (src[sp] == '.') {
		if (!src[++sp]) /* but keep lone dot */
			{ --sp; break; }
	}

	while (src[sp]) {
		lc = src[sp];

		if (dp < lim)
			dst[dp] = src[sp];

		sp++; dp++;

		/* trim extra dot(s) */
		while (lc == '.' && src[sp] == '.')
			sp++;
	}

	if (flags & SPF_DN_CHOMP) {
		if (lc == '.')
			dp--;
	} else if (flags & SPF_DN_ANCHOR) {
		if (lc != '.') {
			if (dp < lim)
				dst[dp] = '.';

			dp++;
		}
	}

	if (flags & SPF_DN_SUPER) {
		flags &= ~SPF_DN_SUPER;

		while (src[op] == '.') {
			if (!src[++op]) {
				flags &= ~SPF_DN_ANCHOR;

				goto fixdn; /* output empty string */
			}
		}

		op += strcspn(&src[op], ".");

		if (src[op] == '.') {
			sp = op + 1;

			/** don't accidentally trim any final root zone. */
			if (!src[sp])
				sp--;
		}

		goto fixdn;
	} else if ((flags & SPF_DN_TRUNC) && dp >= lim) {
		op += strcspn(&src[op], ".");

		if (src[op] == '.') {
			sp = op + 1;

			if (src[sp])
				goto fixdn;

			/** return the minimum length possible */
		}
	}

	if (lim > 0)
		dst[SPF_MIN(dp, lim - 1)] = '\0';

	return dp;
} /* spf_fixdn() */


size_t spf_4top(char *dst, size_t lim, const struct in_addr *ip) {
	char tmp[16];
	size_t len;
	unsigned i;

	len = spf_itoa(tmp, sizeof tmp, 0xff & (ntohl(ip->s_addr) >> 24));

	for (i = 1; i < 4; i++) {
		tmp[len++] = '.';
		len += spf_itoa(&tmp[len], sizeof tmp - len, 0xff & (ntohl(ip->s_addr) >> (8 * (3 - i))));
	}

	return spf_strlcpy(dst, tmp, lim);
} /* spf_4top() */


/** a simple, optimistic IPv4 address string parser */
struct in_addr *spf_pto4(struct in_addr *ip, const char *src) {
	char *byte[4 + 1];
	char tmp[16];
	unsigned bytes, i, iaddr;

	spf_strlcpy(tmp, src, sizeof tmp);

	bytes = spf_split(spf_lengthof(byte), byte, tmp, ".", 1);
	iaddr = 0;

	for (i = 0; i < SPF_MIN(bytes, 4); i++) {
		iaddr <<= 8;
		iaddr |= 0xff & spf_atoi(byte[i]);
	}

	iaddr <<= 8 * (4 - i);

	ip->s_addr = htonl(iaddr);

	return ip;
} /* spf_pto4() */


#define SPF_6TOP_NYBBLE 1
#define SPF_6TOP_COMPAT 2
#define SPF_6TOP_MAPPED 4
#define SPF_6TOP_MIXED  (SPF_6TOP_COMPAT|SPF_6TOP_MAPPED)

size_t spf_6top(char *dst, size_t lim, const struct in6_addr *ip, int flags) {
	static const char tohex[] = "0123456789abcdef";
	unsigned short group[8];
	char tmp[SPF_MAX(40, 64)]; /* 40 for canon, 64 for nybbles (includes '\0') */
	size_t len;
	unsigned i;
	_Bool run, ran;

	len = 0;

	if (flags & SPF_6TOP_NYBBLE) {
		tmp[len++] = tohex[0x0f & (ip->s6_addr[0] >> 4)];
		tmp[len++] = '.';
		tmp[len++] = tohex[0x0f & (ip->s6_addr[0] >> 0)];

		for (i = 1; i < 16; i++) {
			tmp[len++] = '.';
			tmp[len++] = tohex[0x0f & (ip->s6_addr[i] >> 4)];
			tmp[len++] = '.';
			tmp[len++] = tohex[0x0f & (ip->s6_addr[i] >> 0)];
		}
	} else if (IN6_IS_ADDR_V4COMPAT(ip) && (flags & SPF_6TOP_COMPAT)) {
		tmp[len++] = ':';
		tmp[len++] = ':';

		len += spf_itoa(&tmp[len], sizeof tmp - len, ip->s6_addr[12]);

		for (i = 13; i < 16; i++) {
			tmp[len++] = '.';
			len += spf_itoa(&tmp[len], sizeof tmp - len, ip->s6_addr[i]);
		}
	} else if (IN6_IS_ADDR_V4MAPPED(ip) && (flags & SPF_6TOP_MAPPED)) {
		tmp[len++] = ':';
		tmp[len++] = ':';
		tmp[len++] = 'f';
		tmp[len++] = 'f';
		tmp[len++] = 'f';
		tmp[len++] = 'f';
		tmp[len++] = ':';

		len += spf_itoa(&tmp[len], sizeof tmp - len, ip->s6_addr[12]);

		for (i = 13; i < 16; i++) {
			tmp[len++] = '.';
			len += spf_itoa(&tmp[len], sizeof tmp - len, ip->s6_addr[i]);
		}
	} else {
		for (i = 0; i < 8; i++) {
			group[i] = (0xff00 & (ip->s6_addr[i * 2] << 8))
			         | (0x00ff & (ip->s6_addr[i * 2 + 1] << 0));
		}

		run = 0; ran = 0;

		if (group[0]) {
			len = spf_itox(tmp, sizeof tmp, group[0]);
		} else
			run++;

		for (i = 1; i < 8; i++) {
			if (group[i] || ran) {
				if (run) {
					tmp[len++] = ':';
					ran = 1; run = 0;
				}

				tmp[len++] = ':';
				len += spf_itox(&tmp[len], sizeof tmp - len, group[i]);
			} else
				run++;
		}

		if (run) {
			tmp[len++] = ':';
			tmp[len++] = ':';
		}
	}

	tmp[len] = '\0';

	return spf_strlcpy(dst, tmp, lim);
} /* spf_6top() */


/** a simple, optimistic IPv6 address string parser */
struct in6_addr *spf_pto6(struct in6_addr *ip, const char *src) {
	char *part[32 + 1]; /* 8 words or 32 nybbles */
	char tmp[64];
	unsigned short group[8] = { 0 };
	unsigned count, i, j, k;
	struct in_addr ip4;

	spf_strlcpy(tmp, src, sizeof tmp);

	count = spf_split(spf_lengthof(part), part, tmp, ":", 1);

	if (count > 1) {
		for (i = 0; i < SPF_MIN(count, 8); i++) {
			if (*part[i]) {
				if (strchr(part[i], '.')) {
					spf_pto4(&ip4, part[i]);

					group[i] = 0xffff & (ntohl(ip4.s_addr) >> 16);

					if (++i < 8)
						group[i] = 0xffff & ntohl(ip4.s_addr);
				} else {
					group[i] = spf_xtoi(part[i]);
				}
			} else {
				for (j = 7, k = count - 1; j > i && k > i; j--, k--) {
					if (strchr(part[k], '.')) {
						spf_pto4(&ip4, part[k]);

						group[j] = 0xffff & ntohl(ip4.s_addr);
						assert(j > 0);
						group[--j] = 0xffff & (ntohl(ip4.s_addr) >> 16);
					} else {
						group[j] = spf_xtoi(part[k]);
					}
				}

				break;
			}
		}
	} else {
		spf_strlcpy(tmp, src, sizeof tmp);

		count = spf_split(spf_lengthof(part), part, tmp, ".", 1);
		count = SPF_MIN(count, 32);

		for (i = 0, j = 0; i < count; j++) {
			for (k = 0; k < 4 && i < count; k++, i++) {
				group[j] <<= 4;
				group[j] |= 0xf & spf_xtoi(part[i]);
			}

			group[j] <<= 4 * (4 - k);
		}
	}

	for (i = 0, j = 0; i < 8; i++) {
		ip->s6_addr[j++] = 0xff & (group[i] >> 8);
		ip->s6_addr[j++] = 0xff & (group[i] >> 0);
	}

	while (j < 16)
		ip->s6_addr[j++] = 0;

	return ip;
} /* spf_pto6() */


void *spf_pton(void *dst, int af, const char *src) {
	return (af == AF_INET6)? (void *)spf_pto6(dst, src) : (void *)spf_pto4(dst, src);
} /* spf_pton() */


size_t spf_ntop(char *dst, size_t lim, int af, const void *ip, int flags) {
	if (af == AF_INET6)
		return spf_6top(dst, lim, ip, flags);
	else
		return spf_4top(dst, lim, ip);
} /* spf_ntop() */


int spf_6cmp(const struct in6_addr *a, const struct in6_addr *b, unsigned prefix) {
	unsigned i, n;
	int cmp;

	for (i = 0; i < prefix / 8 && i < 16; i++) {
		if ((cmp = a->s6_addr[i] - b->s6_addr[i]))
			return cmp;
	}

	if ((prefix % 8) && i < 16) {
		n = (8 - (prefix % 8));

		if ((cmp = (a->s6_addr[i] >> n) - (b->s6_addr[i] >> n)))
			return cmp;
	}

	return 0;
} /* spf_6cmp() */


int spf_4cmp(const struct in_addr *a,  const struct in_addr *b, unsigned prefix) {
	unsigned long x = ntohl(a->s_addr), y = ntohl(b->s_addr);

	if (!prefix) {
		return 0;
	} if (prefix < 32) {
		x >>= 32 - (prefix % 32);
		y >>= 32 - (prefix % 32);
	}

	return (x < y)? -1 : (x > y)? 1 : 0;
} /* spf_4cmp() */


int spf_addrcmp(int af, const void *a, const void *b, unsigned prefix) {
	if (af == AF_INET6)
		return spf_6cmp(a, b, prefix);
	else
		return spf_4cmp(a, b, prefix);
} /* spf_addrcmp() */


const char *spf_strterm(int term) {
	switch (term) {
	case SPF_ALL:
		return "all";
	case SPF_INCLUDE:
		return "include";
	case SPF_A:
		return "a";
	case SPF_MX:
		return "mx";
	case SPF_PTR:
		return "ptr";
	case SPF_IP4:
		return "ip4";
	case SPF_IP6:
		return "ip6";
	case SPF_EXISTS:
		return "exists";
	case SPF_REDIRECT:
		return "redirect";
	case SPF_EXP:
		return "exp";
	case SPF_UNKNOWN:
		/* FALL THROUGH */
	default:
		return "unknown";
	}
} /* spf_strterm() */


const char *spf_strresult(int result) {
	switch (result) {
	case SPF_NONE:
		return "None";
	case SPF_NEUTRAL:
		return "Neutral";
	case SPF_PASS:
		return "Pass";
	case SPF_FAIL:
		return "Fail";
	case SPF_SOFTFAIL:
		return "SoftFail";
	case SPF_TEMPERROR:
		return "TempError";
	case SPF_PERMERROR:
		return "PermError";
	default:
		return "Unknown";
	}
} /* spf_strresult() */


int spf_iresult(const char *result) {
	if (!strcasecmp(result, "None")) {
		return SPF_NONE;
	} else if (!strcasecmp(result, "Neutral")) {
		return SPF_NEUTRAL;
	} else if (!strcasecmp(result, "Pass")) {
		return SPF_PASS;
	} else if (!strcasecmp(result, "Fail")) {
		return SPF_FAIL;
	} else if (!strcasecmp(result, "SoftFail")) {
		return SPF_SOFTFAIL;
	} else if (!strcasecmp(result, "TempError")) {
		return SPF_TEMPERROR;
	} else if (!strcasecmp(result, "PermError")) {
		return SPF_PERMERROR;
	} else {
		return 0;
	}
} /* spf_iresult() */


/*
 * S T R I N G  B U F F E R  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define SBUF_INIT(sbuf) { 0 }

struct spf_sbuf {
	unsigned end;

	_Bool overflow;

	char str[512];
}; /* struct spf_sbuf */

static struct spf_sbuf *sbuf_init(struct spf_sbuf *sbuf) {
	return memset(sbuf, 0, sizeof *sbuf);
} /* sbuf_init() */

static _Bool sbuf_putc(struct spf_sbuf *sbuf, int ch) {
	if (sbuf->end < sizeof sbuf->str - 1)
		sbuf->str[sbuf->end++] = ch;
	else
		sbuf->overflow = 1;

	return !sbuf->overflow;
} /* sbuf_putc() */

static _Bool sbuf_puts(struct spf_sbuf *sbuf, const char *src) {
	while (*src && sbuf_putc(sbuf, *src))
		src++;

	return !sbuf->overflow;
} /* sbuf_puts() */

SPF_NOTUSED static _Bool sbuf_putv(struct spf_sbuf *sbuf, const void *src, size_t len) {
	size_t lim = SPF_MIN(len, (sizeof sbuf->str - 1) - sbuf->end);

	memcpy(&sbuf->str[sbuf->end], src, lim);
	sbuf->end += lim;

	sbuf->overflow = (lim != len);

	return !sbuf->overflow;
} /* sbuf_putv() */

static _Bool sbuf_puti(struct spf_sbuf *sbuf, unsigned long i) {
	char tmp[32];

	spf_itoa(tmp, sizeof tmp, i);

	return sbuf_puts(sbuf, tmp);
} /* sbuf_puti() */

static _Bool sbuf_put4(struct spf_sbuf *sbuf, const struct in_addr *ip) {
	char tmp[16];

	spf_4top(tmp, sizeof tmp, ip);

	return sbuf_puts(sbuf, tmp);
} /* sbuf_put4() */

static _Bool sbuf_put6(struct spf_sbuf *sbuf, const struct in6_addr *ip) {
	char tmp[40];

	spf_6top(tmp, sizeof tmp, ip, SPF_6TOP_MIXED);

	return sbuf_puts(sbuf, tmp);
} /* sbuf_put6() */

/*
 * Format specifiers:
 *
 * 	%%	Literal `%'
 * 	%4a	(struct in_addr *)
 * 	%6a	(struct in6_addr *)
 * 	%tq	QTYPE of (struct dns_packet *)
 * 	%nq	QNAME of (struct dns_packet *)
 * 	%d	(int)
 * 	%ld	(long int)
 * 	%u	(unsigned)
 * 	%lu	(unsigned long int)
 * 	%s	NUL-terminated (char *)
 * 	%NNs	NUL-terminated (char *), up to limit.
 */
static _Bool sbuf_vmt(struct spf_sbuf *sbuf, const char *fmt, va_list ap) {
	unsigned char lc = 0, mc = 0, fc;
	unsigned ml = 0;
	void *p;
	unsigned long u;
	long i;
	int error;

	while ((fc = *fmt++)) {
		if (lc == '%') {
			switch (fc) {
			case '0': case '1': case '2': case '3': case '4':
			case '5': case '6': case '7': case '8': case '9':
				ml *= 10; ml += fc - '0';

				/* FALL THROUGH */
			case 'l':
				mc = fc;
				continue;
			case 'a':
				if (ml == 4) {
					p = va_arg(ap, struct in_addr *);
					sbuf_put4(sbuf, p);
				} else if (ml == 6) {
					p = va_arg(ap, struct in6_addr *);
					sbuf_put6(sbuf, p);
				} else
					sbuf_putc(sbuf, '?');

				break;
			case 'n': case 't':
				mc = fc;
				continue;
			case 'q':
				p = va_arg(ap, struct dns_packet *);

				if (mc == 't') {
					struct dns_rr rr;

					if (!(error = dns_rr_parse(&rr, 12, p))) {
						sbuf_puts(sbuf, dns_strtype(rr.type));
					} else {
						sbuf_putc(sbuf, '?');
					}
				} else {
					char qname[DNS_D_MAXNAME + 1];

					if (dns_d_expand(qname, sizeof qname, 12, p, &error)) {
						sbuf_puts(sbuf, qname);
					} else {
						sbuf_putc(sbuf, '?');
					}
				}

				break;
			case 'd':
				/* FALL THROUGH */
			case 'i':
				if (mc == 'l')
					i = va_arg(ap, long);
				else
					i = va_arg(ap, int);
				if (i < 0) {
					sbuf_putc(sbuf, '-');
					i = labs(i);
				}
				sbuf_puti(sbuf, (unsigned long)i);
				break;
			case 'u':
				if (mc == 'l')
					u = va_arg(ap, long unsigned);
				else
					u = va_arg(ap, int unsigned);
				sbuf_puti(sbuf, u);
				break;
			case 's':
				p = va_arg(ap, char *);
				sbuf_puts(sbuf, p);
				break;
			case '%':
				sbuf_putc(sbuf, '%');
				break;
			default:
				sbuf_putc(sbuf, '%');
				sbuf_putc(sbuf, fc);
			} /* switch() */

			mc = 0;
			ml = 0;
			lc = 0;
		} else {
			if (fc != '%')
				sbuf_putc(sbuf, fc);
			lc = fc;
		}
	} /* while() */

	return !sbuf->overflow;
} /* sbuf_vmt() */

SPF_NOTUSED static _Bool sbuf_fmt(struct spf_sbuf *sbuf, const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	sbuf_vmt(sbuf, fmt, ap);
	va_end(ap);
	return !sbuf->overflow;
} /* sbuf_fmt() */

static _Bool spf_fmt(void *dst, size_t lim, const char *fmt, ...) {
	struct spf_sbuf sbuf = SBUF_INIT(&sbuf);
	va_list ap;
	va_start(ap, fmt);
	sbuf_vmt(&sbuf, fmt, ap);
	va_end(ap);
	spf_strlcpy(dst, sbuf.str, lim);
	return !sbuf.overflow;
} /* spf_fmt() */


/*
 * P A R S I N G / C O M P O S I N G  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

static const struct spf_all all_initializer =
	{ .type = SPF_ALL, .result = SPF_PASS };

static void all_comp(struct spf_sbuf *sbuf, struct spf_all *all) {
	sbuf_putc(sbuf, all->result);
	sbuf_puts(sbuf, "all");
} /* all_comp() */


static const struct spf_include include_initializer =
	{ .type = SPF_INCLUDE, .result = SPF_PASS, .macros = SPF_M('d'), .domain = "%{d}" };

static void include_comp(struct spf_sbuf *sbuf, struct spf_include *include) {
	sbuf_putc(sbuf, include->result);
	sbuf_puts(sbuf, "include");
	sbuf_putc(sbuf, ':');
	sbuf_puts(sbuf, include->domain);
} /* include_comp() */


static const struct spf_a a_initializer =
	{ .type = SPF_A, .result = SPF_PASS, .macros = SPF_M('d'), .domain = "%{d}", .prefix4 = 32, .prefix6 = 128 };

static void a_comp(struct spf_sbuf *sbuf, struct spf_a *a) {
	sbuf_putc(sbuf, a->result);
	sbuf_puts(sbuf, "a");
	sbuf_putc(sbuf, ':');
	sbuf_puts(sbuf, a->domain);
	sbuf_putc(sbuf, '/');
	sbuf_puti(sbuf, a->prefix4);
	sbuf_puts(sbuf, "//");
	sbuf_puti(sbuf, a->prefix6);
} /* a_comp() */


static const struct spf_mx mx_initializer =
	{ .type = SPF_MX, .result = SPF_PASS, .macros = SPF_M('d'), .domain = "%{d}", .prefix4 = 32, .prefix6 = 128 };

static void mx_comp(struct spf_sbuf *sbuf, struct spf_mx *mx) {
	sbuf_putc(sbuf, mx->result);
	sbuf_puts(sbuf, "mx");
	sbuf_putc(sbuf, ':');
	sbuf_puts(sbuf, mx->domain);
	sbuf_putc(sbuf, '/');
	sbuf_puti(sbuf, mx->prefix4);
	sbuf_puts(sbuf, "//");
	sbuf_puti(sbuf, mx->prefix6);
} /* mx_comp() */


static const struct spf_ptr ptr_initializer =
	{ .type = SPF_PTR, .result = SPF_PASS, .macros = SPF_M('d'), .domain = "%{d}" };

static void ptr_comp(struct spf_sbuf *sbuf, struct spf_ptr *ptr) {
	sbuf_putc(sbuf, ptr->result);
	sbuf_puts(sbuf, "ptr");
	sbuf_putc(sbuf, ':');
	sbuf_puts(sbuf, ptr->domain);
} /* ptr_comp() */


static const struct spf_ip4 ip4_initializer =
	{ .type = SPF_IP4, .result = SPF_PASS, .prefix = 32 };

static void ip4_comp(struct spf_sbuf *sbuf, struct spf_ip4 *ip4) {
	sbuf_putc(sbuf, ip4->result);
	sbuf_puts(sbuf, "ip4");
	sbuf_putc(sbuf, ':');
	sbuf_put4(sbuf, &ip4->addr);
	sbuf_putc(sbuf, '/');
	sbuf_puti(sbuf, ip4->prefix);
} /* ip4_comp() */


static const struct spf_ip6 ip6_initializer =
	{ .type = SPF_IP6, .result = SPF_PASS, .prefix = 128 };

static void ip6_comp(struct spf_sbuf *sbuf, struct spf_ip6 *ip6) {
	sbuf_putc(sbuf, ip6->result);
	sbuf_puts(sbuf, "ip6");
	sbuf_putc(sbuf, ':');
	sbuf_put6(sbuf, &ip6->addr);
	sbuf_putc(sbuf, '/');
	sbuf_puti(sbuf, ip6->prefix);
} /* ip6_comp() */


static const struct spf_exists exists_initializer =
	{ .type = SPF_EXISTS, .result = SPF_PASS, .macros = SPF_M('d'), .domain = "%{d}" };

static void exists_comp(struct spf_sbuf *sbuf, struct spf_exists *exists) {
	sbuf_putc(sbuf, exists->result);
	sbuf_puts(sbuf, "exists");
	sbuf_putc(sbuf, ':');
	sbuf_puts(sbuf, exists->domain);
} /* exists_comp() */


static const struct spf_redirect redirect_initializer =
	{ .type = SPF_REDIRECT };

static void redirect_comp(struct spf_sbuf *sbuf, struct spf_redirect *redirect) {
	sbuf_puts(sbuf, "redirect");
	sbuf_putc(sbuf, '=');
	sbuf_puts(sbuf, redirect->domain);
} /* redirect_comp() */


static const struct spf_exp exp_initializer =
	{ .type = SPF_EXP };

static void exp_comp(struct spf_sbuf *sbuf, struct spf_exp *exp) {
	sbuf_puts(sbuf, "exp");
	sbuf_putc(sbuf, '=');
	sbuf_puts(sbuf, exp->domain);
} /* exp_comp() */


static const struct spf_unknown unknown_initializer =
	{ .type = SPF_UNKNOWN };

static void unknown_comp(struct spf_sbuf *sbuf, struct spf_unknown *unknown) {
	sbuf_puts(sbuf, unknown->name);
	sbuf_putc(sbuf, '=');
	sbuf_puts(sbuf, unknown->value);
} /* unknown_comp() */


static const struct {
	void (*comp)();
} spf_term[] = {
	[SPF_ALL]     = { &all_comp },
	[SPF_INCLUDE] = { &include_comp },
	[SPF_A]       = { &a_comp },
	[SPF_MX]      = { &mx_comp },
	[SPF_PTR]     = { &ptr_comp },
	[SPF_IP4]     = { &ip4_comp },
	[SPF_IP6]     = { &ip6_comp },
	[SPF_EXISTS]  = { &exists_comp },

	[SPF_REDIRECT] = { &redirect_comp },
	[SPF_EXP]      = { &exp_comp },
	[SPF_UNKNOWN]  = { &unknown_comp },
}; /* spf_term[] */

static char *term_comp(struct spf_sbuf *sbuf, void *term) {
	spf_term[((union spf_term *)term)->type].comp(sbuf, term);

	return sbuf->str;
} /* term_comp() */


%%{
	machine spf_grammar;
	alphtype unsigned char;

	access parser->;
	variable p parser->p;
	variable pe parser->pe;
	variable eof parser->eof;

	action oops { goto oops; }

	action term_begin {
		result = SPF_PASS;
		memset(term, 0, sizeof *term);
		sbuf_init(&domain);
		macros = 0;
		prefix4 = 32; prefix6 = 128;
	}

	action term_macro {
		macros |= 1U << ((tolower((unsigned char)fc)) - 'a');
	}

	action term_expand {
		macros |= 1 << 30;
	}

	action term_end {
		if (term->type) {
			fbreak;
		}
	}

	action all_begin {
		term->all    = all_initializer;
		term->result = result;
	}

	action all_end {
	}

	action include_begin {
		term->include = include_initializer;
		term->result  = result;
	}

	action include_end {
		if (*domain.str) {
			spf_fixdn(term->include.domain, domain.str, sizeof term->include.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}
	}

	action a_begin {
		term->a      = a_initializer;
		term->result = result;
	}

	action a_end {
		if (*domain.str) {
			spf_fixdn(term->a.domain, domain.str, sizeof term->a.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}

		term->a.prefix4 = prefix4;
		term->a.prefix6 = prefix6;
	}

	action mx_begin {
		term->mx     = mx_initializer;
		term->result = result;
	}

	action mx_end {
		if (*domain.str) {
			spf_fixdn(term->mx.domain, domain.str, sizeof term->mx.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}

		term->mx.prefix4 = prefix4;
		term->mx.prefix6 = prefix6;
	}

	action ptr_begin {
		term->ptr    = ptr_initializer;
		term->result = result;
	}

	action ptr_end {
		if (*domain.str) {
			spf_fixdn(term->ptr.domain, domain.str, sizeof term->ptr.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}
	}

	action ip4_begin {
		term->ip4    = ip4_initializer;
		term->result = result;
	}

	action ip4_end {
		spf_pto4(&term->ip4.addr, domain.str);
		term->ip4.prefix = prefix4;
	}

	action ip6_begin {
		term->ip6    = ip6_initializer;
		term->result = result;
	}

	action ip6_end {
		spf_pto6(&term->ip6.addr, domain.str);
		term->ip6.prefix = prefix6;
	}

	action exists_begin {
		term->exists = exists_initializer;
		term->result = result;
	}

	action exists_end {
		if (*domain.str) {
			spf_fixdn(term->exists.domain, domain.str, sizeof term->exists.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}
	}

	action redirect_begin {
		term->redirect = redirect_initializer;
	}

	action redirect_end {
		if (*domain.str) {
			spf_fixdn(term->redirect.domain, domain.str, sizeof term->redirect.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}
	}

	action exp_begin {
		term->exp = exp_initializer;
	}

	action exp_end {
		if (*domain.str) {
			spf_fixdn(term->exp.domain, domain.str, sizeof term->exp.domain, SPF_DN_TRUNC);
			term->macros = macros;
		}
	}

	action unknown_begin {
		term->unknown = unknown_initializer;

		sbuf_init(&name);
		sbuf_init(&value);
	}

	action unknown_end {
		if (term->type == SPF_UNKNOWN) {
			spf_strlcpy(term->unknown.name, name.str, sizeof term->unknown.name);
			spf_strlcpy(term->unknown.value, value.str, sizeof term->unknown.value);
		}
	}

	action domain_putc {
		if (fc == '.' && lc == '.')
			goto oops;
		sbuf_putc(&domain, fc);
		lc = fc;
	}

	#
	# SPF RR grammar per RFC 4408 Sec. 15 App. A.
	#
	blank = [ \t];
	name  = alpha (alnum | "-" | "_" | ".")*;

	delimiter     = "." | "-" | "+" | "," | "/" | "_" | "=";
	transformers  = digit* "r"i?;

	# `c', `r', and `t' not allowed in policy terms.
	macro_letter  = ("s"i | "l"i | "o"i | "d"i | "i"i | "p"i | "v"i | "h"i) $term_macro;
	macro_literal = (0x21 .. 0x24) | (0x26 .. 0x7e);
	macro_expand  = ("%{" macro_letter transformers delimiter* "}") | ("%%" | "%_" | "%-") @term_expand;
	macro_string  = (macro_expand | macro_literal)+;

	toplabel       = (digit* alpha alnum*) | (alnum+ "-" (alnum | "-")* alnum);
	domain_end     = ("." toplabel "."?) | macro_expand;
	domain_literal = (0x21 .. 0x24) | (0x26 .. 0x2e) | (0x30 .. 0x7e);
	domain_macro   = (macro_expand | domain_literal)*;
	domain_spec    = (domain_macro domain_end) $domain_putc;

	qnum        = ("0" | (("3" .. "9") digit?))
	            | ("1" digit{0,2})
	            | ("2" ( ("0" .. "4" digit?)?
	                   | ("5" ("0" .. "5")?)?
	                   | ("6" .. "9")?
	                   )
	              );
	ip4_network = (qnum "." qnum "." qnum "." qnum) $domain_putc;
	ip6_network = (xdigit | ":" | ".")+ $domain_putc;

	ip4_cidr_length  = "/" >{ prefix4 = 32; } digit+ >{ prefix4 = 0; } ${ prefix4 *= 10; if ((prefix4 += fc - '0') > 32) goto oops; };
	ip6_cidr_length  = "/" >{ prefix6 = 128; } digit+ >{ prefix6 = 0; } ${ prefix6 *= 10; if ((prefix6 += fc - '0') > 128) goto oops; };
	dual_cidr_length = ip4_cidr_length? ("/" ip6_cidr_length)?;

	unknown  = name >unknown_begin ${ sbuf_putc(&name, fc); }
	           "=" macro_string ${ sbuf_putc(&value, fc); }
	           %unknown_end;
	exp      = "exp"i %exp_begin "=" domain_spec %exp_end;
	redirect = "redirect"i %redirect_begin "=" domain_spec %redirect_end;
	modifier = redirect | exp | unknown;

	exists  = "exists"i %exists_begin ":" domain_spec %exists_end;
	IP6     = "ip6"i %ip6_begin ":" ip6_network ip6_cidr_length? %ip6_end;
	IP4     = "ip4"i %ip4_begin ":" ip4_network ip4_cidr_length? %ip4_end;
	BARE4   =  ip4_network >ip4_begin ip4_cidr_length? %ip4_end;
	PTR     = "ptr"i %ptr_begin (":" domain_spec)? %ptr_end;
	MX      = "mx"i %mx_begin (":" domain_spec)? dual_cidr_length? %mx_end;
	A       = "a"i %a_begin (":" domain_spec)? dual_cidr_length? %a_end;
	inklude = "include"i %include_begin ":" domain_spec %include_end;
	all     = "all"i %all_begin %all_end;

	mechanism = all | inklude | A | MX | PTR | BARE4 | IP4 | IP6 | exists;
	qualifier = ("+" | "-" | "?" | "~") @{ result = fc; };
	directive = qualifier? mechanism;

	term      = blank+ (directive | modifier) >term_begin %term_end;
	version   = "v=spf1"i;
	record    = version term* blank*;

	main      := record $!oops;

	write data;
}%%


int spf_parse(union spf_term *term, struct spf_parser *parser, int *error) {
	enum spf_result result = 0;
	struct spf_sbuf domain, name, value;
	unsigned prefix4 = 0, prefix6 = 0;
	spf_macros_t macros = 0;
	int lc = '.';
	const unsigned char *part;

	term->type = 0;

	if (parser->p < parser->pe) {
		%% write exec;
	}

	*error = 0;

	return term->type;
oops:
	*error = SPF_EBADPOLICY;

	parser->error.lc = *parser->p;

	if (parser->p - parser->rdata >= (sizeof parser->error.near / 2))
		part = parser->p - (sizeof parser->error.near / 2);
	else
		part = parser->rdata;

	parser->error.lp = parser->p - part;
	parser->error.rp = parser->p - parser->rdata;

	memset(parser->error.near, 0, sizeof parser->error.near);
	memcpy(parser->error.near, part, SPF_MIN(sizeof parser->error.near - 1, parser->pe - part));

	if (SPF_DEBUG) {
		if (isgraph(parser->error.lc))
			SPF_SAY("`%c' invalid near offset %d of `%s'", parser->error.lc, parser->error.lp, parser->error.near);
		else
			SPF_SAY("error near offset %d of `%s'", parser->error.lp, parser->error.near);
	}

	return 0;
} /* spf_parse() */


void spf_parser_init(struct spf_parser *parser, const void *rdata, size_t rdlen) {
	parser->rdata = rdata;
	parser->p     = rdata;
	parser->pe    = parser->p + rdlen;
	parser->eof   = parser->pe;

	%% write init;
} /* spf_parser_init() */


/*
 * E N V I R O N M E N T  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int spf_env_init(struct spf_env *env, int af, const void *ip, const char *helo, const char *sender) {
	struct spf_sbuf mbox;
	struct in_addr ip4;
	char *local, *host;

	if (af == AF_INET6) {
		if (IN6_IS_ADDR_V4MAPPED(((struct in6_addr *)ip))) {
			ip4.s_addr = (((struct in6_addr *)ip)->s6_addr[12] << 24U)
			           | (((struct in6_addr *)ip)->s6_addr[13] << 16U)
			           | (((struct in6_addr *)ip)->s6_addr[14] << 8U)
			           | (((struct in6_addr *)ip)->s6_addr[15] << 0U);
			ip4.s_addr = htonl(ip4.s_addr);
			ip = &ip4;
			goto ip4;
		}

		spf_6top(env->i, sizeof env->i, ip, SPF_6TOP_NYBBLE);
		spf_6top(env->c, sizeof env->c, ip, SPF_6TOP_MIXED);

		spf_strlcpy(env->v, "ip6", sizeof env->v);
	} else {
ip4:
		spf_4top(env->i, sizeof env->i, ip);
		spf_4top(env->c, sizeof env->c, ip);

		spf_strlcpy(env->v, "in-addr", sizeof env->v);
	}

	if (!helo || !*helo)
		helo = "localhost";

	if (sender && *sender) {
		sbuf_init(&mbox);
		sbuf_puts(&mbox, sender);

		if ((host = strchr(mbox.str, '@'))) {
			*host++ = '\0';

			local = (mbox.str[0])? mbox.str : "postmaster";
			host  = (*host)? host : (char *)helo;
		} else {
			local = mbox.str;
			host  = (char *)helo;
		}
	} else {
		local = "postmaster";
		host  = (char *)helo;
	}

	if (strchr(host, '['))
		host = (char *)helo;

	spf_strlcpy(env->d, host, sizeof env->d);
	spf_strlcpy(env->h, helo, sizeof env->h);

	spf_strlcpy(env->l, local, sizeof env->l);
	spf_strlcpy(env->o, host, sizeof env->o);

	sbuf_init(&mbox);
	sbuf_puts(&mbox, env->l);
	sbuf_putc(&mbox, '@');
	sbuf_puts(&mbox, env->o);
	spf_strlcpy(env->s, mbox.str, sizeof env->s);

	spf_strlcpy(env->p, "unknown", sizeof env->p);

	spf_strlcpy(env->r, "unknown", sizeof env->r);
	spf_itoa(env->t, sizeof env->t, (unsigned long)time(0));

	return 0;
} /* spf_env_init() */


static size_t spf_getenv_(char **field, int which, struct spf_env *env) {
	switch (tolower((unsigned char)which)) {
	case 's':
		*field = env->s;
		return sizeof env->s;
	case 'l':
		*field = env->l;
		return sizeof env->l;
	case 'o':
		*field = env->o;
		return sizeof env->o;
	case 'd':
		*field = env->d;
		return sizeof env->d;
	case 'i':
		*field = env->i;
		return sizeof env->i;
	case 'p':
		*field = env->p;
		return sizeof env->p;
	case 'v':
		*field = env->v;
		return sizeof env->v;
	case 'h':
		*field = env->h;
		return sizeof env->h;
	case 'c':
		*field = env->c;
		return sizeof env->c;
	case 'r':
		*field = env->r;
		return sizeof env->r;
	case 't':
		*field = env->t;
		return sizeof env->t;
	default:
		*field = 0;
		return 0;
	}
} /* spf_getenv_() */


size_t spf_getenv(char *dst, size_t lim, int which, const struct spf_env *env) {
	char *src;

	if (!spf_getenv_(&src, which, (struct spf_env *)env))
		return 0;

	return spf_strlcpy(dst, src, lim);
} /* spf_getenv() */


size_t spf_setenv(struct spf_env *env, int which, const char *src) {
	size_t lim, len;
	char *dst;

	if (!(lim = spf_getenv_(&dst, which, (struct spf_env *)env)))
		return strlen(src);

	len = spf_strlcpy(dst, src, lim);

	return SPF_MIN(lim - 1, len);
} /* spf_setenv() */


void spf_printenv(const struct spf_env *env, FILE *fp) {
	fprintf(fp, "%%{s} : %s\n", env->s);
	fprintf(fp, "%%{l} : %s\n", env->l);
	fprintf(fp, "%%{o} : %s\n", env->o);
	fprintf(fp, "%%{d} : %s\n", env->d);
	fprintf(fp, "%%{i} : %s\n", env->i);
	fprintf(fp, "%%{p} : %s\n", env->p);
	fprintf(fp, "%%{v} : %s\n", env->v);
	fprintf(fp, "%%{h} : %s\n", env->h);
	fprintf(fp, "%%{c} : %s\n", env->c);
	fprintf(fp, "%%{r} : %s\n", env->r);
	fprintf(fp, "%%{t} : %s\n", env->t);
} /* spf_printenv() */


/*
 * M A C R O  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

static size_t spf_expand_(char *dst, size_t lim, const char *src, const struct spf_env *env, int *error) {
	char field[512], *part[128], *tmp;
	const char *delim = ".";
	size_t len, dp = 0, sp = 0;
	int macro = 0;
	unsigned keep = 0;
	unsigned i, j, count;
	_Bool tr = 0, rev = 0;

	if (!(macro = *src))
		return 0;

	while (isdigit((unsigned char)src[++sp])) {
		keep *= 10;
		keep += src[sp] - '0';
		tr   = 1;
	}

	if (src[sp] == 'r')
		{ tr = 1; rev = 1; ++sp; }

	if (src[sp]) {
		delim = &src[sp];
		tr = 1;
	}

	if (!(len = spf_getenv(field, sizeof field, macro, env)))
		return 0;
	else if (len >= sizeof field)
		goto toolong;

	if (!tr)
		return spf_strlcpy(dst, field, lim);

	count = spf_split(spf_lengthof(part), part, field, delim, 0);

	if (spf_lengthof(part) <= count)
		goto toobig;

	if (rev) {
		for (i = 0, j = count - 1; i < j; i++, j--) {
			tmp     = part[i];
			part[i] = part[j];
			part[j] = tmp;
		}
	}

	if (keep && keep < count) {
		for (i = 0, j = count - keep; j < count; i++, j++)
			part[i] = part[j];

		count = keep;
	}

	for (i = 0; i < count; i++) {
		if (dp < lim)
			len = spf_strlcpy(&dst[dp], part[i], lim - dp);
		else
			len = strlen(part[i]);

		dp += len;

		if (dp < lim)
			dst[dp] = '.';

		++dp;
	}

	if (dp > 0)
		--dp;

	return dp;
toolong:
	*error = ENAMETOOLONG;

	return 0;
toobig:
	*error = E2BIG;

	return 0;
} /* spf_expand_() */


size_t spf_expand(char *dst, size_t lim, spf_macros_t *macros, const char *src, const struct spf_env *env, int *error) {
	struct spf_sbuf macro;
	size_t len, dp = 0, sp = 0;

	*error = 0;

	do {
		while (src[sp] && src[sp] != '%') {
			if (dp < lim)
				dst[dp] = src[sp];
			++sp; ++dp;
		}

		if (!src[sp])
			break;

		switch (src[++sp]) {
		case '{':
			sbuf_init(&macro);

			while (src[++sp] && src[sp] != '}')
				sbuf_putc(&macro, src[sp]);

			if (src[sp] != '}')
				break;

			++sp;

			if (isalpha((unsigned char)*macro.str))
				*macros |= 1U << (tolower((unsigned char)*macro.str) - 'a');

			len = (dp < lim)
			    ? spf_expand_(&dst[dp], lim - dp, macro.str, env, error)
			    : spf_expand_(0, 0, macro.str, env, error);

			if (!len && *error)
				return 0;

			dp += len;

			break;
		case '_':
			if (dp < lim)
				dst[dp] = ' ';
			++sp; ++dp;

			break;
		case '-':
			if (dp < lim)
				dst[dp] = '%';
			++sp; ++dp;
			if (dp < lim)
				dst[dp] = '2';
			++dp;
			if (dp < lim)
				dst[dp] = '0';
			++dp;

			break;
		case '%':
			/* FALL THROUGH */
		default:
			if (dp < lim)
				dst[dp] = src[sp];
			++sp; ++dp;

			break;
		}
	} while (src[sp]);

	if (lim)
		dst[SPF_MIN(dp, lim - 1)] = '\0';

	return dp;
} /* spf_expand() */


_Bool spf_isset(spf_macros_t macros, int which) {
	if (!isalpha((unsigned char)which))
		return 0;

	return !!(macros & (1U << (tolower((unsigned char)which) - 'a')));
} /* spf_isset() */


spf_macros_t spf_macros(const char *src, const struct spf_env *env) {
	spf_macros_t macros = 0;
	int error;

	spf_expand(0, 0, &macros, src, env, &error);

	return macros;
} /* spf_macros() */


/*
 * V I R T U A L  M A C H I N E  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

enum vm_type {
	T_INT = 0x01,
	T_REF = 0x02,
	T_MEM = 0x04,

	T_ANY = T_INT|T_REF|T_MEM,
}; /* enum vm_type */

enum vm_opcode {
	OP_HALT,	/* 0/0 */
	OP_TRAP,
	OP_NOOP,

	OP_PC,		/* 0/1 Push vm.pc */
	OP_CALL,	/* 2/N Pops #params and address. Inserts return address below parameters and jumps to address. */
	OP_RET,		/* Pops #params, shifts return address (#params-1) to top, pops and jumps */
	OP_EXIT,	/* Same as OP_RET, but return address follows vm.end, which is removed and restored. */

	OP_TRUE,	/* 0/1 Push true. */
	OP_FALSE,	/* 0/1 Push false. */
	OP_ZERO,	/* 0/1 Push 0. */
	OP_ONE,		/* 0/1 Push 1 */
	OP_TWO,		/* 0/1 Push 2 */
	OP_THREE,	/* 0/1 Push 3 */
	OP_I8,		/* 0/1 Decode next op and push as T_INT */
	OP_I16,		/* 0/1 Decode next 2 ops and push as T_INT */
	OP_I32,		/* 0/1 Decode next 4 ops and push as T_INT */
	OP_NIL,		/* 0/1 Push 0 as T_REF */
	OP_REF,		/* 0/1 Decode next sizeof(intptr_t) ops and push as T_REF */
	OP_MEM,		/* 0/1 Decode next sizeof(intptr_t) ops and push as T_MEM */
	OP_STR,		/* 0/1 Decode until next NUL, allocate and push as T_MEM. */
	OP_IN4,		/* 0/1 Decode (struct in_addr) and push as T_MEM. */
	OP_IN6,		/* 0/1 Decode (struct in6_addr) and push as T_MEM. */

	OP_DEC,		/* 1/1 Decrement S(-1) */
	OP_INC,		/* 1/1 Increment S(-1) */
	OP_NEG,		/* 1/1 Arithmetically negate S(-1) (changes type to T_INT) */
	OP_ADD,		/* 2/1 Push S(-2) + S(-1). */
	OP_NOT,		/* 1/1 Logically Negate S(-1) (changes type to T_INT)  */

	OP_EQ,		/* 2/1 Push S(-2) == S(-1) */
	OP_LT,		/* 2/1 Push S(-2) < S(-1) */

	OP_JMP,		/* 2/0 If S(-2) is non-zero, jump S(-1) instruction */
	OP_GOTO,	/* 2/0 If S(-2) is non-zero, goto I(S(-1)) */

	OP_POP,		/* 0/0 Pop item from stack */
	OP_DUP,		/* 1/2 Dup item at top of stack */
	OP_LOAD,	/* 1/1 Push a copy of S(S(-1)) onto stack (changes T_MEM to T_REF) */
	OP_STORE,	/* 2/0 Pop index and item and store at index (index computed after popping). */
	OP_MOVE,	/* 1/1 Move S(S(-1)) to top of stack, shifting everything else down. */
	OP_SWAP,	/* 0/0 Swap top two items. */

	OP_GETENV,	/* 1/1 Push spf_getenv(S(-1)) */
	OP_SETENV,	/* 2/0 Do spf_setenv(S(-1), S(-2)) */

	OP_EXPAND,	/* 1/1 Push spf_expand(S(-1)). */
	OP_ISSET,	/* 2/1 Check for macro S(-1) in S(-2). */

	OP_SUBMIT,	/* 2/0 dns_res_submit(). in: 2(qtype, qname) out: 0 */
	OP_FETCH,	/* 0/1 dns_res_fetch(). in: 0 out: 1(struct dns_packet) */
	OP_QNAME,	/* 1/1 Pop packet, Push QNAME. */
	OP_GREP,	/* 3/1 Push iterator. Takes QNAME, section and type. */
	OP_NEXT,	/* 1/2 Push next stringized RR data. */

	OP_ADDRINFO,	/* 3/0 dns_ai_open(). */
	OP_NEXTENT,	/* 0/1 dns_ai_nextent(). */

	OP_CHECK,	/* 1/2 Pop target domain, push exp and result. */
	OP_COMP,	/* 1/1 Compile S(-1), push code address or 0 if invalid policy. */

	OP_FCRD,	/* 0/0 Forward-confirmed Reverse DNS */
	OP_FCRDx,	/* 1/0 Pop addrinfo; if A/AAAA matches ${i}, add .ai_canonname as FCRD */

	OP_SLEEP,	/* 1/0 Sleep */

	OP_CAT,		/* 2/1 Concatenate into string */
	OP_CMP,		/* 2/1 strcmp() */
	OP_LC,          /* 1/1 strtolower() */
	OP_GETS,	/* 0/1 Push line from stdin */
	OP_PUTI,	/* 1/0 Print integer */
	OP_PUTS,	/* 1/0 Print string */
	OP_PUTP,	/* 1/0 Print `struct dns_packet' */
	OP_PUTA,	/* 1/0 Print `struct addrinfo' */
	OP_RSTR,	/* 1/1 Convert to string with spf_strresult() */
	OP_ATOI,	/* 1/1 Convert to string with atoi() */
	OP_4TOP,	/* 1/1 Convert (struct in_addr) to string */
	OP_PTO4,	/* 1/1 Convert string to (struct in_addr) */
	OP_6TOP,	/* 1/1 Convert (struct in6_addr) to string */
	OP_PTO6,	/* 1/1 Convert string to (struct in6_addr) */

	OP_ERROR,	/* 2/0 Throw a PERMERROR or TEMPERROR S(-2) code, S(-1) exp */

	OP_INCLUDE,
	OP_A,
	OP_MX,
	OP_A_MXv,
	OP_PTR,
	OP_IP4,
	OP_IP6,
	OP_EXISTS,
	OP_EXP,

	OP__COUNT,
}; /* enum vm_opcode */


#if !defined VM_MAXCODE
#define VM_MAXCODE 2048
#endif

#if !defined VM_MAXSTACK
#define VM_MAXSTACK 64
#endif

struct spf_resolver;

struct spf_vm {
	unsigned char code[VM_MAXCODE];
	unsigned pc, end;

	unsigned char type[VM_MAXSTACK];
	intptr_t stack[VM_MAXSTACK];
	unsigned sp;

	jmp_buf trap;

	struct spf_resolver *spf;
}; /* struct spf_vm */

static void vm_init(struct spf_vm *vm, struct spf_resolver *spf) {
	vm->spf = spf;
} /* vm_init() */


/** forward definition */
struct spf_resolver {
	struct spf_options opt;

	struct spf_limits stat;

	struct spf_env env;

	struct spf_vm vm;

	struct dns_resolver *res;

	struct {
		struct dns_addrinfo *res;
		char cname[DNS_D_MAXNAME + 1]; /* last canonical name */
		unsigned count;
	} ai;

	struct {
		_Bool done;

		union {
			struct dns_packet ptr;
			char buf[dns_p_calcsize(512)];
		};
	} fcrd;

	enum spf_result result;
	const char *exp;

	struct spf_info info;
}; /* struct spf_resolver */


static void vm_throw() __attribute__((__noreturn__));
static void vm_throw(struct spf_vm *vm, int error) {
	_longjmp(vm->trap, (error)? error : EINVAL);
} /* vm_throw() */

/*
 * NOTE: Using a macro because it delays evaluation of `error' to allow
 * code like:
 *
 * 	vm_assert(vm, !(error = do_something()), error)
 * 	vm_assert(vm, (rval = do_something(&error)), error)
 * 	vm_assert(vm, (p = malloc()), errno)
 */
#define vm_assert0(vm, cond, error) do { \
	if (spf_unlikely(!(cond))) { \
		SPF_SAY("fail: %s", SPF_STRINGIFY(cond)); \
		vm_throw((vm), (error)); \
	} \
} while (0)

#define vm_assert1(vm, cond, err, ...) do { \
	if (spf_unlikely(!(cond))) { \
		spf_fmt((vm)->spf->info.error.exp, sizeof (vm)->spf->info.error.exp, __VA_ARGS__); \
		SPF_SAY("fail: %s", (vm)->spf->info.error.exp); \
		vm_throw((vm), (err)); \
	} \
} while (0)

#define vm_assert2(...) vm_assert1(__VA_ARGS__)
#define vm_assert3(...) vm_assert1(__VA_ARGS__)
#define vm_assert4(...) vm_assert1(__VA_ARGS__)
#define vm_assert5(...) vm_assert1(__VA_ARGS__)
#define vm_assert6(...) vm_assert1(__VA_ARGS__)
#define vm_assert7(...) vm_assert1(__VA_ARGS__)
#define vm_assert8(...) vm_assert1(__VA_ARGS__)

#define vm_assert(...) DNS_PP_CALL(DNS_PP_XPASTE(vm_assert, DNS_PP_DEC(DNS_PP_DEC(DNS_PP_DEC(DNS_PP_NARG(__VA_ARGS__))))), __VA_ARGS__)


static void lim_checkterms(struct spf_vm *vm, _Bool inc) {
	/*
	 * NOTE: Use greater-than-or-equal because we do an implicit
	 * check_host() in the very beginning--OP_CHECK instruction--which
	 * adds an extra lookup to the count.
	 */
	vm_assert(vm, (vm->spf->stat.query.terms <= vm->spf->opt.limit.query.terms), SPF_EQUERYLIMIT, "Exceeded term query limit of %u", vm->spf->opt.limit.query.terms);

	if (inc)
		vm->spf->stat.query.terms++;
} /* lim_checkterms() */


static inline void vm_extend(struct spf_vm *vm, unsigned n) {
	vm_assert(vm, spf_lengthof(vm->stack) - vm->sp >= n, EFAULT);
} /* vm_extend() */


static inline int vm_indexof(struct spf_vm *vm, int p) {
	if (spf_likely(p < 0))
		p = vm->sp + p;

	vm_assert(vm, p >= 0 && p < vm->sp, EFAULT);

	return p;
} /* vm_indexof() */


static inline enum vm_type vm_typeof(struct spf_vm *vm, int p) {
	return vm->type[vm_indexof(vm, p)];
} /* vm_typeof() */


static void t_free(struct spf_vm *vm, enum vm_type t, intptr_t v) {
	switch (t) {
	case T_INT:
		/* FALL THROUGH */
	case T_REF:
		break;
	case T_MEM:
		free((void *)v);

		break;
	default:
		vm_throw(vm, EFAULT);
	} /* switch() */
} /* t_free() */


static inline intptr_t vm_pop(struct spf_vm *vm, enum vm_type t) {
	intptr_t v;
	vm_assert(vm, vm->sp, EFAULT);
	vm->sp--;
	vm_assert(vm, (vm->type[vm->sp] & t), EINVAL);
	t = vm->type[vm->sp];
	v = vm->stack[vm->sp];
	t_free(vm, t, v);
	vm->type[vm->sp]  = 0;
	vm->stack[vm->sp] = 0;
	return v;
} /* vm_pop() */


static void vm_discard(struct spf_vm *vm, unsigned n) {
	vm_assert(vm, n <= vm->sp, EFAULT);
	while (n--)
		vm_pop(vm, T_ANY);
} /* vm_discard() */


static inline intptr_t vm_push(struct spf_vm *vm, enum vm_type t, intptr_t v) {
	vm_assert(vm, vm->sp < spf_lengthof(vm->stack), ENOMEM);

	vm->type[vm->sp]  = t;
	vm->stack[vm->sp] = v;

	vm->sp++;

	return v;
} /* vm_push() */


#define vm_swap(vm) vm_move((vm), -2)

static intptr_t vm_move(struct spf_vm *vm, int p) {
	enum vm_type t;
	intptr_t v;
	int i;

	p = vm_indexof(vm, p);
	t = vm->type[p];
	v = vm->stack[p];

	i = p;

	/*
	 * DO NOT move a T_MEM item over an equivalent T_REF, because that
	 * breaks garbage-collection. Instead, swap types with the first
	 * equivalent T_REF found. (WARNING: This breaks if T_REF points
	 * into a T_MEM object. Just don't do that--nest pointers and swap
	 * stack positions.)
	 */
	if (v == T_MEM) {
		for (; i < vm->sp - 1; i++) {
			if (vm->type[i + 1] == T_REF && vm->stack[i + 1] == v) {
				vm->type[i + 1] = T_MEM;
				t = T_REF;

				break;
			}

			vm->type[i]  = vm->type[i + 1];
			vm->stack[i] = vm->stack[i + 1];
		}
	}

	for (; i < vm->sp - 1; i++) {
		vm->type[i]  = vm->type[i + 1];
		vm->stack[i] = vm->stack[i + 1];
	}

	vm->type[i]  = t;
	vm->stack[i] = v;

	return v;
} /* vm_move() */


static intptr_t vm_strdup(struct spf_vm *vm, const void *s) {
	void *v;

	vm_extend(vm, 1);
	vm_assert(vm, (v = strdup(s)), errno);
	vm_push(vm, T_MEM, (intptr_t)v);

	return (intptr_t)v;
} /* vm_strdup() */


static intptr_t vm_memdup(struct spf_vm *vm, const void *p, size_t len) {
	void *v;

	vm_extend(vm, 1);
	vm_assert(vm, (v = malloc(len)), errno);
	vm_push(vm, T_MEM, (intptr_t)memcpy(v, p, len));

	return (intptr_t)v;
} /* vm_memdup() */


static inline intptr_t vm_peek(struct spf_vm *vm, int p, enum vm_type t) {
	p = vm_indexof(vm, p);
	vm_assert(vm, (t & vm->type[p]), EINVAL);
	return vm->stack[p];
} /* vm_peek() */


static inline intptr_t vm_poke(struct spf_vm *vm, int p, enum vm_type t, intptr_t v) {
	p = vm_indexof(vm, p);
	t_free(vm, vm->type[p], vm->stack[p]);
	vm->type[p]  = t;
	vm->stack[p] = v;
	return v;
} /* vm_poke() */


static inline int vm_opcode(struct spf_vm *vm) {
	vm_assert(vm, vm->pc < spf_lengthof(vm->code), EFAULT);

	return vm->code[vm->pc];
} /* vm_opcode() */


#define vm_emit_(vm, code, v, ...) vm_emit((vm), (code), (v))
#define vm_emit(vm, ...) vm_emit_((vm), __VA_ARGS__, 0)

static unsigned (vm_emit)(struct spf_vm *vm, enum vm_opcode code, intptr_t v_) {
	uintptr_t v;
	void *p;
	unsigned i, n;

	vm_assert(vm, vm->end < spf_lengthof(vm->code), ENOMEM);

	vm->code[vm->end] = code;

	switch (code) {
	case OP_I8:
		n = 1; goto copy;
	case OP_I16:
		n = 2; goto copy;
	case OP_I32:
		n = 4; goto copy;
	case OP_REF:
		/* FALL THROUGH */
	case OP_MEM:
		n = sizeof (uintptr_t);
copy:
		v = (uintptr_t)v_;
		vm_assert(vm, vm->end <= spf_lengthof(vm->code) - n, ENOMEM);

		for (i = 0; i < n; i++)
			vm->code[++vm->end] = 0xffU & (v >> (8U * ((n-i)-1)));

		return vm->end++;
	case OP_STR:
		p = (void *)v_;
		n = strlen(p) + 1;
embed:
		vm_assert(vm, spf_lengthof(vm->code) - (vm->end + 1) >= n, ENOMEM);
		memcpy(&vm->code[++vm->end], p, n);
		vm->end += n;

		return vm->end - 1;
	case OP_IN4:
		p = (void *)v_;
		n = sizeof (struct in_addr);

		goto embed;
	case OP_IN6:
		p = (void *)v_;
		n = sizeof (struct in6_addr);

		goto embed;
	default:
		return vm->end++;
	} /* switch() */
} /* vm_emit() */


#define HALT(sub)   sub_emit((sub), OP_HALT)
#define TRAP(sub)   sub_emit((sub), OP_TRAP)
#define NOOP(sub)   sub_emit((sub), OP_NOOP)
#define PC(sub)     sub_emit((sub), OP_PC)
#define CALL(sub)   sub_emit((sub), OP_CALL)
#define RET(sub)    sub_emit((sub), OP_RET)
#define EXIT(sub)   sub_emit((sub), OP_EXIT)
#define TRUE(sub)   sub_emit((sub), OP_TRUE)
#define FALSE(sub)  sub_emit((sub), OP_FALSE)
#define ZERO(sub)   sub_emit((sub), OP_ZERO)
#define ONE(sub)    sub_emit((sub), OP_ONE)
#define TWO(sub)    sub_emit((sub), OP_TWO)
#define THREE(sub)  sub_emit((sub), OP_THREE)
#define I8(sub,v)   sub_emit((sub), OP_I8, (v))
#define I16(sub,v)  sub_emit((sub), OP_I16, (v))
#define I32(sub,v)  sub_emit((sub), OP_I32, (v))
#define NIL(sub)    sub_emit((sub), OP_NIL)
#define REF(sub,v)  sub_emit((sub), OP_REF, (v))
#define MEM(sub,v)  sub_emit((sub), OP_MEM, (v))
#define STR(sub,v)  sub_emit((sub), OP_STR, (v))
#define IN4(sub,v)  sub_emit((sub), OP_IN4, (v))
#define IN6(sub,v)  sub_emit((sub), OP_IN6, (v))
#define DEC(sub)    sub_emit((sub), OP_DEC)
#define INC(sub)    sub_emit((sub), OP_INC)
#define NEG(sub)    sub_emit((sub), OP_NEG)
#define ADD(sub)    sub_emit((sub), OP_ADD)
#define NOT(sub)    sub_emit((sub), OP_NOT)
#define EQ(sub)     sub_emit((sub), OP_EQ)
#define LT(sub)     sub_emit((sub), OP_LT)
#define POP(sub)    sub_emit((sub), OP_POP)
#define DUP(sub)    sub_emit((sub), OP_DUP)
#define LOAD(sub)   sub_emit((sub), OP_LOAD)
#define STORE(sub)  sub_emit((sub), OP_STORE)
#define MOVE(sub)   sub_emit((sub), OP_MOVE)
#define SWAP(sub)   sub_emit((sub), OP_SWAP)
#define GOTO(sub)   sub_emit((sub), OP_GOTO)
#define GETENV(sub) sub_emit((sub), OP_GETENV)
#define SETENV(sub) sub_emit((sub), OP_SETENV)
#define EXPAND(sub) sub_emit((sub), OP_EXPAND)
#define ISSET(sub)  sub_emit((sub), OP_ISSET)
#define SUBMIT(sub) sub_emit((sub), OP_SUBMIT)
#define FETCH(sub)  sub_emit((sub), OP_FETCH)
#define QNAME(sub)  sub_emit((sub), OP_QNAME)
#define GREP(sub)   sub_emit((sub), OP_GREP)
#define NEXT(sub)   sub_emit((sub), OP_NEXT)
#define CHECK(sub)  sub_emit((sub), OP_CHECK)
#define COMP(sub)   sub_emit((sub), OP_COMP)
#define FCRD(sub)   sub_emit((sub), OP_FCRD)
#define FCRDx(sub)  sub_emit((sub), OP_FCRDx)
#define CAT(sub)    sub_emit((sub), OP_CAT)
#define CMP(sub)    sub_emit((sub), OP_CMP)
#define LC(sub)     sub_emit((sub), OP_LC)
#define PUTI(sub)   sub_emit((sub), OP_PUTI)
#define PUTS(sub)   sub_emit((sub), OP_PUTS)
#define PUTP(sub)   sub_emit((sub), OP_PUTP)
#define ERROR(sub)  sub_emit((sub), OP_ERROR)

#define SUB_MAXJUMP  64
#define SUB_MAXLABEL 8

#define L0(sub) sub_label((sub), 0)
#define L1(sub) sub_label((sub), 1)
#define L2(sub) sub_label((sub), 2)
#define L3(sub) sub_label((sub), 3)
#define L4(sub) sub_label((sub), 4)
#define L5(sub) sub_label((sub), 5)
#define L6(sub) sub_label((sub), 6)
#define L7(sub) sub_label((sub), 7)

#define J0(sub) sub_jump((sub), 0)
#define J1(sub) sub_jump((sub), 1)
#define J2(sub) sub_jump((sub), 2)
#define J3(sub) sub_jump((sub), 3)
#define J4(sub) sub_jump((sub), 4)
#define J5(sub) sub_jump((sub), 5)
#define J6(sub) sub_jump((sub), 6)
#define J7(sub) sub_jump((sub), 7)

struct vm_sub {
	struct spf_vm *vm;
	struct { unsigned id, cp; } j[SUB_MAXJUMP];
	unsigned jc, l[SUB_MAXLABEL];
}; /* struct vm_sub */

static void sub_init(struct vm_sub *sub, struct spf_vm *vm)
	{ memset(sub, 0, sizeof *sub); sub->vm = vm; }

#define sub_emit(sub, ...) vm_emit((sub)->vm, __VA_ARGS__)

static void sub_link(struct vm_sub *sub) {
	unsigned i, lp, jp;

	for (i = 0; i < sub->jc; i++) {
		lp = sub->l[sub->j[i].id];
		jp = sub->j[i].cp;

		if (lp < jp) {
			vm_assert(sub->vm, (jp - lp) <= UCHAR_MAX, SPF_EVMFAULT);

			sub->vm->code[jp-3] = OP_I8;
			sub->vm->code[jp-2] = jp - lp;
			sub->vm->code[jp-1] = OP_NEG;
			sub->vm->code[jp-0] = OP_JMP;
		} else if (lp - jp > UCHAR_MAX) {
			vm_assert(sub->vm, (lp - jp) <= USHRT_MAX, SPF_EVMFAULT);

			sub->vm->code[jp-3] = OP_I16;
			sub->vm->code[jp-2] = 0xffU & ((lp - jp) >> 8U);
			sub->vm->code[jp-1] = 0xffU & ((lp - jp) >> 0U);
			sub->vm->code[jp-0] = OP_JMP;
		} else {
			sub->vm->code[jp-3] = OP_I8;
			sub->vm->code[jp-2] = lp - jp;
			sub->vm->code[jp-1] = OP_NOOP;
			sub->vm->code[jp-0] = OP_JMP;
		}
	}
} /* sub_link() */

static void sub_label(struct vm_sub *sub, unsigned id) {
	sub->l[id % spf_lengthof(sub->l)] = sub->vm->end;
} /* sub_label() */

static void sub_jump(struct vm_sub *sub, unsigned id) {
	vm_assert(sub->vm, sub->jc < spf_lengthof(sub->j), ENOMEM);
	vm_emit(sub->vm, OP_TRAP);
	vm_emit(sub->vm, OP_TRAP);
	vm_emit(sub->vm, OP_TRAP);
	sub->j[sub->jc].cp = vm_emit(sub->vm, OP_TRAP);
	sub->j[sub->jc].id = id % spf_lengthof(sub->l);
	sub->jc++;
} /* sub_jump() */


static void op_pop(struct spf_vm *vm) {
	vm_pop(vm, T_ANY);
	vm->pc++;
} /* op_pop() */


static void op_dup(struct spf_vm *vm) {
	intptr_t v;
	int t;

	v = vm_peek(vm, -1, T_ANY);
	t = vm_typeof(vm, -1);

	/* convert memory to pointer to prevent double free's */
	vm_push(vm, (t & (T_MEM))? T_REF : t, v);

	vm->pc++;
} /* op_dup() */


static void op_load(struct spf_vm *vm) {
	int p, t;

	p = vm_pop(vm, T_INT);
	t = vm_typeof(vm, p);

	/* convert memory to pointer to prevent double free's */
	vm_push(vm, (t & (T_MEM))? T_REF : t, vm_peek(vm, p, T_ANY));

	vm->pc++;
} /* op_load() */


static void op_store(struct spf_vm *vm) {
	int p;
	intptr_t v;
	p = vm_indexof(vm, vm_pop(vm, T_INT));
	v = vm_pop(vm, T_INT); /* restrict to T_INT so we don't have to worry about GC. */
	vm_poke(vm, p, T_INT, v);
	vm->pc++;
} /* op_store() */


static void op_move(struct spf_vm *vm) {
	vm_move(vm, vm_pop(vm, T_INT));
	vm->pc++;
} /* op_move() */


static void op_swap(struct spf_vm *vm) {
	vm_swap(vm);
	vm->pc++;
} /* op_swap() */


static void op_jmp(struct spf_vm *vm) {
	intptr_t cond = vm_peek(vm, -2, T_ANY);
 	int pc = vm->pc + vm_peek(vm, -1, T_INT);

	vm_discard(vm, 2);

	if (cond) {
		vm_assert(vm, pc >= 0 && pc < vm->end, EFAULT);
		vm->pc = pc;
	} else
		vm->pc++;
} /* op_jmp() */


static void op_goto(struct spf_vm *vm) {
	intptr_t cond = vm_peek(vm, -2, T_ANY);
 	int pc = vm_peek(vm, -1, T_INT);

	vm_discard(vm, 2);

	if (cond) {
		vm_assert(vm, pc >= 0 && pc < vm->end, EFAULT);
		vm->pc = pc;
	} else
		vm->pc++;
} /* op_goto() */


static void op_call(struct spf_vm *vm) {
	int f, n, i;

	f = vm_pop(vm, T_INT);
	n = vm_pop(vm, T_INT);

	vm_push(vm, T_INT, vm->pc + 1);

	/* swap return address with parameters */
	for (i = 0; i < n; i++)
		vm_move(vm, -(n + 1));

	vm->pc = f;
} /* op_call() */


static void op_ret(struct spf_vm *vm) {
	int n;

	n = vm_pop(vm, T_INT);

	/* move return address to top */
	vm_move(vm, -(n + 1));

	vm->pc = vm_pop(vm, T_INT);
} /* op_ret() */


static void op_exit(struct spf_vm *vm) {
	int n;

	n = vm_pop(vm, T_INT);

	/* move code end to top */
	vm_move(vm, -(n + 2));

	vm->end = vm_pop(vm, T_INT);

	/* move return address to top */
	vm_move(vm, -(n + 1));

	vm->pc = vm_pop(vm, T_INT);
} /* op_exit() */


static void op_trap(struct spf_vm *vm) {
	vm_throw(vm, EFAULT);
} /* op_trap() */


static void op_noop(struct spf_vm *vm) {
	vm->pc++;
} /* op_noop() */


static void op_pc(struct spf_vm *vm) {
	vm_push(vm, T_INT, vm->pc);
	vm->pc++;
} /* op_pc() */


static void op_lit(struct spf_vm *vm) {
	enum vm_opcode code = vm->code[vm->pc];
	uintptr_t v;
	enum vm_type t;
	int i, n;

	n = 0;
	v = 0;

	switch (code) {
	case OP_TRUE: case OP_FALSE:
		v = (code == OP_TRUE);
		t = T_INT;
		break;
	case OP_ZERO: case OP_ONE: case OP_TWO: case OP_THREE:
		v = (code - OP_ZERO);
		t = T_INT;
		break;
	case OP_NIL:
		v = 0;
		t = T_REF;
		break;
	case OP_I8:
		n = 1;
		t = T_INT;
		break;
	case OP_I16:
		n = 2;
		t = T_INT;
		break;
	case OP_I32:
		n = 4;
		t = T_INT;
		break;
	case OP_REF:
		n = sizeof (uintptr_t);
		t = T_REF;
		break;
	case OP_MEM:
		n = sizeof (uintptr_t);
		t = T_MEM;
		break;
	default:
		vm_throw(vm, EINVAL);
	} /* switch () */

	vm_assert(vm, vm->pc + n < vm->end, EFAULT);

	for (i = 0; i < n; i++) {
		v <<= 8;
		v |= 0xff & vm->code[++vm->pc];
	}

	vm_push(vm, t, (intptr_t)v);
	vm->pc++;
} /* op_lit() */


static void op_str(struct spf_vm *vm) {
	unsigned pe, pc = vm->pc + 1;

	for (pe = pc; pe < spf_lengthof(vm->code) && vm->code[pe]; pe++)
		;;
	pe++;
	vm_assert(vm, pe < spf_lengthof(vm->code), EFAULT);
	vm_memdup(vm, &vm->code[pc], pe - pc);

	vm->pc = pe;
} /* op_str() */


static void op_in4(struct spf_vm *vm) {
	unsigned pc = vm->pc + 1;

	vm_assert(vm, pc + sizeof (struct in_addr) < spf_lengthof(vm->code), EFAULT);
	vm_memdup(vm, &vm->code[pc], sizeof (struct in_addr));

	vm->pc = pc + sizeof (struct in_addr);
} /* op_in4() */


static void op_in6(struct spf_vm *vm) {
	unsigned pc = vm->pc + 1;

	vm_assert(vm, pc + sizeof (struct in6_addr) < spf_lengthof(vm->code), EFAULT);
	vm_memdup(vm, &vm->code[pc], sizeof (struct in6_addr));

	vm->pc = pc + sizeof (struct in6_addr);
} /* op_in6() */


static void op_dec(struct spf_vm *vm) {
	vm_poke(vm, -1, T_INT, vm_peek(vm, -1, T_INT) - 1);
	vm->pc++;
} /* op_dec() */


static void op_inc(struct spf_vm *vm) {
	vm_poke(vm, -1, T_INT, vm_peek(vm, -1, T_INT) + 1);
	vm->pc++;
} /* op_inc() */


static void op_neg(struct spf_vm *vm) {
	vm_poke(vm, -1, T_INT, -vm_peek(vm, -1, T_ANY));
	vm->pc++;
} /* op_neg() */


static void op_add(struct spf_vm *vm) {
	vm_push(vm, T_INT, vm_pop(vm, T_INT) + vm_pop(vm, T_INT));
	vm->pc++;
} /* op_add() */


static void op_not(struct spf_vm *vm) {
	vm_poke(vm, -1, T_INT, !vm_peek(vm, -1, T_ANY));
	vm->pc++;
} /* op_not() */


static void op_eq(struct spf_vm *vm) {
	enum vm_type t = vm_typeof(vm, -1);

	if ((T_REF|T_MEM) & t)
		t = T_REF|T_MEM;

	vm_push(vm, T_INT, (vm_pop(vm, t) == vm_pop(vm, t)));

	vm->pc++;
} /* op_eq() */


static void op_lt(struct spf_vm *vm) {
	enum vm_type t = vm_typeof(vm, -1);
	intptr_t a, b;

	if ((T_REF|T_MEM) & t)
		t = T_REF|T_MEM;

	b = vm_pop(vm, t);
	a = vm_pop(vm, t);

	vm_push(vm, T_INT, a < b);

	vm->pc++;
} /* op_lt() */


static void op_submit(struct spf_vm *vm) {
	void *qname = (void *)vm_peek(vm, -2, T_REF|T_MEM);
	int qtype   = vm_peek(vm, -1, T_INT);
	int error;

	SPF_SAY("querying %s IN %s", (char *)qname, dns_strtype(qtype));

	error = dns_res_submit(vm->spf->res, qname, qtype, DNS_C_IN);
	vm_assert(vm, !error, error);

	vm_discard(vm, 2);

	vm->pc++;
} /* op_submit() */


static void op_fetch(struct spf_vm *vm) {
	struct dns_packet *pkt;
	int error;

	error = dns_res_check(vm->spf->res);
	vm_assert(vm, !error, error);

	vm_extend(vm, 1);
	pkt = dns_res_fetch(vm->spf->res, &error);
	vm_assert(vm, !!pkt, error);
	vm_push(vm, T_MEM, (intptr_t)pkt);

	vm->pc++;
} /* op_fetch() */


static void op_qname(struct spf_vm *vm) {
	struct dns_packet *pkt;
	char qname[DNS_D_MAXNAME + 1];
	int error;

	pkt = (void *)vm_peek(vm, -1, T_REF|T_MEM);

	vm_assert(vm, dns_d_expand(qname, sizeof qname, 12, pkt, &error), error);

	vm_pop(vm, T_ANY);
	vm_strdup(vm, qname);

	vm->pc++;
} /* op_qname() */


struct vm_grep {
	int type;
	struct dns_rr_i iterator;
	char name[DNS_D_MAXNAME + 1];
}; /* struct vm_grep */

static void op_grep(struct spf_vm *vm) {
	struct dns_packet *pkt;
	char *name;
	struct vm_grep *grep;
	int sec, type;

	pkt  = (void *)vm_peek(vm, -4, T_REF|T_MEM);
	name = (void *)vm_peek(vm, -3, T_REF|T_MEM);
	sec  = vm_peek(vm, -2, T_INT);
	type = vm_peek(vm, -1, T_INT);

	vm_assert(vm, (grep = malloc(sizeof *grep)), errno);

	memset(&grep->iterator, 0, sizeof grep->iterator);

	grep->type = type;

	if (name && *name) {
		spf_strlcpy(grep->name, name, sizeof grep->name);
		grep->iterator.name = grep->name;
	}

	grep->iterator.section = sec;
	grep->iterator.type    = abs(type);

	dns_rr_i_init(&grep->iterator, pkt);

	vm_discard(vm, 3);
	vm_push(vm, T_MEM, (intptr_t)grep);

	vm->pc++;
} /* op_grep() */


static _Bool txt_isspf(struct dns_txt *txt) {
	if (txt->len < sizeof "v=spf1" - 1)
		return 0;
	if (strncasecmp((char *)txt->data, "v=spf1", 6))
		return 0;
	return txt->len == sizeof "v=spf1" - 1 || txt->data[6] == ' ';
} /* txt_isspf() */

static void op_next(struct spf_vm *vm) {
	struct dns_packet *pkt;
	struct vm_grep *grep;
	struct dns_rr rr;
	int error;

	pkt  = (void *)vm_peek(vm, -2, T_REF|T_MEM);
	grep = (void *)vm_peek(vm, -1, T_REF|T_MEM);

grep:
	if (dns_rr_grep(&rr, 1, &grep->iterator, pkt, &error)) {
		char rd[DNS_D_MAXNAME + 1];
		union dns_any any;
		char *txt;

		dns_any_init(&any, sizeof any);

		vm_assert(vm, !(error = dns_any_parse(&any, &rr, pkt)), error);

		switch (rr.type) {
		case DNS_T_TXT:
			if (grep->type == -DNS_T_TXT && !txt_isspf(&any.txt))
				goto grep;

			/* FALL THROUGH */
		case DNS_T_SPF:
			if (grep->type == -DNS_T_SPF && !txt_isspf(&any.txt))
				goto grep;

			/*
			 * FIXME: This is a hack to pass the OpenSPF test
			 * suite. Perhaps it would be better to pass a
			 * length parameter to the caller.
			 */
			if (memchr(any.txt.data, '\0', any.txt.len)) {
				struct spf_sbuf exp;
				sbuf_init(&exp);
				vm_assert(vm, dns_d_expand(exp.str, sizeof exp.str, 12, pkt, &error), EFAULT);
				exp.end = strlen(exp.str);
				sbuf_puts(&exp, " has embedded NUL");
				spf_strlcpy(vm->spf->info.error.exp, exp.str, sizeof vm->spf->info.error.exp);
				vm_throw(vm, SPF_EBADPOLICY);
			}

			txt = (char *)vm_memdup(vm, any.txt.data, any.txt.len + 1);
			txt[any.txt.len] = '\0';

			break;
		default:
			if (!dns_any_print(rd, sizeof rd, &any, rr.type))
				goto none;

			vm_strdup(vm, rd);

			break;
		} /* switch() */
	} else {
none:
		vm_push(vm, T_REF, 0);
	}

	vm->pc++;
} /* op_next() */


static void op_addrinfo(struct spf_vm *vm) {
	struct addrinfo hints = { .ai_family = PF_UNSPEC, .ai_socktype = SOCK_STREAM, .ai_flags = AI_CANONNAME };
	const char *host;
	char serv[16];
	int qtype, error;

	host = (char *)vm_peek(vm, -4, T_REF|T_MEM);

	if (T_INT == vm_typeof(vm, -3))
		spf_itoa(serv, sizeof serv, vm_peek(vm, -3, T_INT));
	else
		spf_strlcpy(serv, (char *)vm_peek(vm, -3, T_REF|T_MEM), sizeof serv);

	qtype = vm_peek(vm, -2, T_INT);
	hints.ai_family = vm_peek(vm, -1, T_INT);

	SPF_SAY("querying %s IN %s", host, dns_strtype(qtype));

	dns_ai_close(vm->spf->ai.res);
	vm_assert(vm, (vm->spf->ai.res = dns_ai_open(host, serv, qtype, &hints, vm->spf->res, &error)), error);

	*vm->spf->ai.cname = '\0';
	vm->spf->ai.count  = 0;

	vm_discard(vm, 4);

	vm->pc++;	
} /* op_addrinfo() */


static void op_nextent(struct spf_vm *vm) {
	struct addrinfo *ent = 0;
	int error;

	vm_extend(vm, 1);
	if ((error = dns_ai_nextent(&ent, vm->spf->ai.res)))
		vm_assert(vm, error == ENOENT, error);
	vm_push(vm, T_MEM, (intptr_t)ent);

	vm->pc++;

	if (!ent || !ent->ai_canonname || !strcasecmp(ent->ai_canonname, vm->spf->ai.cname))
		return;

	spf_strlcpy(vm->spf->ai.cname, ent->ai_canonname, sizeof vm->spf->ai.cname);
	vm->spf->ai.count++;
	vm_assert(vm, vm->spf->ai.count < vm->spf->opt.limit.query.cnames, SPF_EQUERYLIMIT, "Exceeded canonical name query limit of %u", vm->spf->opt.limit.query.cnames);
} /* op_nextent() */


static void op_getenv(struct spf_vm *vm) {
	char dst[512];

	spf_getenv(dst, sizeof dst, vm_pop(vm, T_INT), &vm->spf->env);
	vm_strdup(vm, dst);

	vm->pc++;
} /* op_getenv() */


static void op_setenv(struct spf_vm *vm) {
	char *src;

	vm_assert(vm, (src = (char *)vm_peek(vm, -2, T_REF|T_MEM)), EINVAL);
	spf_setenv(&vm->spf->env, vm_pop(vm, T_INT), src);
	vm_discard(vm, 1);

	vm->pc++;
} /* op_setenv() */


static void op_expand(struct spf_vm *vm) {
	spf_macros_t macros = 0;
	char dst[512];
	int error;

	vm_assert(vm, spf_expand(dst, sizeof dst, &macros, (void *)vm_peek(vm, -1, T_REF|T_MEM), &vm->spf->env, &error), error);

	vm_pop(vm, T_ANY);
	vm_strdup(vm, dst);

	vm->pc++;
} /* op_expand() */


static void op_isset(struct spf_vm *vm) {
	spf_macros_t macros = 0;
	int isset, error;

	vm_assert(vm, spf_expand(0, 0, &macros, (void *)vm_peek(vm, -2, T_REF|T_MEM), &vm->spf->env, &error), error);

	isset = !!spf_isset(macros, vm_peek(vm, -1, T_INT));
	vm_discard(vm, 2);
	vm_push(vm, T_INT, isset);

	vm->pc++;
} /* op_isset() */


static void op_check(struct spf_vm *vm) {
	struct vm_sub sub;
	unsigned end, ret;

	lim_checkterms(vm, 1);

	end = vm->end;
	ret = vm->pc + 1;

	sub_init(&sub, vm);

	/* Query #1
	 *
	 * [-3] reset address
	 * [-2] return address
	 * [-1] domain
	 */
	DUP(&sub);
	I8(&sub, vm->spf->opt.lookup[0]);
	SUBMIT(&sub);
	FETCH(&sub);

	/*
	 * [-4] reset address
	 * [-3] return address
	 * [-2] domain
	 * [-1] packet
	 */
	NIL(&sub);
	TWO(&sub);
	I8(&sub, vm->spf->opt.lookup[0]);
	NEG(&sub); /* -DNS_T_TXT asks grep/next to scan for TXT v=spf1 or SPF  */
	GREP(&sub);
	L0(&sub);
	NEXT(&sub);
	DUP(&sub);
	NOT(&sub);
	sub_jump(&sub, (vm->spf->opt.lookup[1])? 2 : 7);
	COMP(&sub); /* pushes code address, or 0 if failed. */
	DUP(&sub);
	J6(&sub);   /* if not 0, jump to transfer code. */ 
	NOT(&sub);
	J0(&sub);   /* otherwise, continue looping */

	if (!vm->spf->opt.lookup[1])
		goto L6; /* Skip Query #2 bytecode generation */

	/* Query #2
	 *
	 * [-6] reset address
	 * [-5] return address
	 * [-4] domain
	 * [-3] packet
	 * [-2] iterator
	 * [-1] rdata
	 */
	L2(&sub);
	POP(&sub);
	POP(&sub);
	POP(&sub);
	DUP(&sub);
	I8(&sub, vm->spf->opt.lookup[1]);
	SUBMIT(&sub);
	FETCH(&sub);

	/*
	 * [-4] reset address
	 * [-3] return address
	 * [-2] domain
	 * [-1] packet
	 */
	NIL(&sub);
	TWO(&sub);
	I8(&sub, vm->spf->opt.lookup[1]);
	NEG(&sub); /* -DNS_T_TXT asks grep/next to scan for TXT v=spf1 or SPF  */
	GREP(&sub);
	L3(&sub);
	NEXT(&sub);
	DUP(&sub);
	NOT(&sub);
	J7(&sub);
	COMP(&sub); /* pushes code address, or 0 if failed. */
	DUP(&sub);
	J6(&sub);   /* if not 0, jump to transfer code. */ 
	NOT(&sub);
	J3(&sub);   /* otherwise, continue looping */

	/* Transfer
	 * 
	 * [-6] reset address
	 * [-5] return address
	 * [-4] domain
	 * [-3] packet
	 * [-2] iterator
	 * [-1] code address
	 */
L6:	L6(&sub);
	SWAP(&sub);
	POP(&sub);
	SWAP(&sub);
	POP(&sub);
	SWAP(&sub);
	POP(&sub);
	TRUE(&sub);
	SWAP(&sub);
	 /*
	  * [-4] reset address
	  * [-3] return address
	  * [-2] true
	  * [-1] code address
	  */
	GOTO(&sub);

	/* Return None
	 *
	 * [-6] reset address
	 * [-5] return address
	 * [-4] domain
	 * [-3] packet
	 * [-2] iterator
	 * [-1] rdata
	 */
	L7(&sub);
	POP(&sub);
	POP(&sub);
	POP(&sub);
	POP(&sub);
	NIL(&sub);
	I8(&sub, SPF_NONE);
	TWO(&sub);
	EXIT(&sub);

	sub_link(&sub);

	vm_push(vm, T_INT, end);
	vm_swap(vm);
	vm_push(vm, T_INT, ret);
	vm_swap(vm);

	vm->pc = end;
} /* op_check() */


static void op_comp(struct spf_vm *vm) {
	struct spf_parser parser;
	union spf_term term;
	struct spf_exp exp = { 0 };
	struct spf_redirect redir = { 0 };
	struct vm_sub sub;
	const char *txt;
	int type, error;
	unsigned end;

	end = vm->end;

	vm_assert(vm, (txt = (char *)vm_peek(vm, -1, T_REF|T_MEM)), EINVAL);

	spf_parser_init(&parser, txt, strlen(txt));

	/*
	 * L2 is for matches
	 * L4 is the explanation (i.e. exp= or default).
	 * L6 is to return
	 * L7 is for failed includes
	 */
	sub_init(&sub, vm);

	while ((type = spf_parse(&term, &parser, &error))) {
#if 0
		STR(&sub, (intptr_t)"checking");
		STR(&sub, (intptr_t)spf_strterm(type));
		I8(&sub, ' ');
		CAT(&sub);
		PUTS(&sub);
#endif
		switch (type) {
		case SPF_ALL:
			TRUE(&sub);
			break;
		case SPF_INCLUDE:
			I8(&sub, 'd');
			GETENV(&sub);

			STR(&sub, (intptr_t)&term.include.domain[0]);
			if (term.macros) {
				if (spf_isset(term.macros, 'p'))
					FCRD(&sub);
				EXPAND(&sub);
			}
			DUP(&sub);
			I8(&sub, 'd');
			SETENV(&sub);

			sub_emit(&sub, OP_CHECK);
			SWAP(&sub);
			POP(&sub);    /* discard exp */

			DUP(&sub);
			I8(&sub, SPF_NONE);
			EQ(&sub);
			J7(&sub); /* no policy found */

			SWAP(&sub);
			I8(&sub, 'd');
			SETENV(&sub); /* replace our ${d} */

			I8(&sub, SPF_PASS);
			EQ(&sub);

			break;
		case SPF_A:
			/* FALL THROUGH */
		case SPF_MX:
			I8(&sub, term.mx.prefix6);
			I8(&sub, term.mx.prefix4);
			if (term.mx.domain[0]) {
				STR(&sub, (intptr_t)&term.mx.domain[0]);
				if (term.macros) {
					if (spf_isset(term.macros, 'p'))
						FCRD(&sub);
					EXPAND(&sub);
				}
			} else {
				STR(&sub, (intptr_t)"%{d}");
				EXPAND(&sub);
			}
			sub_emit(&sub, (type == SPF_A)? OP_A : OP_MX);
			break;
		case SPF_PTR:
			FCRD(&sub);
			if (term.ptr.domain[0]) {
				STR(&sub, (intptr_t)&term.ptr.domain[0]);
				if (term.macros)
					EXPAND(&sub);
			} else {
				STR(&sub, (intptr_t)"%{d}");
				EXPAND(&sub);
			}
			sub_emit(&sub, OP_PTR);
			break;
		case SPF_IP4:
			I32(&sub, (intptr_t)term.ip4.addr.s_addr);
			I8(&sub, term.ip4.prefix);
			sub_emit(&sub, OP_IP4);
			break;
		case SPF_IP6:
			IN6(&sub, (intptr_t)&term.ip6.addr.s6_addr);
			I8(&sub, term.ip6.prefix);
			sub_emit(&sub, OP_IP6);
			break;
		case SPF_EXISTS:
			STR(&sub, (intptr_t)&term.exists.domain[0]);
			if (term.macros) {
				if (spf_isset(term.macros, 'p'))
					FCRD(&sub);
				EXPAND(&sub);
			}
			sub_emit(&sub, OP_EXISTS);
			break;
		case SPF_EXP:
			if (exp.type) {
				vm_assert(vm, spf_expand(vm->spf->info.error.exp, sizeof vm->spf->info.error.exp, &(spf_macros_t){ 0 }, "multiple exp terms in %{d} policy", &vm->spf->env, &error), error);
				vm_throw(vm, SPF_EBADPOLICY);
			}
			exp = term.exp;
			continue;
		case SPF_REDIRECT:
			if (redir.type) {
				vm_assert(vm, spf_expand(vm->spf->info.error.exp, sizeof vm->spf->info.error.exp, &(spf_macros_t){ 0 }, "multiple redirect terms in %{d} policy", &vm->spf->env, &error), error);
				vm_throw(vm, SPF_EBADPOLICY);
			}
			redir = term.redirect;
			continue;
		default:
			SPF_SAY("unknown term: %d", type);
			continue;
		} /* switch (type) */

		/* [-1] matched */
		I8(&sub, term.result);
		SWAP(&sub);
		J2(&sub);
		POP(&sub);
	}

	if (error) {
		struct spf_sbuf exp;
		char *p;

		vm->end = end;
		end = 0;

		/*
		 * Pretty print the parsing error.
		 */
		sbuf_init(&exp);
		sbuf_puts(&exp, vm->spf->env.d);
		sbuf_puts(&exp, " policy syntax error: `");

		for (p = parser.error.near; p < spf_endof(parser.error.near) && *p; p++) {
			if (p == &parser.error.near[parser.error.lp]) {
				sbuf_putc(&exp, '[');
				sbuf_putc(&exp, *p);
				sbuf_putc(&exp, ']');
			} else
				sbuf_putc(&exp, *p);
		}

		sbuf_putc(&exp, '\'');

		spf_strlcpy(vm->spf->info.error.exp, exp.str, sizeof vm->spf->info.error.exp);

		vm_throw(vm, error);
	}

	if (redir.type) {
		I8(&sub, 'd');
		GETENV(&sub);

		STR(&sub, (intptr_t)&redir.domain[0]);
		if (redir.macros) {
			if (spf_isset(redir.macros, 'p'))
				FCRD(&sub);
			EXPAND(&sub);
		}
		DUP(&sub);
		I8(&sub, 'd');
		SETENV(&sub);

		sub_emit(&sub, OP_CHECK);

		DUP(&sub);
		I8(&sub, SPF_NONE);
		EQ(&sub);
		J7(&sub); /* no policy found */

		I8(&sub, 3);
		NEG(&sub);
		MOVE(&sub);
		I8(&sub, 'd');
		SETENV(&sub);

		TRUE(&sub);
		J6(&sub);
	}

	/*
	 * No matches.
	 */
#if 0
	STR(&sub, (intptr_t)"no match");
	PUTS(&sub);
#endif
	I8(&sub, SPF_NEUTRAL);
	TRUE(&sub);
	J4(&sub);

	L2(&sub);
#if 0
	DUP(&sub);
	STR(&sub, (intptr_t)"match : result=");
	SWAP(&sub);
	I8(&sub, ' ');
	CAT(&sub);
	PUTS(&sub);
#endif

	/*
	 * exp
	 *
	 * [-3] reset address
	 * [-2] return address
	 * [-1] result
	 */
	L4(&sub);
	NIL(&sub);  /* queue NIL exp */
	SWAP(&sub);
	DUP(&sub);
	I8(&sub, SPF_FAIL);
	EQ(&sub);
	NOT(&sub);
	J6(&sub);   /* not a fail, so jump to end with our NIL exp */
	SWAP(&sub);
	POP(&sub);  /* otherwise discard the NIL exp */

	if (exp.type) {
		STR(&sub, (intptr_t)&exp.domain[0]);
		if (exp.macros) {
			if (spf_isset(exp.macros, 'p'))
				FCRD(&sub);
			EXPAND(&sub);
		}
		sub_emit(&sub, OP_EXP);
		SWAP(&sub);
	} else {
		REF(&sub, (intptr_t)SPF_DEFEXP);
		EXPAND(&sub);
		SWAP(&sub);
	}

	L6(&sub);
	TWO(&sub);
	EXIT(&sub);

	L7(&sub);
	STR(&sub, (intptr_t)"no policy found for ");
	I8(&sub, 'd');
	GETENV(&sub);
	CAT(&sub);
	I32(&sub, abs(SPF_ENOPOLICY));
	NEG(&sub);
	SWAP(&sub);
	ERROR(&sub);
	TRAP(&sub);

	sub_link(&sub);

	/*
	 * We should always be called in conjunction with OP_CHECK. OP_COMP
	 * returns the address of the new code, which OP_CHECK will jump
	 * into (with the reset and return addresses properly set). If
	 * compiling fails, 0 is returned to OP_CHECK.
	 */
	vm_discard(vm, 1);
	vm_push(vm, T_INT, end);

	vm->pc++;
} /* op_comp() */


static void op_ip4(struct spf_vm *vm) {
	struct in_addr a, b;
	unsigned prefix;
	int match;

	prefix   = vm_pop(vm, T_INT);
	a.s_addr = vm_pop(vm, T_INT);

	if (!strcmp(vm->spf->env.v, "in-addr")) {
		spf_pto4(&b, vm->spf->env.i);
		match = (0 == spf_4cmp(&a, &b, prefix));
	} else
		match = 0;

	vm_push(vm, T_INT, match);

	vm->pc++;
} /* op_ip4() */


static void op_ip6(struct spf_vm *vm) {
	struct in6_addr *a, b;
	unsigned prefix;
	int match;

	a = (struct in6_addr *)vm_peek(vm, -2, T_REF|T_MEM);
	prefix = vm_peek(vm, -1, T_INT);

	if (!strcmp(vm->spf->env.v, "ip6")) {
		spf_pto6(&b, vm->spf->env.i);
		match = (0 == spf_6cmp(a, &b, prefix));
	} else
		match = 0;

	vm_discard(vm, 2);
	vm_push(vm, T_INT, match);

	vm->pc++;
} /* op_ip6() */


static void op_exists(struct spf_vm *vm) {
	struct vm_sub sub;
	unsigned end, ret;

	lim_checkterms(vm, 1);

	end = vm->end;
	ret = vm->pc + 1;

	sub_init(&sub, vm);

	/*
	 * [-3] reset address
	 * [-2] return address
	 * [-1] domain
	 */
	I8(&sub, DNS_T_A);
	SUBMIT(&sub);
	FETCH(&sub);
	NIL(&sub);
	TWO(&sub);
	I8(&sub, DNS_T_A);
	GREP(&sub);
	NEXT(&sub);

	/*
	 * [-5] reset address
	 * [-4] return address
	 * [-3] packet
	 * [-2] iterator
	 * [-1] rdata
	 */
	SWAP(&sub);
	POP(&sub);
	SWAP(&sub);
	POP(&sub);
	NOT(&sub); /* to... */
	NOT(&sub); /* ...boolean */
	ONE(&sub);
	EXIT(&sub);

	sub_link(&sub);

	vm_push(vm, T_INT, end);
	vm_swap(vm);
	vm_push(vm, T_INT, ret);
	vm_swap(vm);

	vm->pc = end;
} /* op_exists() */


static void op_a_mxv(struct spf_vm *vm) {
	int prefix6 = vm_peek(vm, -3, T_INT);
	int prefix4 = vm_peek(vm, -2, T_INT);
	struct addrinfo *ent = (void *)vm_peek(vm, -1, T_REF|T_MEM);
	union { struct in_addr a4; struct in6_addr a6; } a, b;
	int af, prefix, match = 0;

	if (!strcmp(vm->spf->env.v, "ip6")) {
		af     = AF_INET6;
		prefix = prefix6;
	} else {
		af     = AF_INET;
		prefix = prefix4;
	}

	if (ent->ai_addr->sa_family != af)
		goto done;

	spf_pton(&a, af, vm->spf->env.c);

	if (af == AF_INET6)
		b.a6 = ((struct sockaddr_in6 *)ent->ai_addr)->sin6_addr;
	else
		b.a4 = ((struct sockaddr_in *)ent->ai_addr)->sin_addr;

	match = (0 == spf_addrcmp(af, &a, &b, prefix));
done:
	vm_discard(vm, 3);
	vm_push(vm, T_INT, match);

	vm->pc++;
} /* op_a_mxv() */


static void op_a_mx(struct spf_vm *vm, enum dns_type type) {
	struct vm_sub sub;
	unsigned end, ret;

	end = vm->end;
	ret = vm->pc + 1;

	sub_init(&sub, vm);

	/*
	 * [-5] reset address
	 * [-4] return address
	 * [-3] prefix6
	 * [-2] prefix4
	 * [-1] domain
	 */
	I8(&sub, 0);
	I8(&sub, type);
	I8(&sub, (!strcmp(vm->spf->env.v, "ip6"))? PF_INET6 : PF_INET);
	sub_emit(&sub, OP_ADDRINFO);

	L0(&sub);
	sub_emit(&sub, OP_NEXTENT);
	DUP(&sub);
	NOT(&sub);
	J1(&sub);
	/* push prefix6 */
	THREE(&sub);
	NEG(&sub);
	LOAD(&sub);
	SWAP(&sub);
	/* push prefix4 */
	THREE(&sub);
	NEG(&sub);
	LOAD(&sub);
	SWAP(&sub);
	/* call MXv with [-3] prefix6 [-2] prefix4 [-1] ent */
#if 0
	DUP(&sub);
	sub_emit(&sub, OP_PUTA);
#endif
	sub_emit(&sub, OP_A_MXv);
	NOT(&sub);
	J0(&sub);
	TRUE(&sub);
	TRUE(&sub);
	J1(&sub);

	L1(&sub);
	NOT(&sub); /* to... */
	NOT(&sub); /* ...boolean */
	SWAP(&sub);
	POP(&sub);
	SWAP(&sub);
	POP(&sub);
	ONE(&sub);
	EXIT(&sub);

	sub_link(&sub);

	vm_push(vm, T_INT, end);
	vm_push(vm, T_INT, ret);
	vm_move(vm, -5);
	vm_move(vm, -5);
	vm_move(vm, -5);

	vm->pc = end;
} /* op_a_mx() */


static void op_a(struct spf_vm *vm) {
	lim_checkterms(vm, 1);
	op_a_mx(vm, (!strcmp(vm->spf->env.v, "ip6"))? DNS_T_AAAA : DNS_T_A);
} /* op_a() */


static void op_mx(struct spf_vm *vm) {
	lim_checkterms(vm, 1);
	op_a_mx(vm, DNS_T_MX);
} /* op_mx() */


static void op_ptr(struct spf_vm *vm) {
	const char *arg;
	struct dns_rr rr;
	char dn[DNS_D_MAXNAME + 1], cn[DNS_D_MAXNAME + 1];
	int error, match = 0;

	lim_checkterms(vm, 0);

	vm_assert(vm, vm->spf->fcrd.done, EFAULT);
	vm_assert(vm, (arg = (char *)vm_peek(vm, -1, T_REF|T_MEM)), EFAULT);

	spf_strlcpy(dn, arg, sizeof dn);
	spf_fixdn(dn, dn, sizeof dn, SPF_DN_ANCHOR);

	dns_rr_foreach(&rr, &vm->spf->fcrd.ptr, .section = DNS_S_ANSWER) {
		vm_assert(vm, dns_d_expand(cn, sizeof cn, rr.dn.p, &vm->spf->fcrd.ptr, &error), error);

		do {
			if ((match = !strcasecmp(dn, cn)))
				goto done;
		} while (spf_fixdn(cn, cn, sizeof cn, SPF_DN_SUPER));
	}

done:
	vm_discard(vm, 1);
	vm_push(vm, T_INT, match);

	vm->spf->stat.query.terms++;
	vm->pc++;
} /* op_ptr() */


static void op_fcrdx(struct spf_vm *vm) {
	struct addrinfo *ent = (void *)vm_peek(vm, -1, T_REF|T_MEM);
	union { struct in_addr a4; struct in6_addr a6; } a, b;
	int af, rtype, error;

	if (!strcmp(vm->spf->env.v, "ip6")) {
		af    = AF_INET6;
		rtype = DNS_T_AAAA;
	} else {
		af    = AF_INET;
		rtype = DNS_T_A;
	}

	if (ent->ai_addr->sa_family != af)
		goto done;

	spf_pton(&a, af, vm->spf->env.c);

	if (af == AF_INET6)
		b.a6 = ((struct sockaddr_in6 *)ent->ai_addr)->sin6_addr;
	else
		b.a4 = ((struct sockaddr_in *)ent->ai_addr)->sin_addr;

	if (0 != spf_addrcmp(af, &a, &b, 128))
		goto done;
	
	vm_assert(vm, !(error = dns_p_push(&vm->spf->fcrd.ptr, DNS_S_AN, ent->ai_canonname, strlen(ent->ai_canonname), rtype, DNS_C_IN, 0, &b)), error);

	/*
	 * FIXME: We need to give preference to a verified domain which is
	 * the same as %{d}, or a sub-domain of %{d}. HOWEVER, include: and
	 * require= recursion temporarily replace %{d}, so we need to copy
	 * the _original_ %{d} somewhere for comparing.
	 */
	if (!*vm->spf->env.p || !strcmp(vm->spf->env.p, "unknown"))
		spf_strlcpy(vm->spf->env.p, ent->ai_canonname, sizeof vm->spf->env.p);
done:
	vm_discard(vm, 1);

	vm->pc++;
} /* op_fcrdx() */


static void op_fcrd(struct spf_vm *vm) {
	struct vm_sub sub;
	unsigned end, ret;

	if (vm->spf->fcrd.done)
		{ vm->pc++; return; }

	end = vm->end;
	ret = vm->pc + 1;

	sub_init(&sub, vm);

	REF(&sub, (intptr_t)"%{ir}.%{v}.arpa.");
	EXPAND(&sub);
	I8(&sub, 0);
	I8(&sub, DNS_T_PTR);
	I8(&sub, (!strcmp(vm->spf->env.v, "ip6"))? PF_INET6 : PF_INET);
	sub_emit(&sub, OP_ADDRINFO);
	L0(&sub);
	sub_emit(&sub, OP_NEXTENT);
	DUP(&sub);
	NOT(&sub);
	J1(&sub);
	FCRDx(&sub);
	TRUE(&sub);
	J0(&sub);
	L1(&sub);
	POP(&sub);
	ZERO(&sub);
	EXIT(&sub); /* [-1] return address [-2] reset address */

	sub_link(&sub);

	vm->spf->fcrd.done = 1;

	vm_push(vm, T_INT, end);
	vm_push(vm, T_INT, ret);

	vm->pc = end;
} /* op_fcrd() */


static void op_exp(struct spf_vm *vm) {
	struct vm_sub sub;
	unsigned end, ret;

	end = vm->end;
	ret = vm->pc + 1;

	sub_init(&sub, vm);

	/*
	 * Query for TXT record
	 * 	[-1] target
	 */
	L0(&sub);
	I8(&sub, DNS_T_TXT);
	SUBMIT(&sub);
	FETCH(&sub);
	REF(&sub, (intptr_t)"");
	I8(&sub, DNS_S_AN);
	I8(&sub, DNS_T_TXT);
	GREP(&sub);
	NEXT(&sub); // pops 0, pushes rdata (rdata could be NULL)
	SWAP(&sub);
	POP(&sub);  // discard grep iterator
	SWAP(&sub);
	POP(&sub);  // discard DNS packet

	/*
	 * TXT record present?
	 * 	[-1] exp
	 */
	DUP(&sub);  // take a copy
	J1(&sub);   // jump to FCRD check if present
	POP(&sub);  // otherwise, pop and push default string
	REF(&sub, (intptr_t)SPF_DEFEXP);

	/*
	 * Do we need to do FCRD match?
	 * 	[-1] exp
	 */
	L1(&sub);
	DUP(&sub);     // take a copy
	I8(&sub, 'p'); // %{p} macro triggers FCRD
	ISSET(&sub);   // check for macro (pops 2, pushs boolean)
	NOT(&sub);
	J2(&sub);      // if not set, jump to expansion
	FCRD(&sub);    // otherwise do FCRD

	/*
	 * Expand rdata
	 * 	[-1] exp
	 */
	L2(&sub);
	EXPAND(&sub); // pops 1, pushes expansion

	/*
	 * Epilog.
	 * 	[-1] exp
	 */
	ONE(&sub);  // returning one result
	EXIT(&sub); // expects [-1] result [-2] return address [-3] reset address

	sub_link(&sub);

	/*
	 * Call above routine.
	 * 	[-3] code reset address
	 * 	[-2] return address
	 * 	[-1] target
	 */
	vm_push(vm, T_INT, end);
	vm_swap(vm);
	vm_push(vm, T_INT, ret);
	vm_swap(vm);

	vm->pc = end;
} /* op_exp() */


static void op_sleep(struct spf_vm *vm) {
	sleep(vm_pop(vm, T_INT));
	vm->pc++;
} /* op_sleep() */


static void op_gets(struct spf_vm *vm) {
	char sbuf[1024];

	if (fgets(sbuf, sizeof sbuf, stdin)) {
		spf_rtrim(sbuf, "\r\n");
		vm_strdup(vm, sbuf);
	} else if (feof(stdin)) {
		vm_push(vm, T_REF, 0);
	} else
		vm_throw(vm, errno);

	vm->pc++;
} /* op_gets() */


static void op_cat(struct spf_vm *vm) {
	struct spf_sbuf sbuf = SBUF_INIT(&sbuf);

	/* Print [-2] as string */
	if ((T_REF|T_MEM) & vm_typeof(vm, -2))
		sbuf_puts(&sbuf, (char *)vm_peek(vm, -2, T_REF|T_MEM));
	else
		sbuf_puti(&sbuf, vm_peek(vm, -2, T_ANY));

	/* Print [-1] as string */
	if ((T_REF|T_MEM) & vm_typeof(vm, -1))
		sbuf_puts(&sbuf, (char *)vm_peek(vm, -1, T_REF|T_MEM));
	else
		sbuf_puti(&sbuf, vm_peek(vm, -1, T_ANY));

	vm_assert(vm, !sbuf.overflow, ENOMEM);
	vm_discard(vm, 2);
	vm_strdup(vm, sbuf.str);

	vm->pc++;
} /* op_cat() */


static void op_cmp(struct spf_vm *vm) {
	char *a, *b;
	int cmp;
	a = (char *)vm_peek(vm, -2, T_REF|T_MEM);
	b = (char *)vm_peek(vm, -1, T_REF|T_MEM);
	cmp = strcmp(a, b);
	vm_discard(vm, 2);
	vm_push(vm, T_INT, cmp);
	vm->pc++;
} /* op_cmp() */


static void op_lc(struct spf_vm *vm) {
	char *s;
	s = (char *)vm_peek(vm, -1, T_REF|T_MEM);
	spf_tolower((char *)vm_strdup(vm, s));
	vm_swap(vm);
	vm_pop(vm, T_REF|T_MEM);
	vm->pc++;
} /* op_lc() */


static void op_puti(struct spf_vm *vm) {
	if ((T_REF|T_MEM) & vm_typeof(vm, -1))
		printf("%p\n", (void *)vm_pop(vm, (T_REF|T_MEM)));
	else
		printf("%ld\n", (long)vm_pop(vm, T_ANY));
	vm->pc++;
} /* op_puti() */


static void op_puts(struct spf_vm *vm) {
	printf("%s\n", (char *)vm_peek(vm, -1, T_REF|T_MEM));
	vm_pop(vm, T_ANY);
	vm->pc++;
} /* op_puts() */


static void op_putp(struct spf_vm *vm) {
	struct dns_packet *pkt = (void *)vm_peek(vm, -1, T_REF|T_MEM);
	enum dns_section section;
	struct dns_rr rr;
	int error;
	char pretty[1024];
	size_t len;

	section	= 0;

	dns_rr_foreach(&rr, pkt) {
		if (section != rr.section)
			printf("\n;; [%s:%d]\n", dns_strsection(rr.section), dns_p_count(pkt, rr.section));

		if ((len = dns_rr_print(pretty, sizeof pretty, &rr, pkt, &error)))
			printf("%s\n", pretty);

		section	= rr.section;
	}

	vm_discard(vm, 1);

	vm->pc++;
} /* op_putp() */


static void op_puta(struct spf_vm *vm) {
	struct addrinfo *ent = (void *)vm_peek(vm, -1, T_REF|T_MEM);
	char pretty[1024];

	dns_ai_print(pretty, sizeof pretty, ent, vm->spf->ai.res);
	printf("%s", pretty);

	vm_discard(vm, 1);

	vm->pc++;
} /* op_puta() */


static void op_rstr(struct spf_vm *vm) {
	vm_strdup(vm, spf_strresult(vm_pop(vm, T_INT)));

	vm->pc++;
} /* op_rstr() */


static void op_atoi(struct spf_vm *vm) {
	unsigned long i;

	i = spf_atoi((char *)vm_peek(vm, -1, T_REF|T_MEM));
	vm_pop(vm, T_REF|T_MEM);
	vm_push(vm, T_INT, i);

	vm->pc++;
} /* op_atoi() */


static void op_4top(struct spf_vm *vm) {
	char sbuf[INET_ADDRSTRLEN + 1];

	spf_4top(sbuf, sizeof sbuf, (void *)vm_peek(vm, -1, T_REF|T_MEM));
	vm_pop(vm, T_REF|T_MEM);
	vm_strdup(vm, sbuf);

	vm->pc++;
} /* op_4top() */


static void op_pto4(struct spf_vm *vm) {
	struct in_addr in;

	spf_pto4(&in, (void *)vm_peek(vm, -1, T_REF|T_MEM));
	vm_pop(vm, T_REF|T_MEM);
	vm_memdup(vm, &in, sizeof in);

	vm->pc++;
} /* op_pto4() */


static void op_6top(struct spf_vm *vm) {
	char sbuf[INET6_ADDRSTRLEN + 1];

	spf_6top(sbuf, sizeof sbuf, (void *)vm_peek(vm, -1, T_REF|T_MEM), SPF_6TOP_MIXED);
	vm_pop(vm, T_REF|T_MEM);
	vm_strdup(vm, sbuf);

	vm->pc++;
} /* op_6top() */


static void op_pto6(struct spf_vm *vm) {
	struct in6_addr in;

	spf_pto6(&in, (void *)vm_peek(vm, -1, T_REF|T_MEM));
	vm_pop(vm, T_REF|T_MEM);
	vm_memdup(vm, &in, sizeof in);

	vm->pc++;
} /* op_pto6() */


static void op_error(struct spf_vm *vm) {
	int error = vm_peek(vm, -2, T_INT);
	char *exp = (char *)vm_peek(vm, -1, T_REF|T_MEM);

	spf_strlcpy(vm->spf->info.error.exp, (exp)? exp : "", sizeof vm->spf->info.error.exp);

	vm->pc++;

	vm_throw(vm, error);
} /* op_error() */


static const struct {
	const char *name;
	void (*exec)(struct spf_vm *);
} vm_op[] = {
	[OP_HALT]  = { "halt", 0 },
	[OP_TRAP]  = { "trap", &op_trap },
	[OP_NOOP]  = { "noop", &op_noop },
	[OP_PC]    = { "pc", &op_pc, },
	[OP_CALL]  = { "call", &op_call, },
	[OP_RET]   = { "ret", &op_ret, },
	[OP_EXIT]  = { "exit", &op_exit, },

	[OP_TRUE]  = { "true", &op_lit, },
	[OP_FALSE] = { "false", &op_lit, },
	[OP_ZERO]  = { "zero", &op_lit, },
	[OP_ONE]   = { "one", &op_lit, },
	[OP_TWO]   = { "two", &op_lit, },
	[OP_THREE] = { "three", &op_lit, },

	[OP_I8]    = { "i8", &op_lit, },
	[OP_I16]   = { "i16", &op_lit, },
	[OP_I32]   = { "i32", &op_lit, },
	[OP_NIL]   = { "nil", &op_lit, },
	[OP_REF]   = { "ref", &op_lit, },
	[OP_MEM]   = { "mem", &op_lit, },
	[OP_STR]   = { "str", &op_str, },
	[OP_IN4]   = { "in4", &op_in4, },
	[OP_IN6]   = { "in6", &op_in6, },

	[OP_DEC]   = { "dec", &op_dec, },
	[OP_INC]   = { "inc", &op_inc, },
	[OP_NEG]   = { "neg", &op_neg, },
	[OP_ADD]   = { "add", &op_add, },
	[OP_NOT]   = { "not", &op_not, },

	[OP_EQ]   = { "eq", &op_eq, },
	[OP_LT]   = { "lt", &op_lt, },

	[OP_JMP]   = { "jmp", &op_jmp, },
	[OP_GOTO]  = { "goto", &op_goto, },

	[OP_POP]   = { "pop", &op_pop, },
	[OP_DUP]   = { "dup", &op_dup, },
	[OP_LOAD]  = { "load", &op_load, },
	[OP_STORE] = { "store", &op_store, },
	[OP_MOVE]  = { "move", &op_move, },
	[OP_SWAP]  = { "swap", &op_swap, },

	[OP_GETENV] = { "getenv", &op_getenv, },
	[OP_SETENV] = { "setenv", &op_setenv, },

	[OP_EXPAND] = { "expand", &op_expand, },
	[OP_ISSET]  = { "isset", &op_isset, },

	[OP_SUBMIT] = { "submit", &op_submit, },
	[OP_FETCH]  = { "fetch", &op_fetch, },
	[OP_QNAME]  = { "qname", &op_qname, },
	[OP_GREP]   = { "grep", &op_grep, },
	[OP_NEXT]   = { "next", &op_next, },

	[OP_ADDRINFO] = { "addrinfo", &op_addrinfo, },
	[OP_NEXTENT]  = { "nextent", &op_nextent, },

	[OP_IP4]    = { "ip4", &op_ip4, },
	[OP_IP6]    = { "ip6", &op_ip6, },
	[OP_EXISTS] = { "exists", &op_exists, },
	[OP_A]      = { "a", &op_a },
	[OP_MX]     = { "mx", &op_mx },
	[OP_A_MXv]  = { "mxv", &op_a_mxv },
	[OP_PTR]    = { "ptr", &op_ptr },

	[OP_FCRD]  = { "fcrd", &op_fcrd, },
	[OP_FCRDx] = { "fcrdx", &op_fcrdx, },

	[OP_CHECK]  = { "check", &op_check, },
	[OP_COMP]   = { "comp", &op_comp, },

	[OP_SLEEP] = { "sleep", &op_sleep },

	[OP_CAT]  = { "cat", &op_cat, },
	[OP_CMP]  = { "cmp", &op_cmp, },
	[OP_LC]   = { "lc", &op_lc, },
	[OP_GETS] = { "gets", &op_gets, },
	[OP_PUTI] = { "puti", &op_puti, },
	[OP_PUTS] = { "puts", &op_puts, },
	[OP_PUTP] = { "putp", &op_putp, },
	[OP_PUTA] = { "puta", &op_puta, },
	[OP_RSTR] = { "rstr", &op_rstr, },
	[OP_ATOI] = { "atoi", &op_atoi, },
	[OP_4TOP] = { "4top", &op_4top, },
	[OP_PTO4] = { "pto4", &op_pto4, },
	[OP_6TOP] = { "6top", &op_6top, },
	[OP_PTO6] = { "pto6", &op_pto6, },

	[OP_ERROR] = { "error", &op_error, },

	[OP_EXP] = { "exp", &op_exp, },
}; /* vm_op[] */

SPF_NOTUSED static const char *vm_strcode(int code) {
	return vm_op[code].name;
} /* vm_strcode() */

static int vm_icode(const char *name) {
	int code;

	for (code = 0; code < (int)spf_lengthof(vm_op); code++) {
		if (vm_op[code].name && !strcasecmp(name, vm_op[code].name))
			return code;
	}

	return -1;
} /* vm_icode() */


static int vm_exec(struct spf_vm *vm) {
	enum vm_opcode code;
	int error;

	if ((error = _setjmp(vm->trap))) {
		SPF_SAY("trap: %s", spf_strerror(error));
		return error;
	}

	while ((code = vm_opcode(vm))) {
		if (spf_unlikely(SPF_DEBUG >= 2)) {
			SPF_SAY("code: %-7s (%u)", vm_op[code].name, vm->pc);
		}
		vm_op[code].exec(vm);
	}

	return 0;
} /* vm_exec() */


/*
 * R E S O L V E R  R O U T I N E S
 *
 * NOTE: `struct spf_resolver' is forward-defined at the beginning of the VM
 * section.
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define SPF_SAFELIMITS { .query = { .terms = 10, .cnames = 10 }, }

const struct spf_limits spf_safelimits = SPF_SAFELIMITS;

const struct spf_options spf_defaults = {
	.limit  = SPF_SAFELIMITS,
	.lookup = { SPF_RR_TXT, SPF_RR_SPF },
}; /* spf_defaults */

struct spf_resolver *spf_open(const struct spf_env *env, struct dns_resolver *res, const struct spf_options *opts, int *error_) {
	struct spf_resolver *spf = 0;
	int error;

	if (res)
		dns_res_acquire(res);

	if (!(spf = malloc(sizeof *spf)))
		goto syerr;

	memset(spf, 0, sizeof *spf);

	spf->opt = (opts)? *opts : spf_defaults;

	spf->env = *env;

	vm_init(&spf->vm, spf);

	if (res) {
		spf->res = res; res = NULL;
	} else if (!(spf->res = dns_res_stub(NULL, &error)))
		goto error;

	dns_p_init(&spf->fcrd.ptr, sizeof spf->fcrd.buf);

	if ((error = _setjmp(spf->vm.trap)))
		goto error;

	vm_emit(&spf->vm, OP_STR, (intptr_t)"%{d}");
	vm_emit(&spf->vm, OP_EXPAND);
	vm_emit(&spf->vm, OP_CHECK);
	vm_emit(&spf->vm, OP_HALT);

	return spf;
syerr:
	error = errno;
error:
	*error_ = error;

	dns_res_close(res);
	spf_close(spf);

	return 0;
} /* spf_open() */


void spf_close(struct spf_resolver *spf) {
	if (!spf)
		return;

	dns_res_close(spf->res);
	dns_ai_close(spf->ai.res);

	vm_discard(&spf->vm, spf->vm.sp);

	free(spf);
} /* spf_close() */


int spf_check(struct spf_resolver *spf) {
	int error;

	if ((error = vm_exec(&spf->vm))) {
		switch (error) {
		case SPF_EQUERYLIMIT:
			/* FALL THROUGH */
		case SPF_ENOPOLICY:
			/* FALL THROUGH */
		case SPF_EBADPOLICY:
			spf->info.error.code = error;
			spf->result = SPF_PERMERROR;

			return 0;
		case SPF_ESERVFAIL:
			spf->info.error.code = error;
			spf->result = SPF_TEMPERROR;

			return 0;
		default:
			return error;
		}
	}

	if ((error = _setjmp(spf->vm.trap)))
		return error;

	spf->result = vm_peek(&spf->vm, -1, T_INT);
	spf->exp    = (char *)vm_peek(&spf->vm, -2, T_REF|T_MEM);

	return 0;
} /* spf_check() */


enum spf_result spf_result(struct spf_resolver *spf) {
	return spf->result;
} /* spf_result() */


const char *spf_exp(struct spf_resolver *spf) {
	return spf->exp;
} /* spf_exp() */


const struct spf_info *spf_info(struct spf_resolver *spf) {
	return &spf->info;
} /* spf_info() */


int spf_elapsed(struct spf_resolver *spf) {
	return dns_res_elapsed(spf->res);
} /* spf_elapsed() */


void spf_clear(struct spf_resolver *spf) {
	dns_res_clear(spf->res);
} /* spf_clear() */


int spf_events(struct spf_resolver *spf) {
	return dns_res_events(spf->res);
} /* spf_events() */


int spf_pollfd(struct spf_resolver *spf) {
	return dns_res_pollfd(spf->res);
} /* spf_pollfd() */


int spf_poll(struct spf_resolver *spf, int timeout) {
	return dns_res_poll(spf->res, timeout);
} /* spf_poll() */



#if SPF_MAIN

#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#include <ctype.h>	/* isspace(3) */

#include <unistd.h>	/* getopt(3) */

#if SPF_CACHE
#include "cache.h"
#endif


struct {
	const char *resconf;
	const char *cache;
	struct dns_resolver *res;
} MAIN;


#define panic_(fn, ln, fmt, ...) \
	do { fprintf(stderr, fmt "%.1s", (fn), (ln), __VA_ARGS__); _Exit(EXIT_FAILURE); } while (0)

#define panic(...) panic_(__func__, __LINE__, "spf: (%s:%d) " __VA_ARGS__, "\n")


struct dns_cache;

static struct dns_cache *mkcache(void) {
#if SPF_CACHE
	static struct cache *cache;
	int error;

	if (cache)
		return cache_resi(cache);

	if (!MAIN.cache)
		return NULL;

	assert(cache = cache_open(&error));

	if ((error = cache_loadpath(cache, MAIN.cache, NULL, 0)))
		panic("%s: %s", MAIN.cache, strerror(error));

	return cache_resi(cache);
#else
	return NULL;
#endif
} /* mkcache() */


static struct dns_resolv_conf *mkresconf(void) {
	static struct dns_resolv_conf *resconf;
	int error;

	if (resconf)
		return resconf;

	if (MAIN.resconf) {
		assert(resconf = dns_resconf_open(&error));

		if ((error = dns_resconf_loadpath(resconf, MAIN.resconf)))
			panic("%s: %s", MAIN.resconf, strerror(error));
	} else {
		assert(resconf = dns_resconf_local(&error));

		resconf->lookup[2] = resconf->lookup[1];
		resconf->lookup[1] = resconf->lookup[0];
		resconf->lookup[0] = 'c';
	}

	return resconf;
} /* mkresconf() */


static struct dns_resolver *mkres(void) {
	int error;

	if (MAIN.res)
		return MAIN.res;

	MAIN.res = dns_res_open(mkresconf(), dns_hosts_mortal(dns_hosts_local(&error)), dns_hints_mortal(dns_hints_local(mkresconf(), &error)), mkcache(), dns_opts(), &error);

	assert(MAIN.res);

	return MAIN.res;
} /* mkres() */


static void frepc(int ch, int count, FILE *fp)
	{ while (count--) fputc(ch, fp); }

static int vm(const struct spf_env *env, const char *file) {
#define VM_C(name) { #name, name }
	static const struct { const char *name; int value; } ctable[] = {
		VM_C(AF_INET), VM_C(AF_INET6), VM_C(PF_UNSPEC),
		VM_C(PF_INET), VM_C(PF_INET6),
		VM_C(SPF_NONE), VM_C(SPF_NEUTRAL), VM_C(SPF_PASS),
		VM_C(SPF_FAIL), VM_C(SPF_SOFTFAIL), VM_C(SPF_TEMPERROR),
		VM_C(SPF_PERMERROR),
		VM_C(DNS_S_QD), VM_C(DNS_S_AN), VM_C(DNS_S_NS),
		VM_C(DNS_S_AR), VM_C(DNS_S_ALL),
		VM_C(DNS_C_IN), VM_C(DNS_C_ANY),
		VM_C(DNS_T_A), VM_C(DNS_T_NS), VM_C(DNS_T_CNAME),
		VM_C(DNS_T_SOA), VM_C(DNS_T_PTR), VM_C(DNS_T_MX),
		VM_C(DNS_T_TXT), VM_C(DNS_T_AAAA), VM_C(DNS_T_SRV),
		VM_C(DNS_T_SPF), VM_C(DNS_T_ALL),
	};
	FILE *fp = stdin;
	struct spf_resolver *spf;
	struct spf_vm *vm;
	char line[256], *eos;
	long i;
	struct vm_sub sub;
	int code, error;

	if (file && strcmp(file, "-"))
		assert((fp = fopen(file, "r")));

	assert((spf = spf_open(env, mkres(), NULL, &error)));
	vm = &spf->vm;
	vm->end = 0;

	if ((error = _setjmp(spf->vm.trap)))
		panic("vm_exec: %s", spf_strerror(error));

	sub_init(&sub, vm);

	while (fgets(line, sizeof line, fp)) {
		spf_rtrim(line, "\r\n");

		switch (line[0]) {
		case '#': case ';':
			break;
		case '$':
			spf_rtrim(line, " \t");

			for (i = 0; i < spf_lengthof(ctable); i++) {
				if (!strcasecmp(&line[1], ctable[i].name))
					break;
			}

			if (i >= spf_lengthof(ctable))
				panic("%s: unknown constant", line);

			i = ctable[i].value;

			goto number;
		case '-': case '+':
		case '0': case '1': case '2': case '3': case '4':
		case '5': case '6': case '7': case '8': case '9':
			i = labs(strtol(line, &eos, 0));

			if (isalpha((unsigned char)*eos))
				goto search;
number:
			if (i < 4)
				sub_emit(&sub, OP_ZERO + i);
			else if (i < (1U<<8))
				sub_emit(&sub, OP_I8, i);
			else if (i < (1U<<16))
				sub_emit(&sub, OP_I16, i);
			else
				sub_emit(&sub, OP_I32, i);

			if (line[0] == '-')
				sub_emit(&sub, OP_NEG);

			break;
		case '"':
			sub_emit(&sub, OP_STR, (intptr_t)&line[1]);

			break;
		case '\'':
			sub_emit(&sub, OP_I8, (intptr_t)line[1]);

			break;
		case 'L':
			if (!isdigit((unsigned char)line[1]))
				break;

			sub_label(&sub, line[1] - '0');

			break;
		case 'J':
			if (!isdigit((unsigned char)line[1]))
				break;

			sub_jump(&sub, line[1] - '0');

			break;
		default:
search:
			spf_rtrim(line, " \t");

			if (*line) {
				if (-1 != (code = vm_icode(line))) {
					sub_emit(&sub, code);
				} else
					SPF_SAY("%s: unknown opcode", line);
			}
		} /* switch() */
	} /* while() */

	sub_emit(&sub, OP_HALT);

	sub_link(&sub);

	while ((error = vm_exec(vm))) {
		switch (error) {
		case EAGAIN:
			if ((error = dns_res_poll(spf->res, 5)))
				panic("poll: %s", spf_strerror(error));
			break;
		default:
			panic("exec: %s", spf_strerror(error));
		}
	}

	spf_close(spf);

	return 0;
} /* vm() */


static int check(int argc, char *argv[], const struct spf_env *env) {
	struct spf_resolver *spf;
	const struct spf_info *info;
	int error;

	assert((spf = spf_open(env, mkres(), NULL, &error)));

	while ((error = spf_check(spf))) {
		switch (error) {
		case EAGAIN:
			if ((error = spf_poll(spf, 5)))
				panic("poll: %s", spf_strerror(error));
			break;
		default:
			panic("check: %s", spf_strerror(error));
		}
	}

	printf("result: %s\n", spf_strresult(spf_result(spf)));
	printf("exp:    %s\n", (spf_exp(spf))? spf_exp(spf) : "[no exp]");

	info = spf_info(spf);
	if (info->error.code)
		printf("error:  %s\n", info->error.exp);

	spf_close(spf);

	return 0;
} /* check() */


static int parse(const char *txt) {
	struct spf_parser parser;
	union spf_term term;
	struct spf_sbuf sbuf;
	int error;

	spf_parser_init(&parser, txt, strlen(txt));

	while (spf_parse(&term, &parser, &error)) {
		term_comp(sbuf_init(&sbuf), &term);
		puts(sbuf.str);
	}

	if (error) {
		fprintf(stderr, "error near `%s'\n", parser.error.near);
		frepc('.', 11 + parser.error.lp, stderr);
		fputc('^', stderr);
		fputc('\n', stderr);
	}

	return error;
} /* parse() */


static int expand(const char *src, const struct spf_env *env) {
	char dst[512];
	spf_macros_t macros = 0;
	int error;

	if (!(spf_expand(dst, sizeof dst, &macros, src, env, &error)) && error)
		panic("%s: %s", src, spf_strerror(error));	

	fprintf(stdout, "[%s]\n", dst);

	if (SPF_DEBUG >= 2) {
		fputs("macros:", stderr);

		for (unsigned M = 'A'; M <= 'Z'; M++) {
			if (spf_isset(macros, M))
				{ fputc(' ', stderr); fputc(M, stderr); }
		}

		fputc('\n', stderr);
	}

	return 0;
} /* expand() */


static int macros(const char *src, const struct spf_env *env) {
	spf_macros_t macros = 0;
	int error;

	if (!(spf_expand(0, 0, &macros, src, env, &error)) && error)
		panic("%s: %s", src, spf_strerror(error));	

	for (unsigned M = 'A'; M <= 'Z'; M++) {
		if (spf_isset(macros, M)) {
			fputc(M, stdout);
			fputc('\n', stdout);
		}
	}

	return 0;
} /* macros() */


static void ip_flags(int *flags, _Bool *libc, int argc, char *argv[]) {
	for (int i = 0; i < argc; i++) {
		if (!strcmp(argv[i], "nybble"))
			*flags |= SPF_6TOP_NYBBLE;
		else if (!strcmp(argv[i], "compat"))
			*flags |= SPF_6TOP_COMPAT;
		else if (!strcmp(argv[i], "mapped"))
			*flags |= SPF_6TOP_MAPPED;
		else if (!strcmp(argv[i], "mixed"))
			*flags |= SPF_6TOP_MIXED;
		else if (!strcmp(argv[i], "libc"))
			*libc = 1;
	}

	if (*libc && *flags)
		SPF_SAY("libc and nybble/compat/mapped are mutually exclusive");
	else if ((*flags & SPF_6TOP_NYBBLE) && (*flags & SPF_6TOP_MIXED))
		SPF_SAY("nybble and compat/mapped are mutually exclusive");
} /* ip_flags() */


#include <arpa/inet.h>

int ip6(int argc, char *argv[]) {
	struct in6_addr ip;
	char str[64];
	int ret, flags = 0;
	_Bool libc = 0;

	ip_flags(&flags, &libc, argc - 1, &argv[1]);

	memset(&ip, 0xff, sizeof ip);

	if (libc) {
		if (1 != (ret = inet_pton(AF_INET6, argv[0], &ip)))
			panic("%s: %s", argv[0], (ret == 0)? "not v6 address" : spf_strerror(errno));

		inet_ntop(AF_INET6, &ip, str, sizeof str);
	} else {
		spf_pto6(&ip, argv[0]);
		spf_6top(str, sizeof str, &ip, flags);
	}

	puts(str);

	return 0;
} /* ip6() */


int ip4(int argc, char *argv[]) {
	struct in_addr ip;
	char str[16];
	int ret, flags = 0;
	_Bool libc = 0;

	ip_flags(&flags, &libc, argc - 1, &argv[1]);

	if (flags)
		SPF_SAY("nybble/compat/mapped invalid flags for v4 address");

	memset(&ip, 0xff, sizeof ip);

	if (libc) {
		if (1 != (ret = inet_pton(AF_INET, argv[0], &ip)))
			panic("%s: %s", argv[0], (ret == 0)? "not v4 address" : spf_strerror(errno));

		inet_ntop(AF_INET, &ip, str, sizeof str);
	} else {
		spf_pto4(&ip, argv[0]);
		spf_4top(str, sizeof str, &ip);
	}

	puts(str);

	return 0;
} /* ip4() */


int fixdn(int argc, char *argv[]) {
	char dst[(SPF_MAXDN * 2) + 1];
	size_t lim = (SPF_MAXDN + 1), len;
	int flags = 0;

	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "super")) {
			flags |= SPF_DN_SUPER;
		} else if (!strcmp(argv[i], "trunc")) {
			flags |= SPF_DN_TRUNC;
		} else if (!strncmp(argv[i], "trunc=", 6)) {
			flags |= SPF_DN_TRUNC;
			lim = spf_atoi(&argv[i][6]);
		} else if (!strcmp(argv[i], "anchor")) {
			flags |= SPF_DN_ANCHOR;
		} else if (!strcmp(argv[i], "chomp")) {
			flags |= SPF_DN_CHOMP;
		} else
			panic("%s: invalid flag (\"super\", \"trunc[=LIMIT]\", \"anchor\", \"chomp\")", argv[i]);
	}

	len = spf_fixdn(dst, argv[0], SPF_MIN(lim, sizeof dst), flags);

	if (SPF_DEBUG >= 2) {
		if (len < lim || !len)
			SPF_SAY("%zu[%s]\n", len, dst);
		else if (!lim)
			SPF_SAY("-%zu[%s]\n", (len - lim), dst);
		else
			SPF_SAY("-%zu[%s]\n", (len - lim) + 1, dst);
	}

	puts(dst);

	return 0;
} /* fixdn() */


#define SIZE(x) { SPF_STRINGIFY(x), sizeof (struct x) }
#define SIZEu(x) { SPF_STRINGIFY(x), sizeof (union x) }
int sizes(int argc, char *argv[]) {
	static const struct { const char *name; size_t size; } type[] = {
		SIZE(spf_env), SIZE(spf_resolver), SIZE(spf_vm), SIZE(vm_sub),
		SIZEu(spf_term), SIZE(spf_all), SIZE(spf_include), SIZE(spf_a),
		SIZE(spf_mx), SIZE(spf_ptr), SIZE(spf_ip4), SIZE(spf_ip6), 
		SIZE(spf_exists), SIZE(spf_redirect), SIZE(spf_exp), SIZE(spf_unknown), 
	};
	int i, max;

	for (i = 0, max = 0; i < spf_lengthof(type); i++)
		max = SPF_MAX(max, strlen(type[i].name));

	for (i = 0; i < spf_lengthof(type); i++)
		printf("%*s : %zu\n", max, type[i].name, type[i].size);

	return 0;
} /* sizes() */


int printenv(int argc, char *argv[], const struct spf_env *env) {
	spf_printenv(env, stdout);

	return 0;
} /* printenv() */


#define USAGE \
	"spf [-S:L:O:D:I:P:V:H:C:R:T:f:vh] ACTION\n" \
	"  -S EMAIL   <sender>\n" \
	"  -L LOCAL   local-part of <sender>\n" \
	"  -O DOMAIN  domain of <sender>\n" \
	"  -D DOMAIN  <domain>\n" \
	"  -I IP      <ip>\n" \
	"  -P DOMAIN  the validated domain name of <ip>\n" \
	"  -V STR     the string \"in-addr\" if <ip> is ipv4, or \"ip6\" if ipv6\n" \
	"  -H DOMAIN  HELO/EHLO domain\n" \
	"  -C IP      SMTP client IP\n" \
	"  -R DOMAIN  domain name of host performing the check\n" \
	"  -T TIME    current timestamp\n" \
	"  -f PATH    path to file (e.g. to load vm instead of stdin)\n" \
	"  -c PATH    path to resolv.conf\n" \
	"  -z PATH    path to zone cache file\n" \
	"  -W         print version\n" \
	"  -v         be verbose (use more to increase verboseness)\n" \
	"  -h         print usage\n" \
	"\n" \
	"  check           Check SPF policy\n" \
	"  parse <POLICY>  Parse the SPF policy, pretty-print errors\n" \
	"  expand <MACRO>  Expand the SPF macro\n" \
	"  macros <MACRO>  List the embedded macros\n" \
	"  ip6 <ADDR> [\"nybble\" | \"compat\" | \"mapped\" | \"mixed\" | \"libc\"]\n" \
	"                  Parse and compose address according to options\n" \
	"  ip4 <ADDR>      See ip6\n" \
	"  fixdn <POLICY> [\"super\" | \"trunc[=LIMIT]\" | \"anchor\" | \"chomp\"]\n" \
	"                  Operate on domain string\n" \
	"  vm              Assemble STDIN into bytecode and execute\n" \
	"  sizes           Print data structure sizes\n" \
	"  printenv        Print SPF environment\n" \
	"\n" \
	"Reports bugs to william@25thandClement.com\n"


static void version(FILE *fp) {
	fprintf(fp, "spf (spf.c) %.8X\n", spf_v_rel());
	fprintf(fp, "vendor  %s\n", spf_vendor());
	fprintf(fp, "release %.8X\n", spf_v_rel());
	fprintf(fp, "abi     %.8X\n", spf_v_abi());
	fprintf(fp, "api     %.8X\n", spf_v_api());
	fprintf(fp, "dns     %.8X\n", dns_v_rel());
} /* version() */


int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;
	int opt;
	struct spf_env env;
	const char *file = 0;

	memset(&env, 0, sizeof env);

	spf_strlcpy(env.p, "unknown", sizeof env.p);
	spf_strlcpy(env.v, "in-addr", sizeof env.v);
	gethostname(env.r, sizeof env.r);
	spf_itoa(env.t, sizeof env.t, (unsigned)time(0));

	while (-1 != (opt = getopt(argc, argv, "S:L:O:D:I:P:V:H:C:R:T:f:c:z:vWh"))) {
		switch (opt) {
		case 'S':
			{
				char *argv[3];
				char tmp[256];

				spf_strlcpy(tmp, optarg, sizeof tmp);

				if (2 == spf_split(spf_lengthof(argv), argv, tmp, "@", 0)) {
					if (!*env.l)
						spf_strlcpy(env.l, argv[0], sizeof env.l);

					if (!*env.o)
						spf_strlcpy(env.o, argv[1], sizeof env.o);

					if (!*env.d)
						spf_strlcpy(env.d, argv[1], sizeof env.d);

					if (!*env.h)
						spf_strlcpy(env.h, argv[1], sizeof env.h);
				}
			}

			goto setenv;
		case 'L':
			/* FALL THROUGH */
		case 'O':
			/* FALL THROUGH */
		case 'D':
			goto setenv;
		case 'I':
			if (!*env.c)
				spf_strlcpy(env.c, optarg, sizeof env.c);

			goto setenv;
		case 'P':
			/* FALL THROUGH */
		case 'V':
			/* FALL THROUGH */
		case 'H':
			goto setenv;
		case 'C':
			if (!*env.i)
				spf_strlcpy(env.i, optarg, sizeof env.i);

			goto setenv;
		case 'R':
			/* FALL THROUGH */
		case 'T':
setenv:
			spf_setenv(&env, opt, optarg);

			break;
		case 'f':
			file = optarg;

			break;
		case 'c':
			MAIN.resconf = optarg;

			break;
		case 'z':
			MAIN.cache = optarg;

			break;
		case 'v':
			spf_debug++;

			break;
		case 'W':
			version(stdout);

			return 0;
		case 'h':
			/* FALL THROUGH */
		default:
usage:
			fputs(USAGE, stderr);

			return (opt == 'h')? 0 : EXIT_FAILURE;
		} /* switch() */
	} /* while() */

	argc -= optind;
	argv += optind;

	if (!argc) {
		if (file)
			goto vm;

		goto usage;
	}

	if (!strcmp(argv[0], "check")) {
		return check(argc-1, &argv[1], &env);
	} else if (!strcmp(argv[0], "parse") && argc > 1) {
		return parse(argv[1]);
	} else if (!strcmp(argv[0], "expand") && argc > 1) {
		return expand(argv[1], &env);
	} else if (!strcmp(argv[0], "macros") && argc > 1) {
		return macros(argv[1], &env);
	} else if (!strcmp(argv[0], "ip6") && argc > 1) {
		return ip6(argc - 1, &argv[1]);
	} else if (!strcmp(argv[0], "ip4") && argc > 1) {
		return ip4(argc - 1, &argv[1]);
	} else if (!strcmp(argv[0], "fixdn") && argc > 1) {
		return fixdn(argc - 1, &argv[1]);
	} else if (!strcmp(argv[0], "vm")) {
vm:		return vm(&env, file);
	} else if (!strcmp(argv[0], "sizes")) {
		return sizes(argc - 1, &argv[1]);
	} else if (!strcmp(argv[0], "printenv")) {
		return printenv(argc - 1, &argv[1], &env);
	} else
		goto usage;

	return 0;
} /* main() */

#endif /* SPF_MAIN */


#if __clang__
#pragma clang diagnostic pop
#elif (__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4
#pragma GCC diagnostic pop
#endif
