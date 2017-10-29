/* ==========================================================================
 * spf.h - "spf.c", a Sender Policy Framework library.
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
#ifndef SPF_H
#define SPF_H

#include <stddef.h>	/* size_t */
#include <stdio.h>	/* FILE */

#include <netinet/in.h>	/* struct in_addr struct in6_addr */


/*
 * M I S C .  M A C R O S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define SPF_MIN(a, b) (((a) < (b))? (a) : (b))
#define SPF_MAX(a, b) (((a) > (b))? (a) : (b))

#define SPF_MAXDN 255

#define SPF_RR_TXT 16
#define SPF_RR_SPF 99


/*
 * M I S C .  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/** handles both system errors and (enum dns_errno) errors. */
const char *spf_strerror(int);

const char *spf_strterm(int);

const char *spf_strresult(int);

int spf_iresult(const char *);


/*
 * V E R S I O N
 *
 * Vendor: Entity for which versions numbers are relevant. (If forking
 * change SPF_VENDOR to avoid confusion.)
 *
 * Three versions:
 *
 * REL	Official "release"--bug fixes, new features, etc.
 * ABI	Changes to existing object sizes or parameter types.
 * API	Changes that might effect application source.
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define SPF_VENDOR "william@25thandClement.com"

#define SPF_V_REL  0x20160816
#define SPF_V_ABI  0x20100428
#define SPF_V_API  0x20100428


const char *spf_vendor(void);

int spf_v_rel(void);
int spf_v_abi(void);
int spf_v_api(void);


/*
 * E R R O R  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

enum spf_errno {
	SPF_EQUERYLIMIT	= -(('S' << 24) | ('P' << 16) | ('F' << 8) | 64),
	SPF_ENOPOLICY,
	SPF_EBADPOLICY,
	SPF_ESERVFAIL,
	SPF_EVMFAULT,
}; /* spf_errno */

const char *spf_strerror(int);

extern int spf_debug;


/*
 * P A R S I N G / C O M P O S I N G  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

enum spf_result {
	SPF_NONE      = 0,
	SPF_NEUTRAL   = '?',
	SPF_PASS      = '+',
	SPF_FAIL      = '-',
	SPF_SOFTFAIL  = '~',
	SPF_TEMPERROR = 'e',
	SPF_PERMERROR = 'E',
}; /* enum spf_result */


#define SPF_ISMECHANISM(type) ((type) & 0x10)

enum spf_mechanism {
	SPF_ALL = 0x10,
	SPF_INCLUDE,
	SPF_A,
	SPF_MX,
	SPF_PTR,
	SPF_IP4,
	SPF_IP6,
	SPF_EXISTS,
}; /* enum spf_mechanism */


#define SPF_ISMODIFIER(type) ((type) & 0x20)

enum spf_modifier {
	SPF_REDIRECT = 0x20,
	SPF_EXP,
	SPF_UNKNOWN,
}; /* enum spf_modifier */


/** forward definition. See "Macro Interfaces" below. */
typedef unsigned spf_macros_t;


struct spf_all {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;
}; /* struct spf_all */


struct spf_include {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];
}; /* struct spf_include */


struct spf_a {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];

	unsigned prefix4, prefix6;
}; /* struct spf_a */


struct spf_mx {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];

	unsigned prefix4, prefix6;
}; /* struct spf_mx */


struct spf_ptr {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];
}; /* struct spf_ptr */


struct spf_ip4 {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	struct in_addr addr;
	unsigned prefix;
}; /* struct spf_ip4 */


struct spf_ip6 {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	struct in6_addr addr;
	unsigned prefix;
}; /* struct spf_ip6 */


struct spf_exists {
	enum spf_mechanism type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];
}; /* struct spf_exists */


struct spf_redirect {
	enum spf_modifier type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];
}; /* struct spf_redirect */


struct spf_exp {
	enum spf_modifier type;
	enum spf_result result;
	spf_macros_t macros;

	char domain[SPF_MAXDN + 1];
}; /* struct spf_exp */


struct spf_unknown {
	enum spf_modifier type;
	enum spf_result result;
	spf_macros_t macros;

	char name[(SPF_MAXDN / 2) + 1];
	char value[(SPF_MAXDN / 2) + 1];
}; /* struct spf_unknown */


union spf_term {
	struct {
		int type;               /* enum spf_mechanism | enum spf_modifier */
		enum spf_result result; /* (mechanisms only) */
		spf_macros_t macros;
	};

	struct spf_all all;
	struct spf_include include;
	struct spf_a a;
	struct spf_mx mx;
	struct spf_ptr ptr;
	struct spf_ip4 ip4;
	struct spf_ip6 ip6;
	struct spf_exists exists;

	struct spf_redirect redirect;
	struct spf_exp exp;
	struct spf_unknown unknown;
}; /* union spf_term */


struct spf_parser {
	const unsigned char *rdata;
	const unsigned char *p, *pe, *eof;
	int cs;

	struct {
		int lc; /* last character parsed */
		int lp; /* position of last character in near[] */
		int rp; /* position of last character in rdata */
		char near[64];
	} error;
}; /* struct spf_parser */

void spf_parser_init(struct spf_parser *, const void *, size_t);

int spf_parse(union spf_term *, struct spf_parser *, int *);


/*
 * E N V I R O N M E N T  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

struct spf_env {
	char s[64 + 1 + SPF_MAXDN + 1];
	char l[64 + 1];
	char o[SPF_MAXDN + 1];
	char d[SPF_MAXDN + 1];
	char i[63 + 1]; /* IPv6 in long nybble format (32 nybbles + 31 "."s) */
	char p[SPF_MAXDN + 1];
	char v[SPF_MAX(sizeof "in-addr", sizeof "ip6")];
	char h[SPF_MAXDN + 1];

	char c[SPF_MAX(INET_ADDRSTRLEN, INET6_ADDRSTRLEN) + 1];
	char r[SPF_MAXDN + 1];
	char t[32];
}; /* struct spf_env */

int spf_env_init(struct spf_env *, int, const void *, const char *, const char *);

size_t spf_setenv(struct spf_env *, int, const char *);

size_t spf_getenv(char *, size_t, int, const struct spf_env *);

void spf_printenv(const struct spf_env *, FILE *);


/*
 * M A C R O  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/** must manually call tolower(3) if necessary */
#define SPF_M(m) (1U << (unsigned)((m) - 'a'))

/** spf_isset(spf_macros("%{d}"), 'd') returns true */
_Bool spf_isset(spf_macros_t, int);

size_t spf_expand(char *, size_t, spf_macros_t *, const char *, const struct spf_env *, int *);

/** return set of macros used in expansion */
spf_macros_t spf_macros(const char *, const struct spf_env *);


/*
 * R E S O L V E R  I N T E R F A C E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

struct spf_limits {
	struct {
		/* Max. # of term queries. */
		unsigned terms;

		/* Max. # of cname queries per MX or PTR term. (NOT IMPLEMENTED) */
		unsigned cnames;
	} query;
}; /* struct spf_limits */

extern const struct spf_limits spf_safelimits;

struct spf_options {
	struct spf_limits limit;
	int lookup[2]; /* lookup order: SPF_RR_TXT SPF_RR_SPF */
}; /* struct spf_options */

extern const struct spf_options spf_defaults;

struct dns_resolver;
struct spf_resolver;

struct spf_resolver *spf_open(const struct spf_env *, struct dns_resolver *, const struct spf_options *, int *);

void spf_close(struct spf_resolver *);

int spf_check(struct spf_resolver *);

enum spf_result spf_result(struct spf_resolver *);

const char *spf_exp(struct spf_resolver *);

struct spf_info {
	struct {
		enum spf_errno code;
		char exp[128];
	} error;
}; /* struct spf_info */

const struct spf_info *spf_info(struct spf_resolver *);

int spf_elapsed(struct spf_resolver *);

void spf_clear(struct spf_resolver *);

int spf_events(struct spf_resolver *);

int spf_pollfd(struct spf_resolver *);

int spf_poll(struct spf_resolver *, int);


#endif /* SPF_H */
