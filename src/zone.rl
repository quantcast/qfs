/* ==========================================================================
 * zone.rl - RFC 1035 Master File Parser
 * --------------------------------------------------------------------------
 * Copyright (c) 2010  William Ahern
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
#include <stddef.h>	/* NULL */
#include <stdlib.h>	/* malloc(3) free(3) */
#include <stdio.h>	/* fopen(3) fclose(3) fread(3) fputc(3) */

#include <string.h>	/* memset(3) memmove(3) */

#include <ctype.h>	/* isspace(3) isgraph(3) isdigit(3) */

#include <assert.h>	/* assert(3) */

#include <errno.h>	/* errno */

#include <arpa/inet.h>	/* inet_pton(3) */

#include "dns.h"
#include "zone.h"


#ifndef lengthof
#define lengthof(a) (sizeof (a) / sizeof (a)[0])
#endif

#ifndef endof
#define endof(a) (&(a)[lengthof(a)])
#endif

#ifndef MIN
#define MIN(a, b) (((a) < (b))? (a) : (b))
#endif


#define SAY_(fmt, ...) \
	do { fprintf(stderr, fmt "%.1s", __func__, __LINE__, __VA_ARGS__); } while (0)
#define SAY(...) SAY_(">>>> (%s:%d) " __VA_ARGS__, "\n")
#define HAI SAY("HAI")


#if __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#elif (__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#endif


/*
 * P R E - P R O C E S S I N G  R O U T I N E S
 *
 * Outputs an encoded normal form of the RFC1035 master file syntax.
 *
 * o Linear spaces and tabs trimmed.
 *
 * o Multi-line groups are folded, with grouping parentheses replaced by
 *   spaces.
 *
 * o Comments are discarded.
 *
 * o Quoted and escaped whitespace characters are coded as a short with the
 *   T_LIT bit set.
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define T_LIT 0x0100

struct zonepp {
	int escaped, quoted, grouped, comment, lastc;
	struct { int v, n; } octet;
}; /* struct zonepp */

#define islwsp(ch) ((ch) == ' ' || (ch) == '\t')

static size_t fold(short *dst, size_t lim, unsigned char **src, size_t *len, struct zonepp *state) {
	short *p, *pe;
	int ch;

	if (!state->lastc)
		state->lastc = '\n';

	p  = dst;
	pe = p + lim;

	while (p < pe && *len) {
		ch = **src;

		if (state->escaped) {
			if (state->octet.n && (state->octet.n >= 3 || !isdigit(ch))) {
				state->escaped = 0;

				ch = state->octet.v;
				state->octet.v = 0;
				state->octet.n = 0;

				if (isspace(ch))
					ch |= T_LIT;

				*p++ = ch;

				continue; /* don't skip current char */
			} else if (isdigit(ch)) {
				state->octet.v *= 10;
				state->octet.v += ch - '0';
				state->octet.n++;

				goto skip;
			} else {
				state->escaped = 0;

				if (isspace(ch))
					ch |= T_LIT;

				goto copy;
			}
		} else if (state->comment) {
			if (ch != '\n')
				goto skip;

			state->comment = 0;
		}

test:
		switch (ch) {
		case '\\':
			state->escaped = 1;

			goto skip;
		case ';':
			if (state->quoted)
				break;

			state->comment = 1;

			goto skip;
		case '"':
			state->quoted = !state->quoted;

			goto skip;
		case '(':
			if (state->quoted || state->grouped)
				break;

			state->grouped = 1;

			ch = ' ';

			goto test;
		case ')':
			if (state->quoted || !state->grouped)
				break;

			state->grouped = 0;
			
			ch = ' ';

			goto test;
		case '\n':
			if (state->quoted) {
				break;
			} else if (state->lastc == '\n') {
				goto skip;
			} else if (state->grouped) {
				ch = ' ';

				goto test;
			}

			break;
		case '\t':
			ch = ' ';

			goto test;
		case ' ':
			if (state->quoted)
				break;
			else if (islwsp(state->lastc))
				goto skip;

			break;
		} /* switch() */
copy:
		if (state->quoted && isspace((0xff & ch)))
			ch |= T_LIT;

		state->lastc = *p = ch;

		++p;
skip:
		++*src;
		--*len;
	} /* while() */

	return p - dst;
} /* fold() */


struct tokens {
	short base[4096];
	unsigned count;
}; /* struct tokens */


static unsigned tok_eol(struct tokens *b) {
	unsigned i;

	for (i = 0; i < b->count; i++) {
		if (b->base[i] == '\n')
			return i + 1;
	}

	return 0;
} /* tok_eol() */


static void tok_discard(struct tokens *b, unsigned count) {
	if (count) {
		memmove(b->base, &b->base[count], 2 * (b->count - count));
		b->count = b->count - count;
	}
} /* tok_discard() */


struct zonefile {
	unsigned ttl;
	char origin[DNS_D_MAXNAME + 1];
	char lastrr[DNS_D_MAXNAME + 1];

	struct dns_soa soa;

	struct tokens toks;
	struct zonepp pp;
}; /* struct zonefile */


void zone_init(struct zonefile *P, const char *origin, unsigned ttl) {
	memset(P, 0, sizeof *P);

	origin = (origin)? origin : ".";
	dns_strlcpy(P->origin, origin, sizeof P->origin);
	dns_strlcpy(P->lastrr, origin, sizeof P->lastrr);

	P->ttl = (ttl)? ttl : 3600;
} /* zone_init() */


void zonerr_init(struct zonerr *rr, struct zonefile *P) {
	memset(rr, 0, sizeof *rr);
	rr->class = DNS_C_IN;
	rr->ttl   = P->ttl;
	dns_any_init(&rr->data, sizeof rr->data);
} /* zonerr_init() */


%%{
	machine file_grammar;
	alphtype short;

	action oops {
		fprintf(stderr, "OOPS: ");
		do {
			unsigned i, at = p - P->toks.base;
			for (i = 0; i < span; i++) {
				if (i == at) fputc('[', stderr);
				fputc((0xff & P->toks.base[i]), stderr);
				if (i == at) fputc(']', stderr);
			}
		} while(0);
		fputc('\n', stderr);
		goto next;
	}

	action str_init { sp = str; }
	action str_copy { if (sp < endof(str)) *sp++ = fc; lc = fc; }
	action str_end { if (sp >= endof(str)) sp--; *sp = '\0'; }

	string = (any - space)+ >str_init $str_copy %str_end;

	number = digit+ >{ n = 0; } ${ n *= 10; n += fc - '0'; };

	ttl_unit = 
		("w"i @{ ttl *= 604800; }) |
		("d"i @{ ttl *= 86400; }) |
		("h"i @{ ttl *= 3600; }) |
		("m"i @{ ttl *= 60; }) |
		("s"i);
	ttl = number %{ ttl = n; } ttl_unit? space*;

	action dom_end {
		 if (lc != '.') {
			if (P->origin[0] != '.')
				dns_strlcat(str, ".", sizeof str);
			dns_strlcat(str, P->origin, sizeof str); 
		}
	}

	domain = string %dom_end;

	SOA_type    = "SOA"i %{ rr->type = DNS_T_SOA; };
	SOA_mname   = domain %{ dns_strlcpy(rr->data.soa.mname, str, sizeof rr->data.soa.mname); };
	SOA_rname   = string %{ dns_strlcpy(rr->data.soa.rname, str, sizeof rr->data.soa.rname); };
	SOA_serial  = number %{ rr->data.soa.serial = n; };
	SOA_refresh = ttl %{ rr->data.soa.refresh = ttl; };
	SOA_retry   = ttl %{ rr->data.soa.retry = ttl; };
	SOA_expire  = ttl %{ rr->data.soa.expire = ttl; };
	SOA_minimum = ttl %{ rr->data.soa.minimum = ttl; };
	SOA = SOA_type space+ SOA_mname space+ SOA_rname space+ SOA_serial space+ SOA_refresh space+ SOA_retry space+ SOA_expire space+ SOA_minimum space*;

	NS_type = "NS"i %{ rr->type = DNS_T_NS; };
	NS_host = domain %{ dns_strlcpy(rr->data.ns.host, str, sizeof rr->data.ns.host); };
	NS = NS_type space+ NS_host space*;

	MX_type = "MX"i %{ rr->type = DNS_T_MX; };
	MX_pref = number %{ rr->data.mx.preference = n; };
	MX_host = domain %{ dns_strlcpy(rr->data.mx.host, str, sizeof rr->data.mx.host); };
	MX = MX_type space+ MX_pref space+ MX_host space*;

	CNAME_type = "CNAME"i %{ rr->type = DNS_T_CNAME; };
	CNAME_host = domain %{ dns_strlcpy(rr->data.cname.host, str, sizeof rr->data.cname.host); };
	CNAME = CNAME_type space+ CNAME_host space*;

	SRV_type   = "SRV"i %{ rr->type = DNS_T_SRV; };
	SRV_pri    = number %{ rr->data.srv.priority = n; };
	SRV_weight = number %{ rr->data.srv.weight = n; };
	SRV_port   = number %{ rr->data.srv.port = n; };
	SRV_target = domain %{ dns_strlcpy(rr->data.srv.target, str, sizeof rr->data.srv.target); };
	SRV = SRV_type space+ SRV_pri space+ SRV_weight space+ SRV_port space+ SRV_target space*;

	action SSHFP_nybble {
		if (fc >= '0' && fc <= '9') {
			n = (0x0f & (fc - '0'));
		} else if (fc >= 'A' && fc <= 'F') {
			n = (0x0f & (10 + (fc - 'A')));
		} else if (fc >= 'a' && fc <= 'f') {
			n = (0x0f & (10 + (fc - 'a')));
		}
	}

	SSHFP_nybble = xdigit $SSHFP_nybble;

	action SSHFP_sethi {
		if (i < sizeof rr->data.sshfp.digest)
			((unsigned char *)&rr->data.sshfp.digest)[i] = (0xf0 & (n << 4));
	}

	action SSHFP_setlo {
		if (i < sizeof rr->data.sshfp.digest)
			((unsigned char *)&rr->data.sshfp.digest)[i] |= (0x0f & n);
		i++;
	}

	SSHFP_octet = SSHFP_nybble %SSHFP_sethi SSHFP_nybble %SSHFP_setlo;

	SSHFP_type   = "SSHFP"i %{ rr->type = DNS_T_SSHFP; };
	SSHFP_algo   = number %{ rr->data.sshfp.algo = n; };
	SSHFP_digest = number %{ rr->data.sshfp.type = n; };
	SSHFP_data   = SSHFP_octet+ >{ i = 0; memset(&rr->data.sshfp.digest, 0, sizeof rr->data.sshfp.digest); };
	SSHFP = SSHFP_type space+ SSHFP_algo space+ SSHFP_digest space+ SSHFP_data space*;

	# FIXME: Support multiple segments.
	TXT_type = "TXT"i %{ rr->type = DNS_T_TXT; };
	TXT_data = string %{ rr->data.txt.len = MIN(rr->data.txt.size - 1, sp - str); memcpy(rr->data.txt.data, str, rr->data.txt.len); };
	TXT = TXT_type space+ TXT_data space*;

	SPF_type = "SPF"i %{ rr->type = DNS_T_SPF; };
	SPF = SPF_type space+ TXT_data space*;

	action AAAA_pton {
		if (1 != inet_pton(AF_INET6, str, &rr->data.aaaa.addr)) {
			SAY("invalid AAAA address: %s", str);
			goto next;
		}
	}

	AAAA_type   = "AAAA"i %{ rr->type = DNS_T_AAAA; };
	AAAA_digits = (xdigit | "." | ":")+ >str_init $str_copy %str_end;
	AAAA_addr   = AAAA_digits %AAAA_pton;
	AAAA = AAAA_type space+ AAAA_addr space*;

	A_type  = "A"i %{ rr->type = DNS_T_A; };
	A_octet = digit{1,3} >{ n = 0; } ${ n *= 10; n += fc - '0'; } %{ rr->data.a.addr.s_addr <<= 8; rr->data.a.addr.s_addr |= (0xff & n); };
	A_addr  = (A_octet "." A_octet "." A_octet "." A_octet) %{ rr->data.a.addr.s_addr = htonl(rr->data.a.addr.s_addr); };
	A = A_type space+ A_addr space*;

	PTR_type  = "PTR"i %{ rr->type = DNS_T_PTR; };
	PTR_cname = domain %{ dns_strlcpy(rr->data.ptr.host, str, sizeof rr->data.ptr.host); };
	PTR = PTR_type space+ PTR_cname space*;

	rrdata = (SOA | NS | MX | CNAME | SRV | SSHFP | TXT | SPF | AAAA | A | PTR) space*;

	rrname_blank  = " " %{ dns_strlcpy(rr->name, P->lastrr, sizeof rr->name); } " "*;
	rrname_origin = "@ " %{ dns_strlcpy(rr->name, P->origin, sizeof rr->name); } " "*;
	rrname_plain  = domain %{ if (!rr->name[0]) dns_strlcpy(rr->name, str, sizeof rr->name); } " "+;

	rrname = rrname_blank | rrname_origin | rrname_plain;

	rrclass = "IN " %{ rr->class = DNS_C_IN; };

	rrttl = ttl %{ rr->ttl = ttl; } space+;

	zonerr = rrname rrttl? rrclass? rrttl? rrdata;

	ORIGIN = "$ORIGIN " domain %{ dns_strlcpy(P->origin, str, sizeof P->origin); goto next; } space*;

	TTL    = "$TTL " ttl %{ P->ttl = ttl; goto next; } space*;

	nothing = space* %{ goto next; };

	main := (ORIGIN | TTL | zonerr | nothing) $!oops;
}%%

struct zonerr *zone_getrr(struct zonerr *rr, struct dns_soa **soa, struct zonefile *P) {
	%% write data;
	short *p, *pe, *eof;
	int cs;
	char str[1024], *sp, lc;
	unsigned span, ttl, n, i;

	span = 0;
next:
	tok_discard(&P->toks, span);

	if (!(span = tok_eol(&P->toks)))
		return NULL;

	sp  = str;
	lc  = 0;
	ttl = 0;
	n   = 0;
	i   = 0;

	zonerr_init(rr, P);

	%% write init;

	p   = P->toks.base;
	pe  = p + span;
	eof = pe;

	%% write exec;

	tok_discard(&P->toks, span);

	dns_strlcpy(P->lastrr, rr->name, sizeof P->lastrr);

	if (rr->type == DNS_T_SOA)
		P->soa = rr->data.soa;

	*soa = &P->soa;

	return rr;
} /* zone_getrr() */


size_t zone_parsesome(struct zonefile *P, const void *src, size_t len) {
	unsigned char *p = (unsigned char *)src;
	size_t lim = lengthof(P->toks.base) - P->toks.count;

	P->toks.count += fold(&P->toks.base[P->toks.count], lim, &p, &len, &P->pp);

	return p - (unsigned char *)src;
} /* zone_parsesome() */


size_t zone_parsefile(struct zonefile *P, FILE *fp) {
	unsigned char buf[1024];
	size_t tlim, lim, len;

	tlim = lengthof(P->toks.base) - P->toks.count;
	lim  = MIN(lengthof(buf), tlim);

	if (!(len = fread(buf, 1, lim, fp)))
		return 0;

	assert(len == zone_parsesome(P, buf, len));

	return len;
} /* zone_parsefile() */


struct zonefile *zone_open(const char *origin, unsigned ttl, int *error) {
	struct zonefile *P;

	if (!(P = malloc(sizeof *P)))
		{ *error = errno; return NULL; }

	zone_init(P, origin, ttl);

	return P;
} /* zone_open() */


void zone_close(struct zonefile *P) {
	free(P);
} /* zone_close() */


#if ZONE_MAIN

#include <stdlib.h>	/* EXIT_FAILURE */

#include <unistd.h>	/* getopt(3) */


struct {
	const char *progname;
} MAIN;


static void parsefile(FILE *fp, const char *origin, unsigned ttl) {
	struct zonefile P;
	struct zonerr rr;
	struct dns_soa *soa;

	zone_init(&P, origin, ttl);

	while (zone_parsefile(&P, fp)) {
		while (zone_getrr(&rr, &soa, &P)) {
			char data[512];
			dns_any_print(data, sizeof data, &rr.data, rr.type);
			printf("%s %u IN %s %s\n", rr.name, rr.ttl, dns_strtype(rr.type), data);
		}
	}
} /* parsefile() */


static void foldfile(FILE *dst, FILE *src) {
	struct zonepp state;
	unsigned char in[16], *p;
	short out[16];
	size_t len, num, i;

	memset(&state, 0, sizeof state);

	while ((len = fread(in, 1, sizeof in, src))) {
		p = in;

		while ((num = fold(out, sizeof out, &p, &len, &state))) {
			for (i = 0; i < num; i++) {
				if ((out[i] & T_LIT)
				||  (!isgraph(0xff & out[i]) && !isspace(0xff & out[i]))) {
					fprintf(dst, "\\%.3d", (0xff & out[i]));
				} else {
					fputc((0xff & out[i]), dst);
				}
			}
		}
	}
} /* foldfile() */


static void usage(FILE *fp) {
	static const char *usage =
		" [OPTIONS] [COMMAND]\n"
		"  -o ORIGIN  Zone origin\n"
		"  -t TTL     Zone TTL\n"
		"  -V         Print version info\n"
		"  -h         Print this usage message\n"
		"\n"
		"  fold       Fold into simple normal form\n"
		"  parse      Parse zone file and recompose\n"
		"\n"
		"Report bugs to William Ahern <william@25thandClement.com>\n";

	fputs(MAIN.progname, fp);
	fputs(usage, fp);
	fflush(fp);
} /* usage() */


static unsigned parsettl(const char *opt) {
	unsigned ttl;
	char *end;

	ttl = strtoul(opt, &end, 10);

	switch (tolower((unsigned char)*end)) {
	case 'w':
		ttl *= 7;
	case 'd':
		ttl *= 24;
	case 'h':
		ttl *= 60;
	case 'm':
		ttl *= 60;
	case 's':
		ttl *= 1;
	case '\0':
		break;
	default:
		fprintf(stderr, "%s: ", MAIN.progname);

		for (; *opt; opt++) {
			if (opt == end)
				fprintf(stderr, "[%c]", *opt);
			else
				fputc(*opt, stderr);
		}
		fputs(": invalid TTL\n", stderr);

		exit(EXIT_FAILURE);
	}

	return ttl;
} /* parsettl() */


#define CMD_FOLD  1
#define CMD_PARSE 2

static int command(const char *arg) {
	const char *p, *pe, *eof;
	int cs;

	arg = (arg && *arg)? arg : "parse";

	p   = arg;
	pe  = p + strlen(arg);
	eof = pe;

	%%{
		machine command;

		action oops {
			fprintf(stderr, "%s: %s: invalid command\n", MAIN.progname, arg);
			usage(stderr);
			exit(EXIT_FAILURE);
		}

		fold = "fold" %{ return CMD_FOLD; };
		parse = "parse" %{ return CMD_PARSE; };

		main := (fold | parse) $!oops;

		write data;
		write init;
		write exec;
	}%%

	return 0;
} /* command() */


int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;
	int opt;
	const char *origin = ".";
	unsigned ttl = 3600;

	MAIN.progname = argv[0];

	while (-1 != (opt = getopt(argc, argv, "o:t:Vh"))) {
		switch (opt) {
		case 'o':
			origin = optarg;

			break;
		case 't':
			ttl = parsettl(optarg);

			break;
		case 'h':
			usage(stdout);

			return 0;
		default:
			usage(stderr);

			return EXIT_FAILURE;
		} /* switch() */
	} /* getopt() */

	argc -= optind;
	argv += optind;

	switch (command(argv[0])) {
	case CMD_FOLD:
		foldfile(stdout, stdin);
		break;
	case CMD_PARSE:
		parsefile(stdin, origin, ttl);
		break;
	}

	return 0;
} /* main() */

#endif /* ZONE_MAIN */


#if __clang__
#pragma clang diagnostic pop
#elif (__GNUC__ == 4 && __GNUC_MINOR__ >= 6) || __GNUC__ > 4
#pragma GCC diagnostic pop
#endif
