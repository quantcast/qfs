/* ==========================================================================
 * cache.c - Simple Query Cache for dns.c
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
#include <stdio.h>	/* FILE fprintf(3) */

#include <string.h>	/* strcasecmp(3) memset(3) */

#include <errno.h>	/* errno */

#include <assert.h>	/* assert(3) */

#include "dns.h"
#include "zone.h"
#include "cache.h"
#include "tree.h"


#define SAY_(fmt, ...) \
	do { fprintf(stderr, fmt "%.1s", __func__, __LINE__, __VA_ARGS__); } while (0)
#define SAY(...) SAY_(">>>> (%s:%d) " __VA_ARGS__, "\n")
#define HAI SAY("HAI")


struct rrset {
	char name[DNS_D_MAXNAME + 1];
	enum dns_type type;

	union {
		struct dns_packet packet;
		unsigned char pbuf[dns_p_calcsize(1024)];
	};

	RB_ENTRY(rrset) rbe;
}; /* struct rrset */


static int rrset_init(struct rrset *set, const char *name, enum dns_type type) {
	int error;

	memset(set, 0, sizeof *set);

	dns_strlcpy(set->name, name, sizeof set->name);
	set->type = type;

	dns_p_init(&set->packet, sizeof set->pbuf);

	if ((error = dns_p_push(&set->packet, DNS_S_QD, name, strlen(name), type, DNS_C_IN, 0, NULL)))
		return error;

	return 0;
} /* rrset_init() */


RB_HEAD(rrcache, rrset);

static inline int rrset_cmp(struct rrset *a, struct rrset *b) {
	int cmp;
	return ((cmp = a->type - b->type))? cmp : strcasecmp(a->name, b->name);
}

RB_PROTOTYPE(rrcache, rrset, rbe, rrset_cmp)
RB_GENERATE(rrcache, rrset, rbe, rrset_cmp)


struct cache {
	struct dns_cache res;
	struct rrcache root;
}; /* struct cache */


static struct rrset *cache_find(struct cache *C, const char *name, enum dns_type type, _Bool make, int *error_) {
	struct rrset key, *set;
	int error;

	dns_strlcpy(key.name, name, sizeof key.name);
	key.type = type;

	if ((set = RB_FIND(rrcache, &C->root, &key)))
		return set;

	if (!make)
		return NULL;

	if (!(set = malloc(sizeof *set)))
		goto syerr;

	if ((error = rrset_init(set, name, type)))
		goto error;

	assert(!RB_INSERT(rrcache, &C->root, set));

	return set;
syerr:
	error = errno;
error:
	*error_ = error;

	free(set);

	return NULL;
} /* cache_find() */


int cache_insert(struct cache *C, const char *name, enum dns_type type, unsigned ttl, const void *any) {
	struct rrset *set;
	int error;

	if (!(set = cache_find(C, name, type, 1, &error)))
		return error;

	if ((error = dns_p_push(&set->packet, DNS_S_AN, name, strlen(name), type, DNS_C_IN, ttl, any)))
		return error;

	return 0;
} /* cache_insert() */


struct dns_packet *cache_query(struct dns_packet *query, struct dns_cache *res, int *error) {
	struct cache *cache = res->state;
	struct dns_packet *ans = NULL;
	char qname[DNS_D_MAXNAME + 1];
	struct dns_rr rr;
	struct rrset *set;

	if ((*error = dns_rr_parse(&rr, 12, query)))
		return NULL;

	if (!dns_d_expand(qname, sizeof qname, rr.dn.p, query, error))
		goto error;

	if (!(set = cache_find(cache, qname, rr.type, 0, error)))
		return NULL;

	if (!(ans = malloc(dns_p_sizeof(&set->packet))))
		goto syerr;

	dns_p_init(ans, dns_p_sizeof(&set->packet));

	return dns_p_copy(ans, &set->packet);
syerr:
	*error = errno;
error:
	free(ans);

	return NULL;
} /* cache_query() */


struct dns_cache *cache_resi(struct cache *cache) {
	return &cache->res;
} /* cache_resi() */


void cache_close(struct cache *C) {
	struct rrset *set;

	if (!C)
		return;

	while ((set = RB_MIN(rrcache, &C->root))) {
		RB_REMOVE(rrcache, &C->root, set);
		free(set);
	}

	free(C);
} /* cache_close() */


struct cache *cache_open(int *error) {
	struct cache *C;

	if (!(C = malloc(sizeof *C)))
		goto syerr;

	dns_cache_init(&C->res);
	C->res.state = C;
	C->res.query = &cache_query;

	RB_INIT(&C->root);

	return C;
syerr:
	*error = errno;

	cache_close(C);

	return NULL;
} /* cache_open() */


int cache_loadfile(struct cache *C, FILE *fp, const char *origin, unsigned ttl) {
	struct zonefile *zone;
	struct zonerr rr;
	struct dns_soa *soa;
	int error;

	if (!(zone = zone_open(origin, ttl, &error)))
		goto error;

	while (zone_parsefile(zone, fp)) {
		while (zone_getrr(&rr, &soa, zone)) {
			if ((error = cache_insert(C, rr.name, rr.type, rr.ttl, &rr.data)))
				goto error;
		}
	}

	zone_close(zone);

	return 0;
error:
	zone_close(zone);

	return error;
} /* cache_loadfile() */


int cache_loadpath(struct cache *C, const char *path, const char *origin, unsigned ttl) {
	FILE *fp;
	int error;

	if (!strcmp(path, "-"))
		return cache_loadfile(C, stdin, origin, ttl);

	if (!(fp = fopen(path, "r")))
		return errno;

	error = cache_loadfile(C, fp, origin, ttl);

	fclose(fp);

	return error;
} /* cache_loadpath() */


static void cache_showpkt(struct dns_packet *pkt, FILE *fp) {
	char buf[1024];
	struct dns_rr rr;
	union dns_any data;
	int error;

	dns_rr_foreach(&rr, pkt, .section = DNS_S_AN) {
		dns_d_expand(buf, sizeof buf, rr.dn.p, pkt, &error);
		fprintf(fp, "%s %u IN %s ", buf, rr.ttl, dns_strtype(rr.type));

		dns_any_parse(dns_any_init(&data, sizeof data), &rr, pkt);
		dns_any_print(buf, sizeof buf, &data, rr.type);
		fprintf(fp, "%s\n", buf);
	}
} /* cache_showpkt() */


int cache_dumpfile(struct cache *C, FILE *fp) {
	struct rrset *set;

	RB_FOREACH(set, rrcache, &C->root) {
		cache_showpkt(&set->packet, fp);
	}

	return 0;
} /* cache_dumpfile() */


#if CACHE_MAIN

#include <ctype.h>	/* tolower(3) */

#include <unistd.h>	/* getopt(3) */


struct {
	char *progname;
	char *origin;
	unsigned ttl;
	struct dns_resolv_conf *resconf;
	struct dns_hosts *hosts;
	struct dns_hints *hints;
	struct cache *cache;
} MAIN = {
	.origin = ".",
	.ttl = 3600,
};


static struct dns_resolv_conf *resconf(void) {
	int error;

	if (!MAIN.resconf) {
		assert(MAIN.resconf = dns_resconf_local(&error));

		MAIN.resconf->lookup[2] = MAIN.resconf->lookup[1];
		MAIN.resconf->lookup[1] = MAIN.resconf->lookup[0];
		MAIN.resconf->lookup[0] = 'c';
	}

	return MAIN.resconf;
} /* resconf() */


static struct dns_hosts *hosts(void) {
	int error;

	if (!MAIN.hosts)
		assert(MAIN.hosts = dns_hosts_local(&error));

	return MAIN.hosts;
} /* hosts() */


static struct dns_hints *hints(void) {
	int error;

	if (!MAIN.hints)
		assert(MAIN.hints = dns_hints_local(resconf(), &error));

	return MAIN.hints;
} /* hints() */


static struct cache *cache(void) {
	int error;

	if (!MAIN.cache) {
		assert(MAIN.cache = cache_open(&error));
		assert(!cache_loadfile(MAIN.cache, stdin, MAIN.origin, MAIN.ttl));
	}

	return MAIN.cache;
} /* cache() */


static void usage(FILE *fp) {
	static const char *usage =
		" [OPTIONS] [QNAME [QTYPE]]\n"
		"  -o ORIGIN  Zone origin\n"
		"  -t TTL     Zone TTL\n"
		"  -V         Print version info\n"
		"  -h         Print this usage message\n"
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


int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;
	int opt;
	const char *qname = NULL;
	int qtype = DNS_T_A;
	struct dns_resolver *res;
	struct dns_packet *ans;
	int error;

	MAIN.progname = argv[0];

	while (-1 != (opt = getopt(argc, argv, "o:t:Vh"))) {
		switch (opt) {
		case 'o':
			MAIN.origin = optarg;

			break;
		case 't':
			MAIN.ttl = parsettl(optarg);

			break;
		case 'h':
			usage(stdout);

			return 0;
		default:
			usage(stderr);

			return EXIT_FAILURE;
		} /* switch() */
	} /* while(getopt()) */

	argc -= optind;
	argv += optind;

	if (argc > 0)
		qname = argv[0];
	if (argc > 1)
		assert(qtype = dns_itype(argv[1]));

	if (qname) {
		assert(res = dns_res_open(resconf(), hosts(), hints(), cache_resi(cache()), dns_opts(), &error));

		assert(!dns_res_submit(res, qname, qtype, DNS_C_IN));

		while ((error = dns_res_check(res))) {
			assert(error == EAGAIN);
			assert(!dns_res_poll(res, 5));
		}

		assert((ans = dns_res_fetch(res, &error)));

		cache_showpkt(ans, stdout);

		free(ans);

		dns_res_close(res);
	} else {
		cache_dumpfile(cache(), stdout);
	}

	return 0;
} /* main() */


#endif /* CACHE_MAIN */

