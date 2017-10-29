/* ==========================================================================
 * rfc4408-tests.c - OpenSPF test suite check.
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
#include <stdlib.h>

#include <stdio.h>

#include <string.h>

#include <assert.h>

#include <errno.h>

#include <unistd.h>

#include <sys/queue.h>

#include <netinet/in.h>
#include <arpa/inet.h>

#include <yaml.h>

#include "cache.h"
#include "dns.h"
#include "spf.h"


#define lengthof(a) (sizeof (a) / sizeof (a)[0])

#ifndef MIN
#define MIN(a, b) (((a) < (b))? (a) : (b))
#endif

#define STRINGIFY_(s) #s
#define STRINGIFY(s) STRINGIFY_(s)

#define streq(a, b) (!strcmp((a), (b)))


static int debug;

#define panic_(fn, ln, fmt, ...) \
	do { fprintf(stderr, fmt "%.1s", (fn), (ln), __VA_ARGS__); _Exit(EXIT_FAILURE); } while (0)

#define panic(...) panic_(__func__, __LINE__, __FILE__ ": (%s:%d) " __VA_ARGS__, "\n")

#define SAY_(fmt, ...) \
	do { if (debug) fprintf(stderr, fmt "%.1s", __func__, __LINE__, __VA_ARGS__); } while (0)
#define SAY(...) SAY_(">>>> (%s:%d) " __VA_ARGS__, "\n")
#define HAI SAY("HAI")


static const char *yaml_strevent(int type) {
	static const char event[][24] = {
		[YAML_NO_EVENT]             = "NO_EVENT",
		[YAML_STREAM_START_EVENT]   = "STREAM_START_EVENT",
		[YAML_STREAM_END_EVENT]     = "STREAM_END_EVENT",
		[YAML_DOCUMENT_START_EVENT] = "DOCUMENT_START_EVENT",
		[YAML_DOCUMENT_END_EVENT]   = "DOCUMENT_END_EVENT",
		[YAML_ALIAS_EVENT]          = "ALIAS_EVENT",
		[YAML_SCALAR_EVENT]         = "SCALAR_EVENT",
		[YAML_SEQUENCE_START_EVENT] = "SEQUENCE_START_EVENT",
		[YAML_SEQUENCE_END_EVENT]   = "SEQUENCE_END_EVENT",
		[YAML_MAPPING_START_EVENT]  = "MAPPING_START_EVENT",
		[YAML_MAPPING_END_EVENT]    = "MAPPING_END_EVENT",
	};

	return event[type];
} /* yaml_strevent() */


static const char *yaml_strevents(int set) {
	static char str[128];
	char *end;
	int type;

	str[0] = '\0';

	for (type = 1; type < 16; type++) {
		if ((1 << type) & set) {
			strncat(str, yaml_strevent(type), sizeof str - 1);
			strncat(str, "|", sizeof str - 1);
		}
	}

	if ((end = strrchr(str, '|')))
		*end = '\0';

	return str;
} /* yaml_strevents() */


static struct {
	struct {
		unsigned count, passed, failed;
	} tests;
} MAIN;


#define SET4(a, b, c, d, ...) \
	((1 << (a)) | (1 << (b)) | (1 << (c)) | (1 << (d)))
#define SET(...) SET4(__VA_ARGS__, 0, 0, 0)
#define INSET(set, x) ((set) & (1 << (x)))


static struct dns_resolver *mkres(struct cache *zonedata) {
	struct dns_resolv_conf *resconf;
	struct dns_hosts *hosts;
	struct dns_hints *hints;
	struct dns_resolver *res;
	int error;

	resconf = dns_resconf_open(&error);
	assert(resconf);
	memset(resconf->lookup, 0, sizeof resconf->lookup);
	resconf->lookup[0] = 'c';

	hosts = dns_hosts_open(&error);
	assert(hosts);
	hints = dns_hints_open(resconf, &error);
	assert(hints);

	res = dns_res_open(dns_resconf_mortal(resconf), dns_hosts_mortal(hosts), dns_hints_mortal(hints), cache_resi(zonedata), dns_opts(), &error);
	assert(res);

	return res;
} /* mkres() */



struct test {
	char *name;
	char *descr;
	char *comment;
	char *spec;
	char *helo;

	struct {
		int type;

		union {
			struct in_addr ip4;
			struct in6_addr ip6;
		} ip;
	} host;

	char *mailfrom;

	char *result[2];
	int rcount;

	char *exp;

	struct {
		int result;
		char *exp;
	} actual;

	CIRCLEQ_ENTRY(test) cqe;
}; /* struct test */


static void frepc(int ch, int n, FILE *fp) {
	while (n--)
		fputc(ch, fp);
} /* frepc() */

static void test_dump(struct test *test, struct spf_env *env, struct cache *zonedata, FILE *fp) {
	char ip[64];
	int i;

	fprintf(fp, "== [%.*s] ", (int)MIN(70, strlen(test->name)), test->name);
	frepc('=', 70 - MIN(70, strlen(test->name)), fp);
	fputc('\n', fp);
	fprintf(fp, "spec:     %s\n", test->spec);
	fprintf(fp, "helo:     %s\n", (test->helo)? test->helo : "");
	inet_ntop(test->host.type, &test->host.ip, ip, sizeof ip);
	fprintf(fp, "host:     %s\n", ip);
	fprintf(fp, "mailfrom: %s\n", (test->mailfrom)? test->mailfrom : "");
	fprintf(fp, "result:   { %s", test->result[0]);
	for (i = 1; i < test->rcount; i++)
		fprintf(fp, ", %s", test->result[i]);
	fprintf(fp, " } (%s)\n", spf_strresult(test->actual.result));
	fprintf(fp, "exp:      \"%s\" (\"%s\")\n", (test->exp)? test->exp : "", (test->actual.exp)? test->actual.exp : "");
	fputs("-- (env) -------------------------------------------------------------------\n", fp);
	spf_printenv(env, fp);
	fputs("-- (data) ------------------------------------------------------------------\n", fp);
	cache_dumpfile(zonedata, fp);
	fputs("== END ============================================================== END ==\n", fp);
	fputc('\n', fp);
} /* test_dump() */


static void test_run(struct test *test, struct dns_resolver *res, struct cache *zonedata) {
	struct spf_env env;
	struct spf_resolver *spf;
	int error, result, i, passed = 0;
	const char *exp;

	spf_env_init(&env, test->host.type, &test->host.ip, test->helo, test->mailfrom);
	spf = spf_open(&env, res, &spf_defaults, &error);
	assert(spf);

	while ((error = spf_check(spf))) {
		SAY("spf_check: %s", spf_strerror(error));

		goto done;
	}

	result = spf_result(spf);
	exp    = spf_exp(spf);

	for (i = 0; i < test->rcount; i++) {
		if (result != spf_iresult(test->result[i]))
			continue;

		passed = 1;
	}

	test->actual.result = result;

	if (exp) {
		test->actual.exp = strdup(exp);
		assert(test->actual.exp);
	}
done:
	if (passed) {
		MAIN.tests.passed++;

		printf("%s: PASSED\n", test->name);
	} else {
		MAIN.tests.failed++;

		printf("%s: FAILED\n", test->name);

		if (debug)
			test_dump(test, &env, zonedata, stderr);
	}

	spf_close(spf);
} /* test_run() */


static void test_free(struct test *test) {
	free(test->name);
	free(test->descr);
	free(test->comment);
	free(test->spec);
	free(test->helo);
	free(test->mailfrom);
	free(test->result[0]);
	free(test->result[1]);
	free(test->exp);
	free(test->actual.exp);
	free(test);
} /* test_free() */


struct section {
	char *descr;
	char *comment;
	struct cache *zonedata;

	CIRCLEQ_HEAD(, test) tests;
}; /* struct section */


static void section_run(struct section *section, const char *name) {
	struct dns_resolver *res;
	struct test *test;

	res = mkres(section->zonedata);

	CIRCLEQ_FOREACH(test, &section->tests, cqe) {
		if (streq(test->name, name) || streq(name, "all"))
			test_run(test, res, section->zonedata);
	}

	dns_res_close(res);
} /* section_run() */


static void section_free(struct section *section) {
	struct test *test;

	free(section->descr);
	free(section->comment);
	cache_close(section->zonedata);

	while (!CIRCLEQ_EMPTY(&section->tests)) {
		test = CIRCLEQ_FIRST(&section->tests);
		CIRCLEQ_REMOVE(&section->tests, test, cqe);
		test_free(test);
	}

	free(section);
} /* section_free() */


static int expect(yaml_parser_t *parser, yaml_event_t *event, int set) {
	int ok;

	ok = yaml_parser_parse(parser, event);
	assert(ok);

	if (!INSET(set, event->type))
		panic("got %s, expected %s", yaml_strevent(event->type), yaml_strevents(set));

	return event->type;
} /* expect() */


static yaml_event_t *delete(yaml_event_t *event) {
	yaml_event_delete(event);
	return memset(event, 0, sizeof *event);
} /* delete() */


static void discard(yaml_parser_t *parser, int set) {
	yaml_event_t event;
	expect(parser, &event, set);
	delete(&event);
} /* discard() */


#define SCALAR_STR(event) ((char *)(event)->data.scalar.value)
#define SCALAR_LEN(event) ((event)->data.scalar.length)


static char *nextscalar(char **dst, yaml_parser_t *parser) {
	yaml_event_t event;

	if (YAML_SCALAR_EVENT == expect(parser, &event, SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_START_EVENT))) {
		*dst = strdup((char *)event.data.scalar.value);
		assert(*dst);
	} else {
		size_t size = 0;
		*dst = 0;

		while (YAML_SCALAR_EVENT == expect(parser, delete(&event), SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_END_EVENT))) {
			*dst = realloc(*dst, size + SCALAR_LEN(&event) + 1);
			assert(*dst);
			memcpy(&(*dst)[size], SCALAR_STR(&event), SCALAR_LEN(&event));
			size += SCALAR_LEN(&event);
			(*dst)[size] = '\0';
		}
	}

	delete(&event);

	return *dst;
} /* nextscalar() */


static struct test *nexttest(yaml_parser_t *parser) {
	struct test *test;
	yaml_event_t event;
	int type;

	type = expect(parser, &event, SET(YAML_SCALAR_EVENT, YAML_MAPPING_END_EVENT));

	if (type == YAML_MAPPING_END_EVENT)
		{ delete(&event); return NULL; }

	test = malloc(sizeof *test);
	assert(test);
	memset(test, 0, sizeof *test);

	test->name = strdup((char *)event.data.scalar.value);
	assert(test->name);
	delete(&event);

	discard(parser, SET(YAML_MAPPING_START_EVENT));

	while (YAML_SCALAR_EVENT == expect(parser, &event, SET(YAML_SCALAR_EVENT, YAML_MAPPING_END_EVENT))) {
		char *txt = (char *)event.data.scalar.value;

		if (streq(txt, "description")) {
			nextscalar(&test->descr, parser);
		}  else if (streq(txt, "comment")) {
			nextscalar(&test->comment, parser);
		}  else if (streq(txt, "spec")) {
			nextscalar(&test->spec, parser);
		}  else if (streq(txt, "helo")) {
			nextscalar(&test->helo, parser);
		}  else if (streq(txt, "host")) {
			char *host;
			int rv;

			nextscalar(&host, parser);

			if (strchr(host, ':')) {
				test->host.type = AF_INET6;
			} else {
				test->host.type = AF_INET;
			}

			rv = inet_pton(test->host.type, host, &test->host.ip);
			assert(rv == 1);

			free(host);
		}  else if (streq(txt, "mailfrom")) {
			nextscalar(&test->mailfrom, parser);
		}  else if (streq(txt, "result")) {
			if (YAML_SCALAR_EVENT == expect(parser, delete(&event), SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_START_EVENT))) {
				test->result[0] = strdup((char *)event.data.scalar.value);
				assert(test->result[0]);
				test->rcount = 1;
			} else {
				while (YAML_SCALAR_EVENT == expect(parser, delete(&event), SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_END_EVENT))) {
					assert(test->rcount < lengthof(test->result));
					test->result[test->rcount] = strdup((char *)event.data.scalar.value);
					assert(test->result[test->rcount]);
					test->rcount++;
				}
			}
		}  else if (streq(txt, "explanation")) {
			nextscalar(&test->exp, parser);
		} else {
			SAY("%s: unknown field", txt);

			discard(parser, SET(YAML_SCALAR_EVENT));
		}

		delete(&event);
	} /* while() */

	delete(&event);

	MAIN.tests.count++;

	return test;
} /* nexttest() */


static void nextspf(struct dns_txt *txt, yaml_parser_t *parser) {
	yaml_event_t event;

	memset(txt->data, ' ', sizeof txt->data);

	if (YAML_SCALAR_EVENT == expect(parser, &event, SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_START_EVENT))) {
		assert(SCALAR_LEN(&event) < txt->size);
		memcpy(txt->data, SCALAR_STR(&event), SCALAR_LEN(&event));
		txt->len = SCALAR_LEN(&event);
	} else {
		while (YAML_SCALAR_EVENT == expect(parser, delete(&event), SET(YAML_SCALAR_EVENT, YAML_SEQUENCE_END_EVENT))) {
#if 1
			assert(txt->size - txt->len >= SCALAR_LEN(&event));
			memcpy(&txt->data[txt->len], SCALAR_STR(&event), SCALAR_LEN(&event));
			txt->len += SCALAR_LEN(&event);
#else /* FIXME: Need a way to insert raw rdata. Padding doesn't do the right thing. */
			assert(txt->size - txt->len >= 255);
			assert(SCALAR_LEN(&event) <= 255);
			memcpy(&txt->data[txt->len], SCALAR_STR(&event), SCALAR_LEN(&event));
			txt->len += 255;
#endif
		}
	}

	delete(&event);
} /* nextspf() */


static struct cache *nextzonedata(yaml_parser_t *parser, _Bool insert) {
	struct cache *zonedata;
	yaml_event_t event;
	char rrname[256];
	union dns_any anyrr;
	int rv, error, rrtype;

	zonedata = cache_open(&error);
	assert(zonedata);

	discard(parser, SET(YAML_MAPPING_START_EVENT));

	while (YAML_SCALAR_EVENT == expect(parser, &event, SET(YAML_SCALAR_EVENT, YAML_MAPPING_END_EVENT))) {
		dns_d_init(rrname, sizeof rrname, (char *)event.data.scalar.value, strlen((char *)event.data.scalar.value), DNS_D_ANCHOR);
		discard(parser, SET(YAML_SEQUENCE_START_EVENT));

		while (INSET(SET(YAML_MAPPING_START_EVENT, YAML_SCALAR_EVENT), expect(parser, delete(&event), SET(YAML_MAPPING_START_EVENT, YAML_SCALAR_EVENT, YAML_SEQUENCE_END_EVENT)))) {
			if (event.type == YAML_SCALAR_EVENT) {
				SAY("%s: unknown zonedata value", (char *)event.data.scalar.value);

				continue;
			}

			dns_any_init(&anyrr, sizeof anyrr);

			expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));

			if (!(rrtype = dns_itype((char *)event.data.scalar.value)))
				panic("%s: unknown RR type", event.data.scalar.value);

			switch (rrtype) {
			case DNS_T_A:
				expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));
				rv = inet_pton(AF_INET, SCALAR_STR(&event), &anyrr.a);
				assert(rv == 1);

				break;
			case DNS_T_AAAA:
				expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));
				rv = inet_pton(AF_INET6, SCALAR_STR(&event), &anyrr.aaaa);
				assert(rv == 1);

				break;
			case DNS_T_PTR:
				expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));
				if (!dns_d_init(anyrr.ptr.host, sizeof anyrr.ptr.host, SCALAR_STR(&event), SCALAR_LEN(&event), DNS_D_ANCHOR))
					goto next;

				break;
			case DNS_T_MX:
				discard(parser, SET(YAML_SEQUENCE_START_EVENT));

				expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));
				anyrr.mx.preference = atoi(SCALAR_STR(&event));

				expect(parser, delete(&event), SET(YAML_SCALAR_EVENT));
				dns_d_init(anyrr.mx.host, sizeof anyrr.mx.host, SCALAR_STR(&event), SCALAR_LEN(&event), DNS_D_ANCHOR);

				discard(parser, SET(YAML_SEQUENCE_END_EVENT));

				if (!SCALAR_LEN(&event))
					goto next;

				break;
			case DNS_T_SPF:
				/* FALL THROUGH */
			case DNS_T_TXT:
				nextspf(&anyrr.txt, parser);

				break;
			default:
				discard(parser, SET(YAML_SCALAR_EVENT));

				SAY("%s: unknown RR type", dns_strtype(rrtype));

				goto next;
			} /* switch() */

			if (insert) {
				error = cache_insert(zonedata, rrname, rrtype, 3600, &anyrr);
				assert(!error);
			}
next:
			discard(parser, SET(YAML_MAPPING_END_EVENT));
		} /* while() */

		delete(&event);
	} /* while() */

	delete(&event);

	return zonedata;
} /* nextzonedata() */


static struct section *nextsection(yaml_parser_t *parser, const char *testname) {
	struct section *section;
	yaml_event_t event;
	_Bool insert = 0; /* this only works if zonedata follows tests */
	int type;
	char *txt;

	if (YAML_STREAM_END_EVENT == expect(parser, &event, SET(YAML_DOCUMENT_START_EVENT, YAML_STREAM_END_EVENT))) {
		delete(&event);
		return NULL;
	}

	discard(parser, SET(YAML_MAPPING_START_EVENT));

	section = malloc(sizeof *section);
	assert(section);
	memset(section, 0, sizeof *section);
	CIRCLEQ_INIT(&section->tests);

	while (expect(parser, delete(&event), SET(YAML_SCALAR_EVENT, YAML_MAPPING_END_EVENT))) {
		if (event.type == YAML_MAPPING_END_EVENT)
			break;

		txt = (char *)event.data.scalar.value;

		if (streq(txt, "description")) {
			nextscalar(&section->descr, parser);
		} else if (streq(txt, "comment")) {
			nextscalar(&section->comment, parser);
		} else if (streq(txt, "tests")) {
			struct test *test;

			discard(parser, SET(YAML_MAPPING_START_EVENT));

			while ((test = nexttest(parser))) {
				if (streq(test->name, testname) || streq("all", testname)) {
					insert = 1;
					CIRCLEQ_INSERT_TAIL(&section->tests, test, cqe);
				} else {
					test_free(test);
				}
			}
		} else if (streq(txt, "zonedata")) {
			section->zonedata = nextzonedata(parser, insert);
		} else {
			panic("%s: unknown top-level field", txt);
		}
	}

	delete(&event);

	discard(parser, SET(YAML_DOCUMENT_END_EVENT));

	return section;
} /* nextsection() */


static void trace(yaml_parser_t *parser) {
	yaml_event_t event;
	int ok, done = 0;

	while (!done) {
		ok = yaml_parser_parse(parser, &event);
		assert(ok);

		puts(yaml_strevent(event.type));

		switch (event.type) {
		case YAML_SCALAR_EVENT:
			printf("anchor:%s tag:%s value:%s\n", event.data.scalar.anchor, event.data.scalar.tag, event.data.scalar.value);

			break;
		case YAML_STREAM_END_EVENT:
			done = 1;

			break;
		} /* switch(event.type) */

		yaml_event_delete(&event);
	}
} /* trace() */


#define USAGE \
	"rfc4408-tests [-vh] [TEST]\n" \
	"  -v  increase verboseness\n" \
	"  -h  print usage\n" \
	"\n" \
	"Report bugs to William Ahern <william@25thandClement.com>\n"

int main(int argc, char **argv) {
	extern char *optarg;
	extern int optind;
	int opt;
	yaml_parser_t parser;
	yaml_event_t event;
	struct section *section;
	char *test;

	while (-1 != (opt = getopt(argc, argv, "vh"))) {
		switch (opt) {
		case 'v':
			spf_debug++;
			dns_debug++;
			debug++;

			break;
		case 'h':
			fputs(USAGE, stdout);

			return 0;
		default:
			fputs(USAGE, stderr);

			return EXIT_FAILURE;
		} /* switch() */
	} /* while() */

	argc -= optind;
	argv += optind;

	test = (argc)? *argv : "all";

	/* apologies to anyone harmed by this trick */
	if (isatty(fileno(stdin))) {
		if (!freopen("rfc4408-tests.yml", "r", stdin))
			panic("rfc4408-tests.yml: %s", strerror(errno));
	}

	yaml_parser_initialize(&parser);
	yaml_parser_set_input_file(&parser, stdin);

	discard(&parser, SET(YAML_STREAM_START_EVENT));

	if (streq(test, "trace")) {
		trace(&parser);
	} else {
		while ((section = nextsection(&parser, test))) {
			section_run(section, test);
			section_free(section);
		} /* while() */

		#define PCT(a, b) (((float)a / (float)b) * (float)100)
		printf("PASSED %u of %u (%.2f%%)\n", MAIN.tests.passed, MAIN.tests.count, PCT(MAIN.tests.passed, MAIN.tests.count));
		printf("FAILED %u of %u (%.2f%%)\n", MAIN.tests.failed, MAIN.tests.count, PCT(MAIN.tests.failed, MAIN.tests.count));
	}

	yaml_parser_delete(&parser);

	return 0;
} /* main() */
