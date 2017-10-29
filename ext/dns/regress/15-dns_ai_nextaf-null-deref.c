#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <err.h>

#include "dns.h"

#define AI_HINTS(af, flags) (&(struct addrinfo){ .ai_family = (af), .ai_flags = (flags) })

#define croak(...) do { cluck(__VA_ARGS__); goto epilog; } while (0)
#define cluck_(fmt, ...) warnx(fmt " (at line %d)", __VA_ARGS__);
#define cluck(...) cluck_(__VA_ARGS__, __LINE__)
#define pfree(pp) do { free(*(pp)); *(pp) = NULL; } while (0)
#define ai_close(aip) do { dns_ai_close(*(aip)); *(aip) = NULL; } while (0)
#define rc_close(rcp) do { dns_resconf_close(*(rcp)); *(rcp) = NULL; } while (0)
#define db_close(dbp) do { dns_hosts_close(*(dbp)); *(dbp) = NULL; } while (0)
#define hints_close(hp) do { dns_hints_close(*(hp)); *(hp) = NULL; } while (0)
#define res_close(resp) do { dns_res_close(*(resp)); *(resp) = NULL; } while (0)

static _Bool expect(int af, const char *ip, struct dns_addrinfo *ai, int *_error) {
	struct addrinfo *ent = NULL;
	char addr[INET6_ADDRSTRLEN + 1];
	int found = 0, error;

	if ((error = dns_ai_nextent(&ent, ai)))
		croak("no addrinfo result %s", dns_strerror(error));
	if (!ent)
		croak("no addrinfo result");
	if (ent->ai_family != af)
		croak("expected AF of %d, got %d", af, ent->ai_family);
	if ((error = getnameinfo(ent->ai_addr, ent->ai_addrlen, addr, sizeof addr, NULL, 0, NI_NUMERICHOST)))
		croak("getnameinfo: %s", gai_strerror(error));
	if (0 != strcmp(addr, ip))
		croak("expected %s, got %s", ip, addr);

	error = 0;
	found = 1;
	goto epilog;
epilog:
	pfree(&ent);
	*_error = error;
	return found;
}

int main(void) {
	struct dns_resolv_conf *resconf = NULL;
	struct dns_hosts *hosts = NULL;
	struct dns_resolver *res = NULL;
	struct dns_addrinfo *ai = NULL;
	struct addrinfo *ent = NULL;
	int error, status = 1;

	/*
	 * Bug caused a segfault when calling dns_ai_nextent if we passed a
	 * NULL pointer as the resolver to dns_ai_open. This was previously
	 * allowed as long as AI_NUMERICHOST was specified, but some IPv6
	 * work caused a regression.
	 */
	if (!(ai = dns_ai_open("127.0.0.1", NULL, 0, AI_HINTS(AF_UNSPEC, AI_NUMERICHOST), NULL, &error)))
		goto error;
	if ((error = dns_ai_nextent(&ent, ai))) 
		goto error;
	if (!ent)
		croak("no addrinfo result");
	if (ent->ai_family != AF_INET)
		croak("expected AF of %d, got %d", AF_INET, ent->ai_family);
	if (((struct sockaddr_in *)ent->ai_addr)->sin_addr.s_addr != htonl(INADDR_LOOPBACK))
		croak("expected IPv4 loopback address");
	pfree(&ent);
	ai_close(&ai);

	/*
	 * Also test that EINVAL is returned if we violate the constraint
	 * that AI_NUMERICHOST is required if resolver is NULL.
	 */
	if ((ai = dns_ai_open("127.0.0.1", NULL, 0, AI_HINTS(AF_UNSPEC, 0), NULL, &error))) {
		croak("expected failure when resolver is NULL and AI_NUMERICHOST not set");
	} else if (error != EINVAL) {
		croak("expected EINVAL when resolver is NULL and AI_NUMERICHOST not set, got %d", error);
	}
	ai_close(&ai);

	/*
	 * Because we refactored dns_ai_nextaf test that the expected
	 * semantics work. Specifically, we intended to copy the semantics
	 * of OpenBSD's behavior, where the address families resolved are
	 * the intersection of those specifed by the /etc/resolv.conf family
	 * directive and the .ai_type hint. So if family is "inet4" and
	 * .ai_type == AF_INET6, a name of ::1 will return 0 entries even if
	 * AI_NUMERICHOST is specified. (See also comment in dns_ai_nextaf.)
	 */
	if (!(resconf = dns_resconf_open(&error)))
		goto error;
	if (!(hosts = dns_hosts_open(&error)))
		goto error;
	struct in_addr inaddr_loopback;
	inaddr_loopback.s_addr = htonl(INADDR_LOOPBACK);
	if ((error = dns_hosts_insert(hosts, AF_INET, &inaddr_loopback, "localhost", 0)))
		goto error;
	if ((error = dns_hosts_insert(hosts, AF_INET6, &in6addr_loopback, "localhost", 0)))
		goto error;

	/* check that IPv4 hostname resolves with AF_UNSPEC and AF_INET */
	if (!(res = dns_res_open(resconf, hosts, dns_hints_mortal(dns_hints_open(resconf, &error)), NULL, dns_opts(), &error)))
		goto error;
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_UNSPEC, 0), res, &error)))
		goto error;
	if (!expect(AF_INET, "127.0.0.1", ai, &error))
		croak("expected localhost to resolve to 127.0.0.1");
	ai_close(&ai);
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_INET, 0), res, &error)))
		goto error;
	if (!expect(AF_INET, "127.0.0.1", ai, &error))
		croak("expected localhost to resolve to 127.0.0.1");
	ai_close(&ai);
	res_close(&res);

	/* check that IPv6 hostname resolves with AF_UNSPEC and AF_INET6 */
	resconf->family[0] = AF_INET6;
	resconf->family[1] = AF_INET;
	resconf->family[2] = AF_UNSPEC;
	if (!(res = dns_res_open(resconf, hosts, dns_hints_mortal(dns_hints_open(resconf, &error)), NULL, dns_opts(), &error)))
		goto error;
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_UNSPEC, 0), res, &error)))
		goto error;
	if (!expect(AF_INET6, "::1", ai, &error))
		croak("expected localhost to resolve to ::1");
	ai_close(&ai);
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_INET6, 0), res, &error)))
		goto error;
	if (!expect(AF_INET6, "::1", ai, &error))
		croak("expected localhost to resolve to ::1");
	ai_close(&ai);
	res_close(&res);

	/* check that IPv6 hostname DOES NOT resolve if missing from hints or resconf */
	resconf->family[0] = AF_INET; /* missing here but allowed in hints */
	resconf->family[1] = AF_UNSPEC;
	if (!(res = dns_res_open(resconf, hosts, dns_hints_mortal(dns_hints_open(resconf, &error)), NULL, dns_opts(), &error)))
		goto error;
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_UNSPEC, 0), res, &error)))
		goto error;
	if (!expect(AF_INET, "127.0.0.1", ai, &error))
		croak("expected localhost to resolve to 127.0.0.1");
	if (!(error = dns_ai_nextent(&ent, ai)))
		croak("expected localhost to NOT resolve");
	if (error != ENOENT)
		croak("expected ENOENT error, got %d (%s)", error, dns_strerror(error));
	ai_close(&ai);
	res_close(&res);

	resconf->family[0] = AF_INET; /* missing here but allowed in hints */
	resconf->family[1] = AF_UNSPEC;
	if (!(res = dns_res_open(resconf, hosts, dns_hints_mortal(dns_hints_open(resconf, &error)), NULL, dns_opts(), &error)))
		goto error;
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_INET6, 0), res, &error)))
		goto error;
	if (!(error = dns_ai_nextent(&ent, ai)))
		croak("expected localhost to NOT resolve");
	if (error != DNS_ENONAME && error != DNS_EFAIL)
		croak("expected DNS_ENONAME or DNS_EFAIL error, got %d (%s)", error, dns_strerror(error));
	ai_close(&ai);
	res_close(&res);

	resconf->family[0] = AF_INET6; /* set here but not set in hints */
	resconf->family[1] = AF_UNSPEC;
	if (!(res = dns_res_open(resconf, hosts, dns_hints_mortal(dns_hints_open(resconf, &error)), NULL, dns_opts(), &error)))
		goto error;
	if (!(ai = dns_ai_open("localhost", NULL, 0, AI_HINTS(AF_INET, 0), res, &error)))
		goto error;
	if (!(error = dns_ai_nextent(&ent, ai)))
		croak("expected localhost to NOT resolve");
	if (error != DNS_ENONAME && error != DNS_EFAIL)
		croak("expected DNS_ENONAME or DNS_EFAIL error, got %d (%s)", error, dns_strerror(error));
	ai_close(&ai);
	res_close(&res);

	warnx("OK");
	status = 0;

	goto epilog;
error:
	warnx("%s", dns_strerror(error));

	goto epilog;
epilog:
	pfree(&ent);
	ai_close(&ai);
	res_close(&res);
	db_close(&hosts);
	rc_close(&resconf);

	return status;
}
