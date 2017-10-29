#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <err.h>

#include "dns.h"

#define countof(a) (sizeof (a) / sizeof *(a))

static void
search_add(struct dns_resolv_conf *resconf, const char *dn)
{
	size_t i;

	for (i = 0; i < countof(resconf->search); i++) {
		if (*resconf->search[i])
			continue;
		dns_strlcpy(resconf->search[i], dn, sizeof resconf->search[i]);
		break;
	}
}

static void
search_check(struct dns_resolv_conf *resconf, const char *host, const char **expect)
{
	dns_resconf_i_t search = 0;
	char dn[DNS_D_MAXNAME + 1];

	while (dns_resconf_search(dn, sizeof dn, host, strlen(host), resconf, &search)) {
		if (*expect) {
			if (strcmp(dn, *expect))
				errx(1, "expected domain %s, but got %s", *expect, dn);
			expect++;
		} else {
			errx(1, "unexpected domain (%s)", dn);
		}
	}
}

int
main(void)
{
	struct dns_resolv_conf resconf = { 0 };

	search_add(&resconf, "foo.local");
	search_add(&resconf, "bar.local");

	resconf.options.ndots = 1;
	search_check(&resconf, "host", (const char *[]){ "host.foo.local.", "host.bar.local.", "host.", NULL });

	resconf.options.ndots = 0;
	search_check(&resconf, "host", (const char *[]){ "host.", "host.foo.local.", "host.bar.local.", NULL });

	search_check(&resconf, "host.", (const char *[]){ "host.", NULL });

	warnx("OK");

	return 0;
}
