#include <errno.h>
#include <stdio.h>

#include <err.h>
#include <sys/resource.h>

#include "dns.h"

int main(void) {
	struct dns_resolv_conf *resconf = NULL;
	struct dns_hosts *hosts = NULL;
	struct dns_hints *hints = NULL;
	struct dns_resolver *res = NULL;
	int error, status = 1;

	if (!(resconf = dns_resconf_local(&error)))
		goto error;
	if (!(hosts = dns_hosts_local(&error)))
		goto error;
	if (!(hints = dns_hints_local(resconf, &error)))
		goto error;

	/* arrange for dns_so_init to fail */
	struct rlimit nofile = { 0, 0 };
	if (0 != setrlimit(RLIMIT_NOFILE, &nofile))
		goto syerr;

	/* bug #12 caused dns_res_open to segfault when dns_so_init failed */
	if ((res = dns_res_open(resconf, hosts, hints, NULL, dns_opts(), &error))) {
		warnx("expected dns_res_open to fail");
		goto epilog;
	} else if (error != EMFILE) {
		warnx("expected dns_res_open to fail with EMFILE, got %d (%s)", error, dns_strerror(error));
		goto epilog;
	}

	warnx("OK");
	status = 0;

	goto epilog;
syerr:
	error = errno;
error:
	warnx("%s", dns_strerror(error));

	goto epilog;
epilog:
	dns_res_close(res);
	dns_hints_close(hints);
	dns_hosts_close(hosts);
	dns_resconf_close(resconf);

	return status;
}
