/* ==========================================================================
 * zone.h - RFC 1035 Master File Parser
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
#ifndef ZONE_H
#define ZONE_H

#include <stddef.h>	/* size_t */
#include <stdio.h>	/* FILE */

#include "dns.h"


struct zonerr {
	char name[DNS_D_MAXNAME + 1];
	enum dns_class class;
	enum dns_type type;
	unsigned ttl;
	union dns_any data;
}; /* struct zonerr */


struct zonefile;

struct zonefile *zone_open(const char *, unsigned, int *);

void zone_close(struct zonefile *);

size_t zone_parsesome(struct zonefile *, const void *, size_t);

size_t zone_parsefile(struct zonefile *, FILE *);

struct zonerr *zone_getrr(struct zonerr *, struct dns_soa **, struct zonefile *);


#endif /* ZONE_H */
