/* ==========================================================================
 * cache.h - Simple Query Cache for dns.c
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
#ifndef CACHE_H
#define CACHE_H

#include <stdio.h>	/* FILE */

#include "dns.h"


struct cache;

struct cache *cache_open(int *);

void cache_close(struct cache *);

int cache_loadfile(struct cache *, FILE *, const char *, unsigned);

int cache_loadpath(struct cache *, const char *, const char *, unsigned);

struct dns_cache *cache_resi(struct cache *);

int cache_insert(struct cache *, const char *, enum dns_type, unsigned, const void *);

int cache_dumpfile(struct cache *, FILE *);


#endif /* CACHE_H */
