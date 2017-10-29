/* ==========================================================================
 * fifo.h - Simple byte FIFO with simple bit packing.
 * --------------------------------------------------------------------------
 * Copyright (c) 2008-2011  William Ahern
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
#ifndef FIFO_H
#define FIFO_H

#include <stddef.h>	/* size_t */
#include <stdint.h>	/* SIZE_MAX */
#include <stdio.h>	/* EOF FILE fputc(3) */
#include <stdlib.h>	/* realloc(3) free(3) */

#include <string.h>	/* memcpy(3) memmove(3) strlen(3) memchr(3) */

#include <ctype.h>	/* isgraph(3) */

#include <errno.h>	/* ENOMEM EOVERFLOW errno */

#include <assert.h>	/* assert(3) */

#include <sys/uio.h>	/* struct iovec */


/*
 * V E R S I O N  I N T E R F A C E S
 *
 * Vendor: Entity for which versions numbers are relevant. (If forking
 * change FIFO_VENDOR to avoid confusion.)
 *
 * Three versions:
 *
 * REL	Official "release"--bug fixes, new features, etc.
 * ABI	Changes to existing object sizes or parameter types.
 * API	Changes that might effect application source.
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define FIFO_VENDOR "william@25thandClement.com"

#define FIFO_V_REL  0x20111113 /* 0x20100904 */
#define FIFO_V_ABI  0x20111113 /* 0x20100815 */
#define FIFO_V_API  0x20111113 /* 0x20100904 */

static inline const char *fifo_vendor(void) { return FIFO_VENDOR; }

static inline int fifo_v_rel(void) { return FIFO_V_REL; }
static inline int fifo_v_abi(void) { return FIFO_V_ABI; }
static inline int fifo_v_api(void) { return FIFO_V_API; }


/*
 * M I S C E L L A N E O U S  M A C R O S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define FIFO_MIN(a, b) (((a) < (b))? (a) : (b))

#define FIFO_NARG_(_15, _14, _13, _12, _11, _10, _9, _8, _7, _6, _5, _4, _3, _2, _1, N, ...) N
#define FIFO_NARG(...) FIFO_NARG_(__VA_ARGS__, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)

#define FIFO_PASTE(a, b) a ## b
#define FIFO_XPASTE(a, b) FIFO_PASTE(a, b)


/*
 * O B J E C T  I N I T I A L I Z A T I O N  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#define FIFO_INITIALIZER { { 0 } }

#define fifo_new(size) fifo_init(&(struct fifo)FIFO_INITIALIZER, ((size))? (unsigned char [(size) + !(size)]){ 0 } : (void *)0, (size))

struct fifo {
	/* static buffer */
	struct iovec sbuf;

	unsigned char *base;
	size_t size, head, count;

	/* bit accumulators */
	struct {
		unsigned char byte, count;
	} rbits, wbits;
}; /* struct fifo */


#define FIFO_STATIC  (1 << 0)
#define FIFO_DYNAMIC (1 << 1)

static inline int fifo_type(struct fifo *fifo) {
	return (1 << !fifo->sbuf.iov_base);
} /* fifo_type() */


/*
 * Initialize fifo object as static or dynamic buffer.
 *
 *   - Dynamic: Only pass the fifo object to initialize. DO NOT forget to
 *              call fifo_reset() to release dynamic resources.
 *
 *   - Static:  Pass two additional arguments, pointer and size. Can safely
 *              call fifo_reset(), but not required.
 */
static struct fifo *fifo_init(struct fifo *fifo, void *buf, size_t size) {
	fifo->sbuf  = (struct iovec){ buf, size };
	fifo->base  = fifo->sbuf.iov_base;
	fifo->size  = fifo->sbuf.iov_len;
	fifo->head  = 0;
	fifo->count = 0;
	fifo->rbits.byte  = 0;
	fifo->rbits.count = 0;
	fifo->wbits.byte  = 0;
	fifo->wbits.count = 0;

	return fifo;
} /* fifo_init() */

#define fifo_init3(...)       (fifo_init)(__VA_ARGS__)
#define fifo_init2(fifo, buf) fifo_init3((fifo), (buf), __builtin_object_size((buf), 3))
#define fifo_init1(fifo)      fifo_init3((fifo), (void *)0, 0U)
#define fifo_init(...)        FIFO_XPASTE(fifo_init, FIFO_NARG(__VA_ARGS__))(__VA_ARGS__)


static inline size_t fifo_update(struct fifo *, size_t); /* forward declaration */

static inline struct fifo *fifo_from(struct fifo *fifo, void *src, size_t size) {
	fifo_init(fifo, src, size);
	fifo_update(fifo, size);
	return fifo;
} /* fifo_from() */

#define fifo_from3(...)       (fifo_from)(__VA_ARGS__)
#define fifo_from2(src, size) fifo_from3(&(struct fifo)FIFO_INITIALIZER, (src), (size))
#define fifo_from1(src)       fifo_from3(&(struct fifo)FIFO_INITIALIZER, (src), __builtin_object_size((src), 3))
#define fifo_from(...)        FIFO_XPASTE(fifo_from, FIFO_NARG(__VA_ARGS__))(__VA_ARGS__)


static struct fifo *fifo_reset() __attribute__((unused));

static struct fifo *fifo_reset(struct fifo *fifo) {
	if (fifo->base != fifo->sbuf.iov_base)
		free(fifo->base);

	return fifo_init(fifo, fifo->sbuf.iov_base, fifo->sbuf.iov_len);
} /* fifo_reset() */


static struct fifo *fifo_purge() __attribute__((unused));

static struct fifo *fifo_purge(struct fifo *fifo) {
	fifo->head  = 0;
	fifo->count = 0;
	fifo->rbits.byte  = 0;
	fifo->rbits.count = 0;
	fifo->wbits.byte  = 0;
	fifo->wbits.count = 0;
	return fifo;
} /* fifo_purge() */


/*
 * M E M O R Y  M A N A G E M E N T  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef FIFO_TMPBUFSIZ
#define FIFO_TMPBUFSIZ 2048
#endif

static void fifo_realign(struct fifo *fifo) {
	if (fifo->size - fifo->head >= fifo->count) {
		memmove(fifo->base, &fifo->base[fifo->head], fifo->count);
		fifo->head = 0;
	} else {
		unsigned char tmp[FIFO_TMPBUFSIZ];
		unsigned n, m;

		while (fifo->head != 0) {
			n = FIFO_MIN(fifo->head, sizeof tmp);
			m = fifo->size - n;

			memcpy(tmp, fifo->base, n);
			memmove(fifo->base, &fifo->base[n], m);
			memcpy(&fifo->base[m], tmp, n);
			fifo->head -= n;
		}
	}
} /* fifo_realign() */


static inline size_t fifo_power2(size_t i) {
#if defined SIZE_MAX
	i--;
	i |= i >> 1;
	i |= i >> 2;
	i |= i >> 4;
	i |= i >> 8;
	i |= i >> 16;
#if SIZE_MAX != 0xffffffffu
	i |= i >> 32;
#endif
	return ++i;
#else
#error No SIZE_MAX defined
#endif
} /* fifo_power2() */


static inline size_t fifo_roundup(size_t i) {
	if (i > ~(((size_t)-1) >> 1u))
		return (size_t)-1;
	else
		return fifo_power2(i);
} /* fifo_roundup() */


static int fifo_realloc(struct fifo *fifo, size_t size) {
	void *tmp;

	if (fifo->size >= size)
		return 0;
	if (fifo_type(fifo) == FIFO_STATIC)
		return ENOMEM;

	fifo_realign(fifo);

	size = fifo_roundup(size);

	if (!(tmp = realloc(fifo->base, size)))
		return errno;

	fifo->base = tmp;
	fifo->size = size;

	return 0;
} /* fifo_realloc() */


static inline int fifo_grow(struct fifo *fifo, size_t size) {
	if (fifo->size - fifo->count >= size)
		return 0;

	if (~fifo->count < size)
		return EOVERFLOW;

	return fifo_realloc(fifo, fifo->count + size);
} /* fifo_grow() */


/*
 * V E C T O R  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

static inline size_t fifo_rlen(struct fifo *fifo) {
	return fifo->count;
} /* fifo_rlen() */


static inline size_t fifo_wlen(struct fifo *fifo) {
	return fifo->size - fifo->count;
} /* fifo_wlen() */


static size_t fifo_rvec(struct fifo *fifo, struct iovec *iov, _Bool realign) {
	if (fifo->head + fifo->count > fifo->size && realign)
		fifo_realign(fifo);

	iov->iov_base = &fifo->base[fifo->head];
	iov->iov_len  = FIFO_MIN(fifo->size - fifo->head, fifo->count);

	return iov->iov_len;
} /* fifo_rvec() */

#define fifo_rvec3(fifo, iov, realign, ...) (fifo_rvec)((fifo), (iov), (realign))
#define fifo_rvec(...) fifo_rvec3(__VA_ARGS__, 0)


static size_t fifo_wvec(struct fifo *fifo, struct iovec *iov, _Bool realign) {
	size_t tail, count;

	if (fifo->head + fifo->count < fifo->size && realign)
		fifo_realign(fifo);

	tail  = (fifo->size)? ((fifo->head + fifo->count) % fifo->size) : 0;
	count = fifo_wlen(fifo);

	iov->iov_base	= &fifo->base[tail];
	iov->iov_len	= FIFO_MIN(fifo->size - tail, count);

	return iov->iov_len;
} /* fifo_wvec() */

#define fifo_wvec3(fifo, iov, realign, ...) (fifo_wvec)((fifo), (iov), (realign))
#define fifo_wvec(...) fifo_wvec3(__VA_ARGS__, 0)


static size_t fifo_slice() __attribute__((unused));

static size_t fifo_slice(struct fifo *fifo, struct iovec *iov, size_t p, size_t count) {
	size_t pe;

	if (p > fifo->count) {
		iov->iov_base = 0;
		iov->iov_len  = 0;

		return 0;
	}

	count = FIFO_MIN(count, fifo->count - p);
	pe    = p + count;

	if (fifo->head + p < fifo->size && fifo->head + pe > fifo->size)
		fifo_realign(fifo);

	iov->iov_base = &fifo->base[((fifo->head + p) % fifo->size)];
	iov->iov_len  = count;

	return count;
} /* fifo_slice() */


static size_t fifo_tvec(struct fifo *fifo, struct iovec *iov, int ch) {
	unsigned char *p;

	if (fifo_rvec(fifo, iov)) {
		if ((p = memchr(iov->iov_base, ch, iov->iov_len)))
			return iov->iov_len = (p - (unsigned char *)iov->iov_base) + 1;

		if (fifo->count > iov->iov_len) {
			iov->iov_len = fifo->count - iov->iov_len;
			iov->iov_base = fifo->base;

			if ((p = memchr(iov->iov_base, ch, iov->iov_len))) {
				iov->iov_len = (p - fifo->base) + (fifo->size - fifo->head) + 1;
				iov->iov_base = fifo->base;
				fifo_realign(fifo);

				return iov->iov_len;
			}
		}

		iov->iov_len = 0;
	}

	return 0;
} /* fifo_tvec() */


static inline size_t fifo_lvec(struct fifo *fifo, struct iovec *iov) {
	return fifo_tvec(fifo, iov, '\n');
} /* fifo_lvec() */


/*
 * R E A D  /  W R I T E   R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef FIFO_AUTOALIGN
#define FIFO_AUTOALIGN 1
#endif

static inline size_t fifo_discard(struct fifo *fifo, size_t count) {
	count       = FIFO_MIN(count, fifo->count);
	fifo->head  = (fifo->head + count) % fifo->size;
	fifo->count -= count;
#if FIFO_AUTOALIGN
	if (!fifo->count)
		fifo->head = 0;
#endif
	return count;
} /* fifo_discard() */


static inline size_t fifo_update(struct fifo *fifo, size_t count) {
	count       = FIFO_MIN(count, fifo->size - fifo->count);
	fifo->count += count;
	return count;
} /* fifo_update() */


static inline size_t fifo_rewind(struct fifo *fifo, size_t count) {
	count       = FIFO_MIN(count, fifo->size - fifo->count);
	fifo->head  = (fifo->head + (fifo->size - fifo->count)) % fifo->size;
	fifo->count += count;
	return count;
} /* fifo_rewind() */


static int fifo_write() __attribute__((unused));

static int fifo_write(struct fifo *fifo, const void *src, size_t len) {
	const unsigned char *p = src, *pe = p + len;
	struct iovec iov;
	size_t n;
	int error = 0;

	do {
		while (fifo_wvec(fifo, &iov) && p < pe) {
			n = FIFO_MIN(iov.iov_len, (size_t)(pe - p));
			memcpy(iov.iov_base, p, n);
			p += n;
			fifo_update(fifo, n);
		}
	} while (p < pe && !(error = fifo_grow(fifo, pe - p)));

	return error;
} /* fifo_write() */


static size_t fifo_read() __attribute__((unused));

static size_t fifo_read(struct fifo *fifo, void *dst, size_t lim) {
	unsigned char *p = dst, *pe = p + lim;
	struct iovec iov;
	size_t n;

	while (p < pe && fifo_rvec(fifo, &iov)) {
		n = FIFO_MIN(iov.iov_len, (size_t)(pe - p));
		memcpy(p, iov.iov_base, n);
		p += n;
		fifo_discard(fifo, n);
	}

	return p - (unsigned char *)dst;
} /* fifo_read() */


static int fifo_putc(struct fifo *fifo, int c) {
	int error;

	if (fifo->count >= fifo->size && (error = fifo_grow(fifo, 1)))
		return error;

	fifo->base[(fifo->head + fifo->count) % fifo->size] = (0xff & c);
	fifo_update(fifo, 1);

	return 0;
} /* fifo_putc() */


static inline int fifo_puts(struct fifo *fifo, const void *src) {
	return fifo_write(fifo, src, strlen(src));
} /* fifo_puts() */


static inline int fifo_ungetc(struct fifo *fifo, int c)  {
	int error;

	if ((error = fifo_grow(fifo, 1)))
		return error;

	assert(1 == fifo_rewind(fifo, 1));
	fifo->base[fifo->head] = c;

	return 0;
} /* fifo_ungetc() */


static inline int fifo_getc(struct fifo *fifo) {
	int c;

	if (!fifo->count)
		return EOF;

	c = fifo->base[fifo->head];
	fifo_discard(fifo, 1);

	return c;
} /* fifo_getc() */


static inline int fifo_peek(struct fifo *fifo, size_t p) {
	if (p >= fifo->count)
		return EOF;

	return fifo->base[(fifo->head + p) % fifo->size];
} /* fifo_peek() */


static inline int fifo_scan(struct fifo *fifo, size_t *p) {
	return fifo_peek(fifo, (*p)++);
} /* fifo_scan() */


#define FIFO_NOINFO 1

static void fifo_dump() __attribute__((unused));

static void fifo_dump(struct fifo *fifo, FILE *fp, int flags) {
	static const unsigned char hex[] = "0123456789abcdef";
	size_t p, n;
	int ch;

	if (!(FIFO_NOINFO & flags))
		fprintf(fp, "%zu of %zu bytes in FIFO; %u unread bits (0x%.2x); %u unflushed bits (0x%.2x)\n", fifo->count, fifo->size, fifo->rbits.count, (((1 << fifo->rbits.count) - 1) & fifo->rbits.byte), fifo->wbits.count, (((1 << fifo->wbits.count) - 1) & fifo->wbits.byte));

	for (p = 0; p < fifo->count; p += 16) {
		fprintf(fp, "  %.4x  ", (unsigned)p);

		for (n = 0; n < 8 && EOF != (ch = fifo_peek(fifo, p)); n++, p++) {
			fputc(hex[0x0f & (ch >> 4)], fp);
			fputc(hex[0x0f & (ch >> 0)], fp);
			fputc(' ', fp);
		}

		fputc(' ', fp);

		for (; n < 16 && EOF != (ch = fifo_peek(fifo, p)); n++, p++) {
			fputc(hex[0x0f & (ch >> 4)], fp);
			fputc(hex[0x0f & (ch >> 0)], fp);
			fputc(' ', fp);
		}

		p -= n;

		for (; n < 16; n++) {
			fputc(' ', fp);
			fputc(' ', fp);
			fputc(' ', fp);
		}

		fputc(' ', fp);
		fputc('|', fp);
	
		for (n = 0; n < 16 && EOF != (ch = fifo_peek(fifo, p)); n++, p++) {
			fputc((isgraph(ch)? ch : '.'), fp);
		}

		p -= n;

		for (; n < 16; n++)
			fputc(' ', fp);

		fputc('|', fp);
		fputc('\n', fp);
	}
} /* fifo_dump() */

#define fifo_dump3(...) fifo_dump(__VA_ARGS__)
#define fifo_dump2(...) fifo_dump3(__VA_ARGS__, 0)
#define fifo_dump1(...) fifo_dump3(__VA_ARGS__, stderr, 0)
#define fifo_dump(...)   FIFO_XPASTE(fifo_dump, FIFO_NARG(__VA_ARGS__))(__VA_ARGS__)


/*
 * B I T  P A C K I N G   R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

static inline size_t fifo_rbits(struct fifo *fifo) {
	return fifo->rbits.count + (8 * fifo_rlen(fifo));
} /* fifo_rbits() */


static inline size_t fifo_wbits(struct fifo *fifo) {
	return (8 - fifo->wbits.count) + (8 * fifo_wlen(fifo));
} /* fifo_wbits() */


static unsigned long long fifo_unpack() __attribute__((unused));

static unsigned long long fifo_unpack(struct fifo *fifo, unsigned count) {
	unsigned long long bits = 0;
	unsigned mask, nbits;

	if (fifo_rbits(fifo) < count)
		return 0;

	while (count) {
		if (!fifo->rbits.count) {
			fifo->rbits.byte  = fifo_getc(fifo);
			fifo->rbits.count = 8;
		}

		nbits = FIFO_MIN(count, fifo->rbits.count);
		mask  = (1 << nbits) - 1;

		bits <<= nbits;
		bits |= mask & (fifo->rbits.byte >> (fifo->rbits.count - nbits));

		fifo->rbits.count -= nbits;
		count             -= nbits;
	}

	return bits;
} /* fifo_unpack() */


static int fifo_pack() __attribute__((unused));

static int fifo_pack(struct fifo *fifo, unsigned long long bits, unsigned count) {
	unsigned mask, nbits;
	int error;

	if (fifo_wbits(fifo) < count && (error = fifo_grow(fifo, 8)))
		return error;

	while (count) {
		nbits = FIFO_MIN(count, 8U - fifo->wbits.count);
		mask  = (1U << nbits) - 1;

		fifo->wbits.byte  <<= nbits;
		fifo->wbits.byte  |= mask & (bits >> (count - nbits));
		fifo->wbits.count += nbits;

		count -= nbits;

		if (fifo->wbits.count >= 8) {
			fifo_putc(fifo, fifo->wbits.byte);

			fifo->wbits.byte  = 0;
			fifo->wbits.count = 0;
		}
	}

	return 0;
} /* fifo_pack() */

#endif /* FIFO_H */


#if FIFO_MAIN

#include <locale.h>

int main(void) {
	struct fifo *fifo = fifo_new(0);
	struct iovec iov;
	int ch;

	setlocale(LC_ALL, "C");

	while (EOF != (ch = getchar()))
		fifo_putc(fifo, ch);

	puts("[stdin]");
	fifo_dump(fifo, stdout);

	puts("[shifted 4 bits]");
	fifo_pack(fifo, fifo_unpack(fifo, 4), 4);
	fifo_dump(fifo, stdout);

	puts("[shifted 1/2 count]");
	for (size_t i = 0; i < fifo->count / 2; i++)
		fifo_putc(fifo, fifo_getc(fifo));
	fifo_dump(fifo, stdout);

	puts("[slice of 17 bytes at 17]");
	fifo_slice(fifo, &iov, 17, 17);
	fifo_dump(fifo_from(iov.iov_base, iov.iov_len), stdout);

	return 0;
} /* main() */

#endif /* FIFO_MAIN */

