/* ==========================================================================
 * socket.c - Simple Sockets
 * --------------------------------------------------------------------------
 * Copyright (c) 2009, 2010, 2011, 2012  William Ahern
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
#include <stddef.h>	/* offsetof size_t */

#include <limits.h>	/* INT_MAX LONG_MAX */

#include <stdlib.h>	/* malloc(3) free(3) */

#include <string.h>	/* strlen(3) memset(3) strncpy(3) memcpy(3) strerror(3) */

#include <errno.h>	/* EINVAL EAGAIN EWOULDBLOCK EINPROGRESS EALREADY ENAMETOOLONG EOPNOTSUPP ENOTSOCK ENOPROTOOPT */

#include <signal.h>	/* SIGPIPE SIG_BLOCK SIG_SETMASK sigset_t sigprocmask(2) pthread_sigmask(3) sigtimedwait(2) sigpending(2) sigemptyset(3) sigismember(3) sigaddset(3) */

#include <assert.h>	/* assert(3) */

#include <time.h>	/* time(2) */

#include <sys/types.h>	/* socklen_t mode_t in_port_t */
#include <sys/stat.h>	/* fchmod(2) fstat(2) */
#include <sys/select.h>	/* FD_ZERO FD_SET fd_set select(2) */
#include <sys/socket.h>	/* AF_UNIX AF_INET AF_INET6 SO_NOSIGPIPE MSG_NOSIGNAL struct sockaddr_storage socket(2) connect(2) bind(2) listen(2) accept(2) getsockname(2) getpeername(2) */

#if defined(AF_UNIX)
#include <sys/un.h>	/* struct sockaddr_un */
#endif

#include <netinet/in.h>	/* struct sockaddr_in struct sockaddr_in6 */

#include <netinet/tcp.h> /* IPPROTO_TCP TCP_NODELAY TCP_NOPUSH TCP_CORK */

#include <arpa/inet.h>	/* inet_ntop(3) inet_pton(3) ntohs(3) htons(3) */

#include <netdb.h>	/* struct addrinfo */

#include <unistd.h>	/* _POSIX_REALTIME_SIGNALS _POSIX_THREADS close(2) unlink(2) */

#include <fcntl.h>	/* F_SETFD F_GETFD F_GETFL F_SETFL FD_CLOEXEC O_NONBLOCK O_NOSIGPIPE F_SETNOSIGPIPE F_GETNOSIGPIPE */

#include <poll.h>	/* POLLIN POLLOUT */

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "dns.h"

#include "socket.h"


/*
 * V E R S I O N  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

const char *socket_vendor(void) {
	return SOCKET_VENDOR;
} /* socket_vendor() */


int socket_v_rel(void) {
	return SOCKET_V_REL;
} /* socket_v_rel() */


int socket_v_abi(void) {
	return SOCKET_V_ABI;
} /* socket_v_abi() */


int socket_v_api(void) {
	return SOCKET_V_API;
} /* socket_v_api() */


/*
 * F E A T U R E  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#if !defined SO_THREAD_SAFE
#if (defined _REENTRANT || defined _THREAD_SAFE) && _POSIX_THREADS > 0
#define SO_THREAD_SAFE 1
#else
#define SO_THREAD_SAFE 0
#endif
#endif


/*
 * D E B U G  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int socket_debug;


enum so_trace {
	SO_T_CONNECT,
	SO_T_STARTTLS,
	SO_T_READ,
	SO_T_WRITE,
}; /* enum so_trace */


static void so_trace(enum so_trace, int, const struct addrinfo *, ...);


#if !defined(SOCKET_DEBUG)
#define SOCKET_DEBUG 0
#endif

#if SOCKET_DEBUG

#include <stdio.h>
#include <stdarg.h>

#include <ctype.h>


#undef SOCKET_DEBUG
#define SOCKET_DEBUG socket_debug


#if !defined(SAY)
#define SAY_(fmt, ...) fprintf(stderr, fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__);
#define SAY(...) SAY_("@@ %s:%d:%s: " __VA_ARGS__, "\n");
#endif

#if !defined(MARK)
#define MARK SAY_("@@ %s:%d:%s", "\n");
#endif


static void so_dump(const unsigned char *src, size_t len, FILE *fp) {
	static const unsigned char hex[] = "0123456789abcdef";
	static const unsigned char tmp[] = "                                                            |                |\n";
	unsigned char ln[sizeof tmp];
	const unsigned char *p, *pe;
	unsigned char *h, *g;
	unsigned i, n;

	p  = src;
	pe = p + len;

	while (p < pe) {
		memcpy(ln, tmp, sizeof ln);

		h = &ln[2];
		g = &ln[61];

		n = p - src;
		h[0] = hex[0x0f & (n >> 20)];
		h[1] = hex[0x0f & (n >> 16)];
		h[2] = hex[0x0f & (n >> 12)];
		h[3] = hex[0x0f & (n >> 8)];
		h[4] = hex[0x0f & (n >> 4)];
		h[5] = hex[0x0f & (n >> 0)];
		h += 8;

		for (n = 0; n < 2; n++) {
			for (i = 0; i < 8 && pe - p > 0; i++, p++) {
				h[0] = hex[0x0f & (*p >> 4)];
				h[1] = hex[0x0f & (*p >> 0)];
				h += 3;

				*g++ = (isgraph(*p))? *p : '.';
			}

			h++;
		}

		fputs((char *)ln, fp);
	}
} /* so_dump() */


static void so_trace(enum so_trace event, int fd, const struct addrinfo *host, ...) {
	struct sockaddr_storage saddr;
	char addr[64], who[256];
	in_port_t port;
	va_list ap;
	SSL *ctx;
	const void *data;
	size_t count;
	const char *fmt;

	if (!socket_debug)
		return;

	if (host) {
		sa_ntop(addr, sizeof addr, host->ai_addr);
		port = *sa_port(host->ai_addr);

		if (host->ai_canonname)
			snprintf(who, sizeof who, "%.96s/[%s]:%hu", host->ai_canonname, addr, ntohs(port));
		else
			snprintf(who, sizeof who, "[%s]:%hu", addr, ntohs(port));
	} else if (fd != -1 && 0 == getpeername(fd, (struct sockaddr *)&saddr, &(socklen_t){ sizeof saddr })) {
		sa_ntop(addr, sizeof addr, &saddr);
		port = *sa_port(&saddr);

		snprintf(who, sizeof who, "[%s]:%hu", addr, ntohs(port));
	} else
		dns_strlcpy(who, "[unknown]", sizeof who);

	va_start(ap, host);

	flockfile(stderr);

	switch (event) {
	case SO_T_CONNECT:
		fmt = va_arg(ap, char *);

		fprintf(stderr, "connect(%s): ", who);
		vfprintf(stderr, fmt, ap);
		fputc('\n', stderr);

		break;
	case SO_T_STARTTLS:
		ctx = va_arg(ap, SSL *);
		fmt = va_arg(ap, char *);

		fprintf(stderr, "starttls(%s): ", who);
		vfprintf(stderr, fmt, ap);
		fputc('\n', stderr);

		break;
	case SO_T_READ:
		data  = va_arg(ap, void *);
		count = va_arg(ap, size_t);
		fmt   = va_arg(ap, char *);

		fprintf(stderr, "read(%s): ", who);
		vfprintf(stderr, fmt, ap);
		fputc('\n', stderr);

		so_dump(data, count, stderr);

		break;
	case SO_T_WRITE:
		data  = va_arg(ap, void *);
		count = va_arg(ap, size_t);
		fmt   = va_arg(ap, char *);

		fprintf(stderr, "write(%s): ", who);
		vfprintf(stderr, fmt, ap);
		fputc('\n', stderr);

		so_dump(data, count, stderr);

		break;
	} /* switch(event) */

	funlockfile(stderr);

	va_end(ap);
} /* so_trace() */


static void so_initdebug(void) {
	const char *debug = getenv("SOCKET_DEBUG")? : getenv("SO_DEBUG");

	if (debug) {
		switch (*debug) {
		case 'Y': case 'y':
		case 'T': case 't':
		case '1':
			socket_debug = 1;

			break;
		case 'N': case 'n':
		case 'F': case 'f':
		case '0':
			socket_debug = 0;

			break;
		} /* switch() */
	}
} /* so_initdebug() */


#else

#define so_trace(...) (void)0

#define so_initdebug() (void)0

#endif /* SOCKET_DEBUG */


/*
 * M A C R O  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef lengthof
#define lengthof(a) (sizeof (a) / sizeof *(a))
#endif

#ifndef endof
#define endof(a) (&(a)[lengthof(a)])
#endif


/*
 * E R R O R  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#if _WIN32

#define SO_EINTR	WSAEINTR
#define SO_EINPROGRESS	WSAEINPROGRESS
#define SO_EISCONN	WSAEISCONN
#define SO_EWOULDBLOCK	WSAEWOULDBLOCK
#define SO_EALREADY	WSAEALREADY
#define SO_EAGAIN	WSAEWOULDBLOCK
#define SO_ENOTCONN	WSAENOTCONN

#define so_syerr()	((int)GetLastError())
#define so_soerr()	((int)WSAGetLastError())

#else

#define SO_EINTR	EINTR
#define SO_EINPROGRESS	EINPROGRESS
#define SO_EISCONN	EISCONN
#define SO_EWOULDBLOCK	EWOULDBLOCK
#define SO_EALREADY	EALREADY
#define SO_EAGAIN	EAGAIN
#define SO_ENOTCONN	ENOTCONN

#define so_syerr()	errno
#define so_soerr()	errno

#endif


const char *so_strerror(int error) {
	static const char *errlist[] = {
		[SO_EOPENSSL - SO_ERRNO0] = "TLS/SSL error",
		[SO_EX509INT - SO_ERRNO0] = "X.509 certificate lookup interrupt",
		[SO_ENOTVRFD - SO_ERRNO0] = "absent or unverified peer certificate",
		[SO_ECLOSURE - SO_ERRNO0] = "peers elected to shutdown secure transport",
	};

	if (error >= 0)
		return strerror(error);

	if (error == SO_EOPENSSL) {
#if SO_THREAD_SAFE
		static __thread char sslstr[256];
#else
		static char sslstr[256];
#endif
		unsigned long code = ERR_peek_last_error();

		if (!code)
			return "Unknown TLS/SSL error";

		ERR_error_string_n(code, sslstr, sizeof sslstr);

		return sslstr;
	} else {
		int index = error - SO_ERRNO0;

		if (index >= 0 && index < (int)lengthof(errlist) && errlist[index])
			return errlist[index];
		else
			return "Unknown socket error";
	}
} /* so_strerror() */


/*
 * Translate SSL_get_error(3) errors into something sensible.
 */
static int ssl_error(SSL *ctx, int rval, short *events) {
	unsigned long code;

	switch (SSL_get_error(ctx, rval)) {
	case SSL_ERROR_ZERO_RETURN:
		return SO_ECLOSURE;
	case SSL_ERROR_WANT_READ:
		*events |= POLLIN;

		return SO_EAGAIN;
	case SSL_ERROR_WANT_WRITE:
		*events |= POLLOUT;

		return SO_EAGAIN;
	case SSL_ERROR_WANT_CONNECT:
		*events |= POLLOUT;

		return SO_EAGAIN;
	case SSL_ERROR_WANT_ACCEPT:
		*events |= POLLIN;

		return SO_EAGAIN;
	case SSL_ERROR_WANT_X509_LOOKUP:
		return SO_EX509INT;
	case SSL_ERROR_SYSCALL:
		if ((code = ERR_peek_last_error()))
			return SO_EOPENSSL;
		else if (rval == 0)
			return ECONNRESET;
		else if (rval == -1 && so_soerr() && so_soerr() != SO_EAGAIN)
			return so_soerr();
		else
			return SO_EOPENSSL;
	case SSL_ERROR_SSL:
		/* FALL THROUGH */
	default:
		return SO_EOPENSSL;
	} /* switch(SSL_get_error()) */
} /* ssl_error() */


/*
 * A D D R E S S  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

char *sa_ntop(void *dst, size_t lim, const void *arg) {
	union {
		struct sockaddr sa;
		struct sockaddr_in sin;
		struct sockaddr_in6 sin6;
#if SA_UNIX
		struct sockaddr_un sun;
#endif
	} *any = (void *)arg;
	char sbuf[SA_ADDRSTRLEN];

	switch (*sa_family(&any->sa)) {
	case AF_INET:
		if (!inet_ntop(AF_INET, &any->sin.sin_addr, sbuf, sizeof sbuf))
			goto error;

		break;
	case AF_INET6:
		if (!inet_ntop(AF_INET6, &any->sin6.sin6_addr, sbuf, sizeof sbuf))
			goto error;

		break;
#if SA_UNIX
	case AF_UNIX:
		memset(sbuf, 0, sizeof sbuf);
		memcpy(sbuf, any->sun.sun_path, SO_MIN(sizeof sbuf - 1, sizeof any->sun.sun_path));

		break;
#endif
	default:
		goto error;
	} /* switch() */

	dns_strlcpy(dst, sbuf, lim);

	return dst;
error:
	dns_strlcpy(dst, "0.0.0.0", lim);

	return dst;
} /* sa_ntop() */


void *sa_pton(void *any, size_t lim, const char *addr) {
	union {
		struct sockaddr sa;
		struct sockaddr_in sin;
		struct sockaddr_in6 sin6;
	} saddr[2] = { { { .sa_family = AF_INET } }, { { .sa_family = AF_INET6 } } };
	unsigned i;
	int ret;

	memset(any, 0, lim);

	for (i = 0; i < sizeof saddr / sizeof saddr[0]; i++) {
		ret = inet_pton(*sa_family(&saddr[i]), addr, sa_addr(&saddr[i]));

		assert(-1 != ret);

		if (ret == 1) {
			if (lim >= sa_len(&saddr[i]))
				memcpy(any, &saddr[i], sa_len(&saddr[i]));

			break;
		}
	}

	return any;
} /* sa_pton() */


/*
 * U T I L I T I Y  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void *sa_egress(void *lcl, size_t lim, sockaddr_arg_t rmt, int *error_) {
	static struct { sa_family_t pf; int fd;} udp4 = { PF_INET, -1 }, udp6 = { PF_INET6, -1 }, *udp;
	struct sockaddr_storage ss;
	int error;

	switch (*sa_family(rmt)) {
	case AF_INET:
		udp = &udp4;

		break;
	case AF_INET6:
		udp = &udp6;

		break;
	default:
		error = EINVAL;

		goto error;
	}

	if (udp->fd == -1) {
		if (-1 == (udp->fd = socket(udp->pf, SOCK_DGRAM, 0)))
			goto syerr;

		if ((error = so_cloexec(udp->fd, 1))) {
			so_closesocket(&udp->fd);

			goto error;
		}
	}

	assert(sizeof ss >= sa_len(rmt));
	memcpy(&ss, sockaddr_ref(rmt).sa, sa_len(rmt));

	if (!*sa_port(&ss))
		*sa_port(&ss) = htons(6970);

	if (0 != connect(udp->fd, (struct sockaddr *)&ss, sa_len(&ss)))
		goto syerr;

	if (0 != getsockname(udp->fd, (struct sockaddr *)&ss, &(socklen_t){ sizeof ss }))
		goto syerr;

	if (lim < sa_len(&ss)) {
		error = ENOSPC;

		goto error;
	}

	memcpy(lcl, &ss, sa_len(&ss));

	return lcl;
syerr:
	error = so_syerr();
error:
	if (error_)
		*error_ = error;

	return memset(lcl, 0, lim);
} /* sa_egress() */


static int so_opts2flags(const struct so_options *);

int so_socket(int domain, int type, const struct so_options *opts, int *_error) {
	int error, fd, flags, mask, need;

	if (-1 == (fd = socket(domain, type, 0)))
		goto syerr;

	flags = so_opts2flags(opts);
	mask  = (domain == SOCK_STREAM)? ~0 : ~(SF_NODELAY|SF_NOPUSH);
	need  = ~(SF_NODELAY|SF_NOPUSH|SF_NOSIGPIPE);

	if ((error = so_setfl(fd, flags, mask, need)))
		goto error;

	return fd;
syerr:
	error = so_syerr();

	goto error;
error:
	*_error = error;

	so_closesocket(&fd);

	return -1;
} /* so_socket() */


int so_bind(int fd, sockaddr_arg_t arg, const struct so_options *opts) {
#if SA_UNIX
	if (*sa_family(arg) == AF_UNIX && opts->sun_unlink) {
		(void)unlink(strncpy((char [sizeof sockaddr_ref(arg).sun->sun_path + 1]){ 0 }, sockaddr_ref(arg).sun->sun_path, sizeof sockaddr_ref(arg).sun->sun_path));
	}
#endif
	if (0 != bind(fd, sockaddr_ref(arg).sa, sa_len(arg)))
		return so_soerr();

#if SA_UNIX
	if (*sa_family(arg) == AF_UNIX && opts->sun_mode) {
		if (0 != fchmod(fd, opts->sun_mode))
			return so_syerr();
	}
#endif

	return 0;
} /* so_bind() */


void so_closesocket(int *fd) {
	if (*fd != -1) {
#if _WIN32
		closesocket(*fd);
#else
		close(*fd);
#endif

		*fd = -1;
	}
} /* so_closesocket() */


int so_cloexec(int fd, _Bool cloexec) {
#if _WIN32
	return 0;
#else
	if (-1 == fcntl(fd, F_SETFD, cloexec))
		return so_syerr();

	return 0;
#endif
} /* so_cloexec() */


int so_nonblock(int fd, _Bool nonblock) {
	int flags, mask = (nonblock)? ~0 : (~O_NONBLOCK);

	if (-1 == (flags = fcntl(fd, F_GETFL))
	||  -1 == fcntl(fd, F_SETFL, mask & (flags | O_NONBLOCK)))
		return so_syerr();

	return 0;
} /* so_nonblock() */


static _Bool so_getboolopt(int fd, int lvl, int opt) {
	int val;

	if (0 != getsockopt(fd, lvl, opt, &val, &(socklen_t){ sizeof val }))
		return 0;

	return !!val;
} /* so_getboolopt() */


static int so_setboolopt(int fd, int lvl, int opt, _Bool enable) {
	if (0 != setsockopt(fd, lvl, opt, &(int){ enable }, sizeof (int))) {
		switch (errno) {
		case ENOTSOCK:
			/* FALL THROUGH */
		case ENOPROTOOPT:
			return EOPNOTSUPP;
		default:
			return errno;
		}
	}

	return 0;
} /* so_setboolopt() */


int so_reuseaddr(int fd, _Bool reuseaddr) {
	return so_setboolopt(fd, SOL_SOCKET, SO_REUSEADDR, reuseaddr);
} /* so_reuseaddr() */


int so_nodelay(int fd, _Bool nodelay) {
	return so_setboolopt(fd, IPPROTO_TCP, TCP_NODELAY, nodelay);
} /* so_nodelay() */


#ifndef TCP_NOPUSH
#ifdef TCP_CORK
#define TCP_NOPUSH TCP_CORK
#endif
#endif

int so_nopush(int fd, _Bool nopush) {
#ifdef TCP_NOPUSH
	return so_setboolopt(fd, IPPROTO_TCP, TCP_NOPUSH, nopush);
#else
	return EOPNOTSUPP;
#endif
} /* so_nopush() */


int so_nosigpipe(int fd, _Bool nosigpipe) {
#if defined O_NOSIGPIPE
	int flags, mask = (nosigpipe)? ~0 : (~O_NOSIGPIPE);

	if (-1 == (flags = fcntl(fd, F_GETFL))
	||  -1 == fcntl(fd, F_SETFL, mask & (flags | O_NOSIGPIPE)))
		return errno;

	return 0;
#elif defined F_SETNOSIGPIPE
	if (0 != fcntl(fd, F_SETNOSIGPIPE, &(int){ nosigpipe }, sizeof (int)))
		return errno;

	return 0;
#elif defined SO_NOSIGPIPE
	return so_setboolopt(fd, SOL_SOCKET, SO_NOSIGPIPE, nosigpipe);
#else
	return EOPNOTSUPP;
#endif
} /* so_nosigpipe() */


#define optoffset(m) offsetof(struct so_options, m)

static const struct flops {
	int flag;
	int (*set)(int, _Bool);
	size_t offset;
} fltable[] = {
	{ SF_CLOEXEC,   &so_cloexec,   optoffset(fd_cloexec),    },
	{ SF_NONBLOCK,  &so_nonblock,  optoffset(fd_nonblock),   },
	{ SF_REUSEADDR, &so_reuseaddr, optoffset(sin_reuseaddr), },
	{ SF_NODELAY,   &so_nodelay,   optoffset(sin_nodelay),   },
	{ SF_NOPUSH,    &so_nopush,    optoffset(sin_nopush),    },
	{ SF_NOSIGPIPE, &so_nosigpipe, optoffset(fd_nosigpipe),  },
};


static int so_opts2flags(const struct so_options *opts) {
	const struct flops *f;
	int flags = 0;

	for (f = fltable; f < endof(fltable); f++) {
		flags |= (*(_Bool *)((char *)opts + f->offset))? f->flag : 0;
	}

	return flags;
} /* so_opts2flags() */


int so_getfl(int fd, int which) {
	int flags = 0, getfl = 0, getfd;

	if ((which & SF_CLOEXEC) && -1 != (getfd = fcntl(fd, F_GETFD))) {
		if (getfd & FD_CLOEXEC)
			flags |= SF_CLOEXEC;
	}

	if ((which & SF_NONBLOCK) && -1 != (getfl = fcntl(fd, F_GETFL))) {
		if (getfl & O_NONBLOCK)
			flags |= SF_NONBLOCK;
	}

	if ((which & SF_REUSEADDR) && so_getboolopt(fd, SOL_SOCKET, SO_REUSEADDR))
		flags |= SF_REUSEADDR;

	if ((which & SF_NODELAY) && so_getboolopt(fd, IPPROTO_TCP, TCP_NODELAY))
		flags |= SF_NODELAY;

#if defined TCP_NOPUSH
	if ((which & SF_NOPUSH) && so_getboolopt(fd, IPPROTO_TCP, TCP_NOPUSH))
		flags |= SF_NOPUSH;
#endif

#if defined O_NOSIGPIPE || defined F_GETNOSIGPIPE || defined SO_NOSIGPIPE
	if ((which & SF_NOSIGPIPE)) {
#if defined O_NOSIGPIPE
		if (getfl) {
			if (getfl != -1 && (getfl & O_NOSIGPIPE))
				flags |= SF_NOSIGPIPE;
		} else if (-1 != (getfl = fcntl(fd, F_GETFL))) {
			if (getfl & O_NOSIGPIPE)
				flags |= SF_NOSIGPIPE;
		}
#elif defined F_GETNOSIGPIPE
		int nosigpipe;

		if (-1 != (nosigpipe = fcntl(fd, F_GETNOSIGPIPE))) {
			if (nosigpipe)
				flags |= SF_NOSIGPIPE;
		}
#else
		if (so_getboolopt(fd, SOL_SOCKET, SO_NOSIGPIPE))
			flags |= SF_NOSIGPIPE;
#endif
	}
#endif

	return flags;
} /* so_getfl() */


int so_rstfl(int fd, int *oflags, int flags, int mask, int require) {
	const struct flops *f;
	int error;

	for (f = fltable; f < endof(fltable); f++) {
		if (!(f->flag & mask))
			continue;

		if ((error = f->set(fd, !!(f->flag & flags)))) {
			if ((f->flag & require) || error != EOPNOTSUPP)
				return error;

			*oflags &= ~f->flag;
		} else {
			*oflags &= ~f->flag;
			*oflags |= (f->flag & flags);
		}
	}

	return 0;
} /* so_rstfl() */


int so_setfl(int fd, int flags, int mask, int require) {
	return so_rstfl(fd, &(int){ 0 }, flags, mask, require);
} /* so_setfl() */


int (so_addfl)(int fd, int flags, int require) {
	return so_rstfl(fd, &(int){ 0 }, flags, flags, require);
} /* so_addfl() */


int (so_delfl)(int fd, int flags, int require) {
	return so_rstfl(fd, &(int){ 0 }, ~flags, flags, require);
} /* so_delfl() */


static void x509_discard(X509 **cert) {
	if (*cert)
		X509_free(*cert);
	*cert = 0;
} /* x509_discard() */


static void ssl_discard(SSL **ctx) {
	if (*ctx)
		SSL_free(*ctx);
	*ctx = 0;
} /* ssl_discard() */


static int thr_sigmask(int how, const sigset_t *set, sigset_t *oset) {
#if SO_THREAD_SAFE
	return pthread_sigmask(how, set, oset);
#else
	return (0 == sigprocmask(how, set, oset))? 0 : errno;
#endif
} /* thr_sigmask() */


static int math_addull(unsigned long long *x, unsigned long long a, unsigned long long b) {
	if (~a < b) {
		*x = ~0ULL;

		return EOVERFLOW;
	} else {
		*x = a + b;

		return 0;
	}
} /* math_addull() */


static void st_update(struct st_log *log, size_t len, const struct so_options *opts) {
	math_addull(&log->count, log->count, len);

	if (opts->st_time)
		time(&log->time);
} /* st_update() */


/*
 * S O C K E T  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

enum so_state {
	SO_S_INIT     = 1<<0,
	SO_S_GETADDR  = 1<<1,
	SO_S_SOCKET   = 1<<2,
	SO_S_BIND     = 1<<3,
	SO_S_LISTEN   = 1<<4,
	SO_S_CONNECT  = 1<<5,
	SO_S_STARTTLS = 1<<6,
	SO_S_SETREAD  = 1<<7,
	SO_S_SETWRITE = 1<<8,
	SO_S_RSTLOWAT = 1<<9,
	SO_S_SHUTRD   = 1<<10,
	SO_S_SHUTWR   = 1<<11,

	SO_S_END,
	SO_S_ALL = ((SO_S_END - 1) << 1) - 1
}; /* enum so_state */


struct socket {
	struct so_options opts;
	struct dns_addrinfo *res;

	int fd;

	mode_t mode;
	int flags;

	struct so_stat st;

	struct {
		_Bool rd;
		_Bool wr;
	} shut;

	struct addrinfo *host;

	short events;

	int done, todo;

	int olowat;

	struct {
		SSL *ctx;
		int error;
		int state;
		_Bool accept;
		_Bool vrfd;
	} ssl;

	struct {
		int ncalls;
		sigset_t pending;
		sigset_t blocked;
	} pipeign;
}; /* struct socket */


static _Bool so_needign(struct socket *so, _Bool rdonly) {
	if (!so->opts.fd_nosigpipe || (so->flags & SF_NOSIGPIPE))
		return 0;
	if (so->ssl.ctx)
		return 1;
	if (rdonly)
		return 0;
#if defined MSG_NOSIGNAL
	if (S_ISSOCK(so->mode))
		return 0;
#endif
	return 1;
} /* so_needign() */


static int so_pipeign(struct socket *so, _Bool rdonly) {
	if (!so_needign(so, rdonly))
		return 0;

#if _POSIX_REALTIME_SIGNALS > 0
	if (so->pipeign.ncalls++ > 0)
		return 0;

	sigemptyset(&so->pipeign.pending);
	sigpending(&so->pipeign.pending);

	if (sigismember(&so->pipeign.pending, SIGPIPE))
		return 0;

	sigset_t piped;
	sigemptyset(&piped);
	sigaddset(&piped, SIGPIPE);
	sigemptyset(&so->pipeign.blocked);

	return thr_sigmask(SIG_BLOCK, &piped, &so->pipeign.blocked);
#else
	return EOPNOTSUPP;
#endif
} /* so_pipeign() */


static int so_pipeok(struct socket *so, _Bool rdonly) {
	if (!so_needign(so, rdonly))
		return 0;

#if _POSIX_REALTIME_SIGNALS > 0
	assert(so->pipeign.ncalls > 0);

	if (--so->pipeign.ncalls)
		return 0;

	if (sigismember(&so->pipeign.pending, SIGPIPE))
		return 0;

	sigset_t piped;
	sigemptyset(&piped);
	sigaddset(&piped, SIGPIPE);

	while (-1 == sigtimedwait(&piped, NULL, &(struct timespec){ 0, 0 }) && errno == EINTR)
		;;

	return thr_sigmask(SIG_SETMASK, &so->pipeign.blocked, NULL);
#else
	return EOPNOTSUPP;
#endif
} /* so_pipeok() */


static int so_getaddr_(struct socket *so) {
	int error;

	if (!so->res)
		return EINVAL;

	so->events = 0;

	free(so->host);
	so->host = 0;

	if ((error = dns_ai_nextent(&so->host, so->res)))
		goto error;

	return 0;
error:
	switch (error) {
#if SO_EWOULDBLOCK != SO_EAGAIN
	case SO_EWOULDBLOCK:
		/* FALL THROUGH */
#endif
	case SO_EAGAIN:
		so->events = dns_ai_events(so->res);

		break;
	} /* switch() */

	return error;
} /* so_getaddr_() */


static int so_socket_(struct socket *so) {
	struct stat st;
	int error;

	if (!so->host)
		return EINVAL;

	so_closesocket(&so->fd);

	if (-1 == (so->fd = so_socket(so->host->ai_family, so->host->ai_socktype, &so->opts, &error)))
		return error;

	so->flags = so_getfl(so->fd, ~0);

	if (0 != fstat(so->fd, &st))
		return errno;

	so->mode = st.st_mode;

	return 0;
} /* so_socket_() */


static int so_bind_(struct socket *so) {
	struct sockaddr *saddr;

	if (so->todo & SO_S_LISTEN) {
		saddr = so->host->ai_addr;
	} else if (so->opts.sa_bind) {
		saddr = (struct sockaddr *)so->opts.sa_bind;
	} else
		return 0;

	return (0 == so_bind(so->fd, saddr, &so->opts))? 0 : so_soerr();
} /* so_bind_() */


static int so_listen_(struct socket *so) {
	/* FIXME: Do we support non-SOCK_STREAM sockets? */
	return (0 == listen(so->fd, SOMAXCONN))? 0 : so_soerr();
} /* so_listen_() */


static int so_connect_(struct socket *so) {
	int error;

	so->events &= ~POLLOUT;

retry:
	if (0 != connect(so->fd, so->host->ai_addr, so->host->ai_addrlen))
		goto soerr;

ready:
	so_trace(SO_T_CONNECT, so->fd, so->host, "ready");

	return 0;
soerr:
	switch ((error = so_soerr())) {
	case SO_EISCONN:
		goto ready;
	case SO_EINTR:
		goto retry;
	case SO_EINPROGRESS:
		/* FALL THROUGH */
	case SO_EALREADY:
		/* FALL THROUGH */
#if SO_EWOULDBLOCK != SO_EAGAIN
	case SO_EWOULDBLOCK
		/* FALL THROUGH */
#endif
		so->events |= POLLOUT;

		return SO_EAGAIN;
	default:
		so_trace(SO_T_CONNECT, so->fd, so->host, "%s", so_strerror(error));

		return error;
	} /* switch() */
} /* so_connect_() */


static int so_starttls_(struct socket *so) {
	X509 *peer;
	int rval, error;

	if (so->ssl.error)
		return so->ssl.error;

	so_pipeign(so, 0);

	ERR_clear_error();

	switch (so->ssl.state) {
	case 0:
		if (1 != SSL_set_fd(so->ssl.ctx, so->fd)) {
			error = SO_EOPENSSL;

			goto error;
		}

		if (so->ssl.accept)
			SSL_set_accept_state(so->ssl.ctx);
		else
			SSL_set_connect_state(so->ssl.ctx);

		so->ssl.state++;
	case 1:
		rval = SSL_do_handshake(so->ssl.ctx);

		if (rval > 0) {
			/* SUCCESS (continue to next state) */
			;;
		} else {
			/* ERROR (either need I/O or a plain error) or SHUTDOWN */
			so->events &= ~(POLLIN|POLLOUT);

			error = ssl_error(so->ssl.ctx, rval, &so->events);

			goto error;
		} /* (rval) */

		so->ssl.state++;
	case 2:
		/*
		 * NOTE: Must call SSL_get_peer_certificate() first, which
		 * processes the certificate. SSL_get_verify_result() merely
		 * returns the result of this processing.
		 */
		peer = SSL_get_peer_certificate(so->ssl.ctx);
		so->ssl.vrfd = (peer && SSL_get_verify_result(so->ssl.ctx) == X509_V_OK);
		x509_discard(&peer);

		so->ssl.state++;
	case 3:
		if (so->opts.tls_verify && !so->ssl.vrfd) {
			error = SO_ENOTVRFD;

			goto error;
		}

		so->ssl.state++;
	case 4:
		break;
	} /* switch(so->ssl.state) */

#if SOCKET_DEBUG
	if (SOCKET_DEBUG) {
		const SSL_CIPHER *cipher = SSL_get_current_cipher(so->ssl.ctx);

		so_trace(SO_T_STARTTLS, so->fd, so->host, so->ssl.ctx,
			"%s-%s", SSL_get_version(so->ssl.ctx), SSL_CIPHER_get_name(cipher));
	}
#endif

	so_pipeok(so, 0);

	return 0;
error:
	if (error != SO_EAGAIN)
		so_trace(SO_T_STARTTLS, so->fd, so->host, so->ssl.ctx, "%s", so_strerror(error));

	so_pipeok(so, 0);

	return error;
} /* so_starttls_() */


static int so_rstlowat_(struct socket *so) {
	if (0 != setsockopt(so->fd, SOL_SOCKET, SO_RCVLOWAT, &so->olowat, sizeof so->olowat))
		return so_soerr();

	return 0;
} /* so_rstlowat_() */


static _Bool so_isconn(int fd) {
		struct sockaddr sa;
		socklen_t slen = sizeof sa;

		return 0 == getpeername(fd, &sa, &slen) || so_soerr() != SO_ENOTCONN;
} /* so_isconn() */

static int so_shutrd_(struct socket *so) {
	if (so->fd != -1 && 0 != shutdown(so->fd, SHUT_RD)) {
		/*
		 * NOTE: OS X will fail with ENOTCONN if the requested
		 * SHUT_RD or SHUT_WR flag is already set, including if the
		 * SHUT_RD flag is set from the peer sending eof. Other OSs
		 * just treat this as a noop and return successfully.
		 */
		if (so_soerr() != SO_ENOTCONN)
			return so_soerr();
		else if (!so_isconn(so->fd))
			return SO_ENOTCONN;
	}

	so->shut.rd = 1;

	return 0;
} /* so_shutrd_() */


static int so_shutwr_(struct socket *so) {
	if (so->fd != -1 && 0 != shutdown(so->fd, SHUT_WR))
		return so_soerr();

	so->shut.wr = 1;
	so->st.sent.eof = 1;

	return 0;
} /* so_shutwr_() */


static inline int so_state(const struct socket *so) {
	if (so->todo & ~so->done) {
		int i = 1;

		while (i < SO_S_END && !(i & (so->todo & ~so->done)))
			i <<= 1;

		return (i < SO_S_END)? i : 0;
	} else
		return 0;
} /* so_state() */


static int so_exec(struct socket *so) {
	int state, error_, error = 0;

exec:

	switch (state = so_state(so)) {
	case SO_S_INIT:
		break;
	case SO_S_GETADDR:
		switch ((error_ = so_getaddr_(so))) {
		case 0:
			break;
		case ENOENT:
			/* NOTE: Return the last error if possible. */
			if (error)
				error_ = error;

			/* FALL THROUGH */
		default:
			error = error_;

			goto error;
		}

		so->done |= state;

		goto exec;
	case SO_S_SOCKET:
		if ((error = so_socket_(so))) {
			so->done = 0;

			goto exec;
		}

		so->done |= state;

		goto exec;
	case SO_S_BIND:
		if ((error = so_bind_(so))) {
			so->done = 0;

			goto exec;
		}

		so->done |= state;

		goto exec;
	case SO_S_LISTEN:
		if ((error = so_listen_(so)))
			return error;

		so->done |= state;

		goto exec;
	case SO_S_CONNECT:
		if ((error = so_connect_(so))) {
			switch (error) {
			case SO_EAGAIN:
				goto error;
			default:
				so->done = 0;

				goto exec;
			} /* switch() */
		}

		so->done |= state;

		goto exec;
	case SO_S_STARTTLS:
		if ((error = so_starttls_(so)))
			goto error;

		so->done |= state;

		goto exec;
	case SO_S_SETREAD:
		so->events |= POLLIN;
		so->done   |= state;

		goto exec;
	case SO_S_SETWRITE:
		so->events |= POLLOUT;
		so->done   |= state;

		goto exec;
	case SO_S_RSTLOWAT:
		if ((error = so_rstlowat_(so)))
			goto error;

		so->todo &= ~state;

		goto exec;
	case SO_S_SHUTRD:
		if ((error = so_shutrd_(so)))
			goto error;

		so->done |= state;

		goto exec;
	case SO_S_SHUTWR:
		if ((error = so_shutwr_(so)))
			goto error;

		so->done |= state;

		goto exec;
	} /* so_exec() */

	return 0;
error:
	return error;
} /* so_exec() */


static struct socket *so_init(struct socket *so, const struct so_options *opts) {
	static const struct socket so_initializer = { .fd = -1 };

	if (!so)
		return 0;

	*so = so_initializer;
	so->opts = *opts;

	return so;
} /* so_init() */


static int so_destroy(struct socket *so) {
	ssl_discard(&so->ssl.ctx);

	dns_ai_close(so->res);
	so->res = 0;

	free(so->host);
	so->host = 0;

	so_closesocket(&so->fd);

	so->events = 0;

	return 0;
} /* so_destroy() */


struct socket *(so_open)(const char *host, const char *port, int qtype, int domain, int type, const struct so_options *opts, int *error_) {
	struct addrinfo hints;
	struct socket *so;
	int error;

	if (!(so = so_init(malloc(sizeof *so), opts)))
		goto syerr;

	hints.ai_flags    = AI_CANONNAME;
	hints.ai_family   = domain;
	hints.ai_socktype = type;

	if (!(so->res = dns_ai_open(host, port, qtype, &hints, dns_res_mortal(dns_res_stub(dns_opts(), &error)), &error)))
		goto error;

	so->todo = SO_S_GETADDR | SO_S_SOCKET | SO_S_BIND;

	return so;
syerr:
	error = so_syerr();
error:
	so_close(so);

	*error_ = error;

	return 0;
} /* so_open() */


struct socket *so_fdopen(int fd, const struct so_options *opts, int *error_) {
	struct socket *so;
	struct stat st;
	int error;

	if (!(so = so_init(malloc(sizeof *so), opts)))
		goto syerr;

	if ((error = so_rstfl(fd, &so->flags, so_opts2flags(opts), ~0, ~(SF_NODELAY|SF_NOPUSH|SF_NOSIGPIPE))))
		goto error;

	if (0 != fstat(fd, &st))
		goto syerr;

	so->mode = st.st_mode;

	so->fd = fd;

	return so;
syerr:
	error = errno;
error:
	so_close(so);

	*error_ = error;

	return 0;
} /* so_fdopen() */


int so_close(struct socket *so) {
	if (!so)
		return EINVAL;

	so_destroy(so);

	free(so);

	return 0;
} /* so_close() */


int so_family(struct socket *so, int *error_) {
	struct sockaddr_storage saddr;
	socklen_t slen = sizeof saddr;
	int error;

	if ((error = so_localaddr(so, (void *)&saddr, &slen)))
		{ *error_ = error; return AF_UNSPEC; }

	return *sa_family(&saddr);
} /* so_family() */


int so_localaddr(struct socket *so, void *saddr, socklen_t *slen) {
	int error;

	if ((error = so_exec(so)))
		return error;

	if (0 != getsockname(so->fd, saddr, slen))
		return errno;

	return 0;
} /* so_localaddr() */


int so_remoteaddr(struct socket *so, void *saddr, socklen_t *slen) {
	int error;

	if ((error = so_exec(so)))
		return error;

	if (0 != getpeername(so->fd, saddr, slen))
		return errno;

	return 0;
} /* so_remoteaddr() */


int so_connect(struct socket *so) {
	if (so->done & SO_S_CONNECT)
		return 0;

	so->todo |= SO_S_CONNECT;

	return so_exec(so);
} /* so_connect() */


int so_listen(struct socket *so) {
	if (so->done & SO_S_LISTEN)
		return 0;

	so->todo |= SO_S_LISTEN;

	return so_exec(so);
} /* so_listen() */


int so_accept(struct socket *so, struct sockaddr *saddr, socklen_t *slen, int *error_) {
	int fd, error;

	if ((error = so_listen(so)))
		goto error;

	if ((error = so_exec(so)))
		goto error;

	so->events = POLLIN;

	if (-1 == (fd = accept(so->fd, saddr, slen)))
		goto soerr;

	return fd;
soerr:
	error = so_soerr();

#if SO_EWOULDBLOCK != SO_EAGAIN
	if (error == SO_EWOULDBLOCK)
		error = SO_EAGAIN;
#endif
error:

	*error_ = error;

	return -1;
} /* so_accept() */


int so_starttls(struct socket *so, SSL_CTX *ctx) {
	SSL_CTX *tmp = 0;
	const SSL_METHOD *method;

	if (so->done & SO_S_STARTTLS)
		return 0;

	if (so->todo & SO_S_STARTTLS)
		goto check;

	/*
	 * reset SSL state
	 */
	ssl_discard(&so->ssl.ctx);
	so->ssl.state  = 0;
	so->ssl.error  = 0;
	so->ssl.accept = 0;
	so->ssl.vrfd   = 0;

	/*
	 * NOTE: Store any error in so->ssl.error because callers expect to
	 * call-and-forget this routine, similar to so_connect() and
	 * so_listen(). Likewise, commit to the SO_S_STARTTLS state at this
	 * point, no matter whether we can allocate the proper objects, so
	 * any errors will persist--so_starttls_() immediately returns if
	 * so->ssl.error is set.
	 */
	so->todo |= SO_S_STARTTLS;

	ERR_clear_error();

	if (!ctx && !(ctx = tmp = SSL_CTX_new(SSLv23_method())))
		goto error;

	if (!(so->ssl.ctx = SSL_new(ctx)))
		goto error;

	/*
	 * NOTE: SSLv3_server_method()->ssl_connect should be a reference to
	 * OpenSSL's internal ssl_undefined_function().
	 *
	 * Server methods such as SSLv23_server_method(), etc. should have
	 * their .ssl_connect method set to this value.
	 */
	method = SSL_get_ssl_method(so->ssl.ctx);

	if (!method->ssl_connect || method->ssl_connect == SSLv3_server_method()->ssl_connect)
		so->ssl.accept = 1;

	if (tmp)
		SSL_CTX_free(tmp);

check:
	return so_exec(so);
error:
	so->ssl.error = SO_EOPENSSL;

	if (tmp)
		SSL_CTX_free(tmp);

	return so->ssl.error;
} /* so_starttls() */


SSL *so_checktls(struct socket *so) {
	return so->ssl.ctx;
} /* so_checktls() */


int so_shutdown(struct socket *so, int how) {
	switch (how) {
	case SHUT_RD:
		so->todo |= SO_S_SHUTRD;

		break;
	case SHUT_WR:
		so->todo |= SO_S_SHUTWR;

		break;
	case SHUT_RDWR:
		so->todo |= SO_S_SHUTRD|SO_S_SHUTWR;

		break;
	} /* switch (how) */

	return so_exec(so);
} /* so_shutdown() */


size_t so_read(struct socket *so, void *dst, size_t lim, int *error_) {
	long len;
	int error;

	so_pipeign(so, 1);

	so->todo |= SO_S_SETREAD;

	if ((error = so_exec(so)))
		goto error;

	if (so->fd == -1) {
		error = ENOTCONN;

		goto error;
	}

	so->events &= ~POLLIN;

retry:
	if (so->ssl.ctx) {
		ERR_clear_error();

		len = SSL_read(so->ssl.ctx, dst, SO_MIN(lim, INT_MAX));

		if (len < 0) {
			goto sslerr;
		} else if (len == 0) {
			*error_ = EPIPE; /* FIXME: differentiate clean from unclean shutdown? */
			so->st.rcvd.eof = 1;
		}
	} else {
#if _WIN32
		len = recv(so->fd, dst, SO_MIN(lim, LONG_MAX), 0);
#else
		len = read(so->fd, dst, SO_MIN(lim, LONG_MAX));
#endif

		if (len == -1) {
			goto soerr;
		} else if (len == 0) {
			*error_ = EPIPE;
			so->st.rcvd.eof = 1;
		}
	}

	so_trace(SO_T_READ, so->fd, so->host, dst, (size_t)len, "rcvd %zu bytes", (size_t)len);
	st_update(&so->st.rcvd, len, &so->opts);

	so_pipeok(so, 1);

	return len;
sslerr:
	error = ssl_error(so->ssl.ctx, (int)len, &so->events);

	if (error == SO_EINTR)
		goto retry;

	goto error;
soerr:
	error = so_soerr();

	switch (error) {
	case SO_EINTR:
		goto retry;
#if SO_EWOULDBLOCK != SO_EAGAIN
	case SO_EWOULDBLOCK:
		/* FALL THROUGH */
#endif
	case SO_EAGAIN:
		so->events |= POLLIN;

		break;
	} /* switch() */
error:
	*error_ = error;

	if (error != SO_EAGAIN)
		so_trace(SO_T_READ, so->fd, so->host, (void *)0, (size_t)0, "%s", so_strerror(error));

	so_pipeok(so, 1);

	return 0;
} /* so_read() */


size_t so_write(struct socket *so, const void *src, size_t len, int *error_) {
	long count;
	int error;

	so_pipeign(so, 0);

	so->todo |= SO_S_SETWRITE;

	if ((error = so_exec(so)))
		goto error;

	if (so->fd == -1) {
		error = ENOTCONN;

		goto error;
	}

	so->events &= ~POLLOUT;

retry:
	if (so->ssl.ctx) {
		if (len > 0) {
			ERR_clear_error();

			count = SSL_write(so->ssl.ctx, src, SO_MIN(len, INT_MAX));

			if (count < 0)
				goto sslerr;
			else if (count == 0)
				*error_ = EPIPE; /* FIXME: differentiate clean from unclean shutdown? */
		} else
			count = 0;
	} else {
#if _WIN32
		count = send(so->fd, src, SO_MIN(len, LONG_MAX), 0);
#else
#if defined(MSG_NOSIGNAL)
		if (S_ISSOCK(so->fd))
			count = send(so->fd, src, SO_MIN(len, LONG_MAX), MSG_NOSIGNAL);
		else
			count = write(so->fd, src, SO_MIN(len, LONG_MAX));
#else
		count = write(so->fd, src, SO_MIN(len, LONG_MAX));
#endif
#endif

		if (count == -1)
			goto soerr;
	}

	so_trace(SO_T_WRITE, so->fd, so->host, src, (size_t)count, "sent %zu bytes", (size_t)len);
	st_update(&so->st.sent, len, &so->opts);

	so_pipeok(so, 0);

	return count;
sslerr:
	error = ssl_error(so->ssl.ctx, (int)count, &so->events);

	if (error == SO_EINTR)
		goto retry;

	goto error;
soerr:
	error = so_soerr();

	switch (error) {
	case SO_EINTR:
		goto retry;
#if SO_EWOULDBLOCK != SO_EAGAIN
	case SO_EWOULDBLOCK:
		/* FALL THROUGH */
#endif
	case SO_EAGAIN:
		so->events |= POLLOUT;

		break;
	} /* switch() */
error:
	*error_ = error;

	if (error != SO_EAGAIN)
		so_trace(SO_T_WRITE, so->fd, so->host, (void *)0, (size_t)0, "%s", so_strerror(error));

	so_pipeok(so, 0);

	return 0;
} /* so_write() */


size_t so_peek(struct socket *so, void *dst, size_t lim, int flags, int *_error) {
	int rstlowat = so->todo & SO_S_RSTLOWAT;
	long count;
	int lowat, error;

	so->todo &= ~SO_S_RSTLOWAT;

	error = so_exec(so);

	so->todo |= rstlowat;

	if (error)
		goto error;

	if (flags & SO_F_PEEKALL)
		so->events &= ~POLLIN;

retry:
	count = recv(so->fd, dst, lim, MSG_PEEK);

	if (count == -1)
		goto soerr;

	if ((size_t)count == lim || !(flags & SO_F_PEEKALL))
		return count;
pollin:
	if (!(so->todo & SO_S_RSTLOWAT)) {
		if (0 != getsockopt(so->fd, SOL_SOCKET, SO_RCVLOWAT, &so->olowat, &(socklen_t){ sizeof so->olowat }))
			goto soerr;

		if (lim > INT_MAX) {
			error = EOVERFLOW;

			goto error;
		}

		lowat = (int)lim;

		if (0 != setsockopt(so->fd, SOL_SOCKET, SO_RCVLOWAT, &lowat, sizeof lowat))
			goto soerr;

		so->todo |= SO_S_RSTLOWAT;
	}

	so->events |= POLLIN;

	*_error = SO_EAGAIN;

	return 0;
soerr:
	error = so_soerr();

	switch (error) {
	case SO_EINTR:
		goto retry;
#if SO_EWOULDBLOCK != SO_EAGAIN
	case SO_EWOULDBLOCK:
		/* FALL THROUGH */
#endif
	case SO_EAGAIN:
		if (flags & SO_F_PEEKALL)
			goto pollin;

		break;
	} /* switch() */
error:
	*_error = error;

	return 0;
} /* so_peek() */


const struct so_stat *so_stat(struct socket *so) {
	return &so->st;
} /* so_stat() */


void so_clear(struct socket *so) {
	so->todo   &= ~(SO_S_SETREAD|SO_S_SETWRITE);
	so->events = 0;
} /* so_clear() */


int so_events(struct socket *so) {
	short events;

	switch (so->opts.fd_events) {
	case SO_LIBEVENT:
		events = SO_POLL2EV(so->events);

		break;
	default:
		/* FALL THROUGH */
	case SO_SYSPOLL:
		events = so->events;

		break;
	} /* switch (.fd_events) */

	return events;
} /* so_events() */


int so_pollfd(struct socket *so) {
	switch (so_state(so)) {
	case SO_S_GETADDR:
		return dns_ai_pollfd(so->res);
	default:
		return so->fd;
	} /* switch() */
} /* so_pollfd() */


int so_poll(struct socket *so, int timeout) {
	int nfds;

#if 1
	struct pollfd pfd = { .fd = so_pollfd(so), .events = so->events, };

	if (!pfd.events)
		return 0;

	if (timeout != -1)
		timeout *= 1000;

	nfds = poll(&pfd, 1, timeout);
#else
	fd_set rfds, wfds;
	int set, fd;

	FD_ZERO(&rfds);
	FD_ZERO(&wfds);

	if (!(set = so->events))
		return 0;

	fd = so_pollfd(so);

	if ((set & POLLIN) && fd < FD_SETSIZE)
		FD_SET(fd, &rfds);

	if ((set & POLLOUT) && fd < FD_SETSIZE)
		FD_SET(fd, &wfds);

	nfds = select(fd + 1, &rfds, &wfds, 0, (timeout >= 0)? &(struct timeval){ timeout, 0 } : 0);
#endif

	switch (nfds) {
	case -1:
		return errno;
	case 0:
		return ETIMEDOUT;
	default:
		return 0;
	}
} /* so_poll() */


int so_peerfd(struct socket *so) {
	return so->fd;
} /* so_peerfd() */


int so_uncork(struct socket *so) {
	return so_nopush(so->fd, 0);
} /* so_uncork() */


/*
 * L I B R A R Y  R O U T I N E S
 *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void socket_init(void) __attribute__((constructor, used));

void socket_init(void) {
	SSL_load_error_strings();
	SSL_library_init();

	so_initdebug();
} /* socket_init() */



#if SOCKET_MAIN

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <string.h>

#include <unistd.h>

#include <err.h>

#include <regex.h>

#include "fifo.h"


struct {
	char arg0[64];

	struct {
		char scheme[32];
		char authority[128];
		char host[128];
		char port[32];
		char path[128];
		char query[64];
		char fragment[32];
	} url;
} MAIN = {
	.url = {
		.scheme = "http",
		.host   = "google.com",
		.port   = "80",
		.path   = "/",
	},
};


static void panic(const char *fmt, ...) {
	va_list ap;

	va_start(ap, fmt);

#if _WIN32
	vfprintf(stderr, fmt, ap);

	exit(EXIT_FAILURE);
#else
	verrx(EXIT_FAILURE, fmt, ap);
#endif
} /* panic() */

#define panic_(fn, ln, fmt, ...)	\
	panic(fmt "%0s", (fn), (ln), __VA_ARGS__)
#define panic(...)			\
	panic_(__func__, __LINE__, "(%s:%d) " __VA_ARGS__, "")


void parseurl(const char *url) {
	static const char *expr = "^(([^:/?#]+):)?(//(([^/@]*@)?([^/?#:]*)(:([[:digit:]]*))?))?([^?#]*)(\\?([^#]*))?(#(.*))?";
	regex_t re;
	regmatch_t match[16];
	char errstr[128];
	int error, i;
	struct { const char *name; char *dst; size_t lim; } part[16] = {
		[2]  = { "scheme:   ", MAIN.url.scheme,    sizeof MAIN.url.scheme },
		[4]  = { "authority:", MAIN.url.authority, sizeof MAIN.url.authority },
		[6]  = { "host:     ", MAIN.url.host,      sizeof MAIN.url.host },
		[8]  = { "port:     ", MAIN.url.port,      sizeof MAIN.url.port },
		[9]  = { "path:     ", MAIN.url.path,      sizeof MAIN.url.path },
		[11] = { "query:    ", MAIN.url.query,     sizeof MAIN.url.query },
		[13] = { "fragment: ", MAIN.url.fragment,  sizeof MAIN.url.fragment },
	};

	if ((error = regcomp(&re, expr, REG_EXTENDED)))
		goto error;

	if ((error = regexec(&re, url, lengthof(match), match, 0)))
		goto error;

	for (i = 0; i < lengthof(match); i++) {
		if (match[i].rm_so == -1)
			continue;

		if (part[i].dst) {
			snprintf(part[i].dst, part[i].lim, "%.*s", (int)(match[i].rm_eo - match[i].rm_so), &url[match[i].rm_so]);
//			SAY("%s %s", part[i].name, part[i].dst);
		} else {
//			SAY("[%d]:       %.*s", i, (int)(match[i].rm_eo - match[i].rm_so), &url[match[i].rm_so]);
			;;
		}
	}

	regfree(&re);

	return;
error:
	regerror(error, &re, errstr, sizeof errstr);
	regfree(&re);

	panic("%s", errstr);
} /* parseurl() */


int httpget(const char *url) {
	const struct so_stat *st;
	struct socket *so;
	struct fifo *req;
	struct iovec iov;
	long n;
	int lc, error;

	parseurl(url);

	if (!(so = so_open(MAIN.url.host, MAIN.url.port, DNS_T_A, PF_INET, SOCK_STREAM, so_opts(), &error)))
		errx(EXIT_FAILURE, "so_open: %s", so_strerror(error));

	so_connect(so);

	if (!strcasecmp("https", MAIN.url.scheme)) {
		SSL_CTX *ctx = SSL_CTX_new(SSLv23_method());

#if 0 /* example code if waiting for SSL negotiation */
		while ((error = so_starttls(so, ctx))) {
			if (error != SO_EAGAIN || (error = so_poll(so, 3)))
				errx(EXIT_FAILURE, "so_starttls: %s", so_strerror(error));
		}
#else
		so_starttls(so, ctx);
#endif
	}

	req = fifo_new(1024);

	fifo_puts(req, "GET ");
	fifo_puts(req, MAIN.url.path);
	fifo_puts(req, " HTTP/1.0\r\n");
	fifo_puts(req, "Host: ");
	fifo_puts(req, MAIN.url.host);
	fifo_putc(req, ':');
	fifo_puts(req, MAIN.url.port);
	fifo_puts(req, "\r\n\r\n");

	while (fifo_rvec(req, &iov)) {
		if (!(n = so_write(so, iov.iov_base, iov.iov_len, &error))) {
			switch (error) {
			case SO_EAGAIN:
				so_poll(so, 1);

				break;
			default:
				errx(EXIT_FAILURE, "so_write: %s", so_strerror(error));
			}
		} else {
			fifo_discard(req, n);
		}
	}

//	so_shutdown(so, SHUT_WR); /* send EOF (but some servers don't like this) */

	lc = 0;

	do {
		char res[512];

		while (0 == (n = so_read(so, res, sizeof res, &error)) && error != EPIPE) {
			switch (error) {
			case SO_EAGAIN:
				so_poll(so, 1);

				continue;
			default:
				errx(EXIT_FAILURE, "so_read: %s", so_strerror(error));
			}
		}

		if (n > 0) {
			fwrite(res, 1, n, stdout);
			lc = res[n-1];
		}
	} while (n);

	if (isatty(STDOUT_FILENO) && isatty(STDERR_FILENO)) {
		if (lc != '\n')
			fputc('\n', stdout);

		fflush(stdout);

		fputs("--\n", stderr);
	}

	st = so_stat(so);

	fprintf(stderr, "sent: %llu bytes\n", st->sent.count);
	fprintf(stderr, "rcvd: %llu bytes\n", st->rcvd.count);

	so_close(so);

	return 0;
} /* httpget() */


int echo(void) {
	struct socket *srv0, *srv, *cli;
	struct fifo out, in;
	char obuf[512], ibuf[512];
	long olen, len;
	struct iovec iov;
	int fd, error;

	if (!(srv0 = so_open("127.0.0.1", "54321", DNS_T_A, PF_INET, SOCK_STREAM, so_opts(), &error)))
		panic("so_open: %s", so_strerror(error));

	if (!(cli = so_open("127.0.0.1", "54321", DNS_T_A, PF_UNSPEC, SOCK_STREAM, so_opts(), &error)))
		panic("so_open: %s", so_strerror(error));

	so_listen(srv0);

	while (-1 == (fd = so_accept(srv0, 0, 0, &error))) {
		if (error != SO_EAGAIN)
			panic("so_accept: %s", so_strerror(error));

		if ((error = so_connect(cli)) && error != SO_EAGAIN)
			panic("so_connect: %s", so_strerror(error));

		so_poll(cli, 1);
	}

	if (!(srv = so_fdopen(fd, so_opts(), &error)))
		panic("so_fdopen: %s", so_strerror(error));

	while ((olen = fread(obuf, 1, sizeof obuf, stdin))) {
		fifo_from(&out, obuf, olen);
		fifo_init(&in, ibuf, sizeof ibuf);

		while (fifo_rlen(&in) < olen) {
			if (fifo_rvec(&out, &iov)) {
				so_poll(cli, 1);

				if (!(len = so_write(cli, iov.iov_base, iov.iov_len, &error)) && error != SO_EAGAIN)
					panic("so_write: %s", so_strerror(error));
				else
					fifo_discard(&out, len);
			}

			so_poll(srv, 1);

			fifo_wvec(&in, &iov);

			if (!(len = so_read(srv, iov.iov_base, iov.iov_len, &error)) && error != SO_EAGAIN && error != EPIPE)
				panic("so_read: %s", so_strerror(error));
			else
				fifo_update(&in, +len);
		}

		while (fifo_rvec(&in, &iov))
			fifo_discard(&in, fwrite(iov.iov_base, 1, iov.iov_len, stdout));
	}

	so_close(srv0);
	so_close(srv);
	so_close(cli);

	return 0;
} /* echo() */


#define USAGE \
	"%s [-h] echo | egress ADDR [PORT] | print ADDR [PORT] | get URI\n" \
	"  -v  be verbose--trace input/output\n" \
	"  -V  print version information\n" \
	"  -h  print usage information\n" \
	"\n" \
	"Report bugs to william@25thandClement.com\n"

int main(int argc, char **argv) {
	extern int optind;
	int opt;

	dns_strlcpy(MAIN.arg0, (strrchr(argv[0], '/')? strrchr(argv[0], '/') + 1 : argv[0]), sizeof MAIN.arg0);

	while (-1 != (opt = getopt(argc, argv, "vVh"))) {
		switch (opt) {
		case 'v':
#if !defined(so_trace) /* macro expanding to void statement if no debug support */
			socket_debug++;
#else
			fprintf(stderr, "%s: not compiled with tracing support\n", MAIN.arg0);
#endif

			break;
		case 'V':
			printf("%s (socket.c) %.8X\n", MAIN.arg0, socket_v_rel());
			printf("vendor  %s\n", socket_vendor());
			printf("release %.8X\n", socket_v_rel());
			printf("abi     %.8X\n", socket_v_abi());
			printf("api     %.8X\n", socket_v_api());
			printf("dns     %.8X\n", dns_v_rel());
			printf("ssl     %s\n", OPENSSL_VERSION_TEXT);

			return 0;
		case 'h':
			/* FALL THROUGH */
usage:		default:
			fprintf(stderr, USAGE, MAIN.arg0);

			return (opt == 'h')? 0: EXIT_FAILURE;
		} /* switch() */
	} /* while () */

	argc -= optind;
	argv += optind;

	socket_init();

	if (!argc) {
		goto usage;
	} else if (!strcmp(*argv, "echo")) {
		return echo();
	} else if (!strcmp(*argv, "egress") && argv[1]) {
		struct sockaddr *saddr;
		struct sockaddr_storage egress;
		int error;

		if (AF_UNSPEC == *sa_family(saddr = sa_aton(argv[1]))) {
			struct dns_resolver *res;
			struct dns_packet *ans;
			struct dns_rr rr;
			union dns_any rd;
			char addr[SA_ADDRSTRLEN];

			if (!(res = dns_res_stub(dns_opts(), &error)))
				panic("dns_res_stub: %s", so_strerror(error));

			if (!(ans = dns_res_query(res, argv[1], DNS_T_A, DNS_C_IN, 1, &error)))
				panic("dns_res_query: %s", so_strerror(error));

			dns_res_close(res);

			if (!dns_rr_grep(&rr, 1, dns_rr_i_new(ans, .section = DNS_S_AN, .type = DNS_T_A), ans, &error))
				panic("%s: no A record", argv[1]);

			if ((error = dns_any_parse(&rd, &rr, ans)))
				panic("%s: %s", argv[1], so_strerror(error));

			dns_any_print(addr, sizeof addr, &rd, rr.type);

			free(ans);

			saddr = sa_aton(addr);

			if (AF_UNSPEC == sa_family(saddr))
				goto usage;
		}

		if (argc > 2) {
			*sa_port(saddr) = htons(atoi(argv[2]));

			printf("[%s]:%hu => %s\n", sa_ntoa(saddr), ntohs(*sa_port(saddr)), sa_ntoa(sa_egress(&egress, sizeof egress, saddr, 0)));
		} else
			printf("%s => %s\n", sa_ntoa(saddr), sa_ntoa(sa_egress(&egress, sizeof egress, saddr, 0)));
	} else if (!strcmp(*argv, "print") && argv[1]) {
		struct sockaddr *saddr = sa_aton(argv[1]);

		if (AF_UNSPEC == sa_family(saddr))
			goto usage;

		if (argc > 2) {
			*sa_port(saddr) = htons(atoi(argv[2]));

			printf("[%s]:%hu\n", sa_ntoa(saddr), ntohs(*sa_port(saddr)));
		} else {
			*sa_port(saddr) = htons(6970);

			printf("%s\n", sa_ntoa(saddr));
		}
	} else if (!strcmp(*argv, "get") && argv[1]) {
		return httpget(argv[1]);
	} else
		goto usage;

	return 0;
} /* main() */

#endif
