## Home Page

This project's home page and main repository is located at
http://25thandClement.com/~william/projects/dns.c.html.

But feel free to rely on Github for tracking the source tree.

## Description

A non-blocking DNS resolver library in a single .c file.

* No dependencies!

* Supports _both_ stub and recursive modes.

* No event callbacks! Callbacks are usually inevitable when writing
  non-blocking network software, but when too many libraries introduce too
  many callback interfaces code quickly becomes incomprehensible beyond
  necessity. dns.c requires no particular callback scheme, nor enforces
  callbacks at all. That makes it easy to use and embed within other
  components.

* Works with any event loop. All resolver objects support three common
  methods: pollfd, events, and timeout.

* Core DNS API built around actual DNS packet; as generic as DNS itself.
  This makes querying and manipulating records other than A, AAAA, and PTR
  much easier, yet with similar simplicity as API's which make annoying
  assumptions.

* Type-specific interfaces for A, AAAA, CNAME, NS, SOA, PTR, MX, TXT, SRV,
  SSHFP, and SPF records.

* Restartable record iterators with user-specified sorting. Iterate over MX
  or SRV records in semantic order (i.e. preference and priority) using a
  single dns_rr_foreach loop. Interruptible loops supported with
  dns_rr_grep.

* Thoughtful /etc/resolv.conf, /etc/nsswitch.conf, and /etc/hosts
  integration. Easy to change system defaults, or to skip entirely.

* getaddrinfo-like auxiliary interface.

* Pluggable cache interface. Application can specify a synchronous or
  asynchronous local cache for use by the core resolver.

* "Smart" queries which automatically dereference NS, MX, SRV, PTR, etc. to
  A or AAAA records. Recursing, caching nameservers don't usually do this
  explicitly, but merely rely on the authoritative server to include glue,
  which won't exist for out-of-bailiwick references (very common these
  days). This means software must do two separate logical DNS operations; a
  headache when patching software which only supported A lookups. Smart
  queries are available with a flag to the core resolver-—in both stub and
  recursive modes—-and more efficiently with the built-in getaddrinfo-like
  auxiliary interface.

* Randomized source ports and encrypted QIDs using a 16-bit Feistel block
  cipher. User specifiable entropy source. Defaults to arc4random where
  available; knows how to use OpenSSL RAND_bytes if specified during the
  build (-DDNS_RANDOM=RAND_bytes).

* Statistics interface. Retrieve count of packets and bytes, sent and
  received; and number of queries processed.

* Used successfully by many projects for many years, including several
  Silicon Valley giants.

* Regularly testing on Linux, OS X, OpenBSD, FreeBSD, NetBSD, and Solaris.
  Occassionally tested in MinGW environment. Builds with GCC, Clang, and
  SunPro.

### spf.c

Asynchronous SPF resolver--no threading, no forking, no callbacks, no
library dependencies.

* A single source file. Requires Ragel precompiler, but no run-time
  dependencies other than dns.c. The Ragel translation can be done once
  and the result stored if you don't plan on hacking spf.c.
* Passes over 90% of OpenSPF test suite. 100% test suite compliance is not
  the goal, as some tests are fairly debatable. (The SPF specification has some bugs.)
* Used successfully in countless MTA installations and for billions (trillions?)
  of queries.

## Build

dns.c is intended to be dropped into existing project builds. The included
Makefile is mostly for development and testing.

## Usage

Until the API is properly documented you must rely on the source code. The
header, dns.h, and the API is fairly straight-forward, with each object
implementating a simple, consistent, and hopefully self-explanatory pattern.

The last 1/6 of dns.c implements a command-line utility and a full regression
testing suite permitting each component to be tested individually. This is
excellent usage documentation as well.

## License

Copyright (c) 2008-2015  William Ahern <william@25thandClement.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
