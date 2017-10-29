#!/bin/sh
# ==========================================================================
# spf.t - "spf.c", a Sender Policy Framework library.
# --------------------------------------------------------------------------
# Copyright (c) 2009  William Ahern
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the
# following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
# NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
# USE OR OTHER DEALINGS IN THE SOFTWARE.
# ==========================================================================
#
SPF=./spf
VERBOSE=
ERRBUF=/dev/null


rand() {
	dd if=/dev/urandom bs=1 count=4 2>/dev/null | od -t u4 | awk 'NR=1{print $2}'
} # rand


say() {
	printf "$@"
} # say


check() {
	if [ $1 -eq 0 ]; then
		say "OK "
		shift 1
		say "$@"
	else
		[ -f $ERRBUF ] && cat $ERRBUF >&2

		say "FAIL "
		shift 1
		say "$@"

		exit 1
	fi
} # check


parse() {
	$SPF $VERBOSE parse "$1" 2>|$ERRBUF

	check $? "parse \`%s'\n" "$1"
} # parse


expand() {
	MACRO=$1
	EXPECT=$2

	shift 2

	EXPANS=$($SPF $VERBOSE "$@" expand $MACRO 2>$ERRBUF | sed -e 's/^\[//' -e 's/\]$//')

	[ "$EXPANS" == "$EXPECT" ]

	check $? "expand \`%s' \`%s'\n" "$MACRO" "$EXPANS"
} # expand


ip() {
	VERSION=$1
	IP=$2
	EXPECT=$3

	shift 3

	EXPANS=$($SPF $VERBOSE $VERSION $IP "$@" 2>$ERRBUF)

	[ "$EXPANS" == "$EXPECT" ]

	check $? "$VERSION \`%s' \`%s' %s\n" "$IP" "$EXPANS" "$*"
} # ip


ip6() {
	ip "ip6" "$@"
} # ip6


ip4() {
	ip "ip4" "$@"
} # ip4


fixdn() {
	DN=$1
	EXPECT=$2

	shift 2

	EXPANS=$($SPF $VERBOSE fixdn $DN "$@" 2>$ERRBUF)

	[ "$EXPANS" == "$EXPECT" ]

	check $? "fixdn \`%s' \`%s' %s\n" "$DN" "$EXPANS" "$*"
} # fixdn


usage() {
	cat <<-EOF
		spf.t -p:vh
		  -p PATH  Path to spf utility
		  -v       Be verbose
		  -h       Print usage

		Report bugs to william@25thandClement.com
	EOF
} # usage

while getopts p:vh OPT; do
	case $OPT in
	p)
		SPF="$OPTARG"
		;;
	v)
		VERBOSE='-v'
		;;
	h)
		usage >&2
		exit 0;
		;;
	?)
		usage >&2
		exit 1;
		;;
	esac
done

shift $(($OPTIND - 1))


#
# Setup secure error buffer
#
TMPDIR=${TMPDIR:-/tmp}
TMPDIR=${TMPDIR%/}

ERRBUF="${TMPDIR}/.spf.t.$(rand)"

if [ "${ERRBUF}" == "${TMPDIR}/.spf.t." ]; then
	printf "$0: unable to divert stderr\n"

	if [ -a /dev/stderr ]; then
		ERRBUF=/dev/stderr
	else
		ERRBUF=/dev/null
	fi
else
	trap "rm ${ERRBUF}" 0
fi


#
# RFC 4408 16.1 B.1. Simple Examples
#
parse 'v=spf1 +all'
parse 'v=spf1 a -all'
parse 'v=spf1 a:example.org -all'
parse 'v=spf1 mx -all'
parse 'v=spf1 mx:example.org -all'
parse 'v=spf1 mx mx:example.org -all'
parse 'v=spf1 mx/30 mx:example.org/30 -all'
parse 'v=spf1 ptr -all'
parse 'v=spf1 ip4:192.0.2.128/28 -all'


#
# RFC 4408 8.2 Expansion Examples
#
expand '%{s}' 'strong-bad@email.example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{o}' 'email.example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d}' 'email.example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d4}' 'email.example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d3}' 'email.example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d2}' 'example.com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d1}' 'com' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{dr}' 'com.example.email' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{d2r}' 'example.email' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{l}' 'strong-bad' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{l-}' 'strong.bad' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{lr}' 'strong-bad' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'
expand '%{lr-}' 'bad.strong' -S 'strong-bad@email.example.com' -L 'strong-bad' -O 'email.example.com' -D 'email.example.com'


#
# RFC 4291 Sec. 2.2. Test Representation of Addresses
#
ip6 'ABCD:EF01:2345:6789:ABCD:EF01:2345:6789' 'abcd:ef01:2345:6789:abcd:ef01:2345:6789'
ip6 '2001:DB8:0:0:8:800:200C:417A' '2001:db8::8:800:200c:417a'
ip6 'FF01:0:0:0:0:0:0:101' 'ff01::101'
ip6 '0:0:0:0:0:0:0:1' '::1'
ip6 '0:0:0:0:0:0:0:0' '::'
ip6 '0:0:0:0:0:0:13.1.68.3' '::d01:4403'
ip6 '0:0:0:0:0:0:13.1.68.3' '::13.1.68.3' compat
ip6 '0:0:0:0:0:0:13.1.68.3' '::13.1.68.3' mixed
ip6 '0:0:0:0:0:FFFF:129.144.52.38' '::ffff:8190:3426'
ip6 '0:0:0:0:0:FFFF:129.144.52.38' '::ffff:8190:3426' compat
ip6 '0:0:0:0:0:FFFF:129.144.52.38' '::ffff:129.144.52.38' mapped


#
# SPF IPv6 corner cases
#
ip6 'abcd:ef01:2345:6789:abcd:ef01:2345:6789' 'abcd:ef01:2345:6789:abcd:ef01:2345:6789'
ip6 '2z01:0zB8::zD30:123;:4567z:89AB~z:CDEF' '2::123:4567:89ab:cdef'
ip6 '::' '::'
ip6 '::1' '::1'
ip6 'ffff::' 'ffff::'


#
# SPF IPv6 nybble format
#
ip6 '0:0:0:0:0:0:13.1.68.3' '0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.0.1.4.4.0.3' nybble
ip6 '0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.0.1.4.4.0.3' '::d01:4403'
ip6 '0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.0.1.4.4.0.3' '::13.1.68.3' mixed


#
# Some IPv6 overflow scenarios
#
ip6 '0:0:0:0:0:0:0:127.0.0.1' '::7f00'
ip6 'f.f.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.f.f' 'ff00::'


#
# Lip-service IPv4 tests
#
ip4 '127.0.0.1' '127.0.0.1'
ip4 '127' '127.0.0.0'
ip6 '::ffff:127.' '::ffff:127.0.0.0' mixed
ip4 '10..1.' '10.0.1.0'
ip6 '::10..1.' '::10.0.1.0' mixed
ip4 '..' '0.0.0.0'
ip4 '~.f.b.9' '0.0.0.9'
ip4 '::' '0.0.0.0'


#
# DN hacking
#
fixdn "www.yahoo.com" "yahoo.com." super anchor
fixdn "yahoo.com" "com." super anchor
fixdn "com." "." super anchor
fixdn "com" "." super anchor
fixdn "." "" super anchor
fixdn ".net." "" anchor super chomp
fixdn "net." "" anchor super chomp
fixdn "." "" anchor super chomp
fixdn "..f..." "f" chomp
fixdn "www.25thandClement.com" "www.25thandClement.com." anchor trunc=24
fixdn "www.25thandClement.com" "25thandClement.com." anchor trunc=23
fixdn "www.25thandClement.com" "www.25thandClement.com" trunc=23
fixdn "www.25thandClement.com." "www.25thandClement.com" chomp trunc=23
fixdn "aa.bb.cc.dd.ee" "aa.bb.cc.dd.ee." anchor trunc=16
fixdn "aa.bb.cc.dd.ee" "bb.cc.dd.ee." anchor trunc=15
fixdn "com.." "com" chomp


#
# Phew!
#
say "GOOD JOB!!!\n"

exit 0

