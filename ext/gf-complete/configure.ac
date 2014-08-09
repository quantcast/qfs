# gf-complete autoconf template

# FIXME - add project url as the last argument
AC_INIT(gf-complete, 1.0)

AC_PREREQ([2.61])

AM_INIT_AUTOMAKE([no-dependencies], [foreign])
LT_INIT # libtool

AC_CONFIG_HEADER(include/config.h)

dnl Needed when reconfiguring with 'autoreconf -i -s'
AC_CONFIG_MACRO_DIR([m4])

# This prevents './configure; make' from trying to run autotools.
AM_MAINTAINER_MODE([disable])

# Override default CFLAGS
CFLAGS="-Wall -Wpointer-arith -O3 -g"

dnl Compiling with per-target flags requires AM_PROG_CC_C_O.
AC_PROG_CC

AC_CHECK_SIZEOF(long)

AX_EXT()

AC_CONFIG_FILES([Makefile src/Makefile tools/Makefile test/Makefile examples/Makefile])
AC_OUTPUT
