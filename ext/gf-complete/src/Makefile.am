# GF-Complete 'core' AM file
# Creates the library

AUTOMAKE_OPTIONS = subdir-objects

AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include

# avoid using SIMD_FLAGS for code that calls strcmp as new gcc
# versions will use SIMD for the strcmp implementation. Instead
# we create a static library just for gf_method that is not compiled
# with SIMD_FLAGS, this static library will get linked into gf_complete.so
noinst_LTLIBRARIES = libgf_util.la
libgf_util_la_SOURCES = gf_method.c
libgf_util_la_CFLAGS = -O3 -fPIC -Wsign-compare

# we narrowly use SIMD_FLAGS for code that needs it
lib_LTLIBRARIES = libgf_complete.la
libgf_complete_la_SOURCES = gf.c gf_wgen.c gf_w4.c gf_w8.c gf_w16.c gf_w32.c \
          gf_w64.c gf_w128.c gf_rand.c gf_general.c gf_cpu.c
libgf_complete_la_CFLAGS = -O3 $(SIMD_FLAGS) -fPIC -Wsign-compare
libgf_complete_la_LIBADD = libgf_util.la

if HAVE_NEON
libgf_complete_la_SOURCES += neon/gf_w4_neon.c  \
                             neon/gf_w8_neon.c  \
                             neon/gf_w16_neon.c \
                             neon/gf_w32_neon.c \
                             neon/gf_w64_neon.c
endif

libgf_complete_la_LDFLAGS = -version-info 1:0:0

