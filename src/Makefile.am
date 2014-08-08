# Jerasure AM file

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS = $(SIMD_FLAGS)

lib_LTLIBRARIES = libJerasure.la
libJerasure_la_SOURCES = galois.c jerasure.c reed_sol.c cauchy.c liberation.c
libJerasure_la_LDFLAGS = -version-info 2:0:0
libJerasure_la_LIBADD = -lgf_complete
include_HEADERS = ../include/jerasure.h

# Install additional Jerasure header files in their own directory.
jerasureincludedir = $(includedir)/jerasure
jerasureinclude_HEADERS = \
  ../include/cauchy.h \
  ../include/galois.h \
  ../include/liberation.h \
  ../include/reed_sol.h

noinst_HEADERS = ../include/timing.h
noinst_LIBRARIES = libtiming.a
libtiming_a_SOURCES = timing.c
