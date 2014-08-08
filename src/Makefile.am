# GF-Complete 'core' AM file
# Creates the library

INCLUDES=-I./ -I../include
AM_CFLAGS = -O3 $(SIMD_FLAGS) -fPIC $(INCLUDES)

lib_LTLIBRARIES = libgf_complete.la
libgf_complete_la_SOURCES = gf.c gf_method.c gf_wgen.c gf_w4.c gf_w8.c gf_w16.c gf_w32.c \
          gf_w64.c gf_w128.c gf_rand.c gf_general.c
libgf_complete_la_LDFLAGS = -version-info 1:0:0

