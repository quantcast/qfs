# GF-Complete 'tools' AM file

INCLUDES=-I./ -I../include
AM_CFLAGS = -O3 $(SIMD_FLAGS) -fPIC $(INCLUDES)

TESTS=run-tests.sh

bin_PROGRAMS = gf_mult gf_div gf_add gf_time gf_methods gf_poly gf_inline_time

gf_mult_SOURCES = gf_mult.c
#gf_mult_LDFLAGS = -lgf_complete
gf_mult_LDADD = ../src/libgf_complete.la

gf_div_SOURCES = gf_div.c
#gf_div_LDFLAGS = -lgf_complete
gf_div_LDADD = ../src/libgf_complete.la

gf_add_SOURCES = gf_add.c
#gf_add_LDFLAGS = -lgf_complete
gf_add_LDADD = ../src/libgf_complete.la

gf_time_SOURCES = gf_time.c
#gf_time_LDFLAGS = -lgf_complete
gf_time_LDADD = ../src/libgf_complete.la

gf_methods_SOURCES = gf_methods.c
#gf_methods_LDFLAGS = -lgf_complete
gf_methods_LDADD = ../src/libgf_complete.la

gf_poly_SOURCES = gf_poly.c
#gf_poly_LDFLAGS = -lgf_complete
gf_poly_LDADD = ../src/libgf_complete.la

gf_inline_time_SOURCES = gf_inline_time.c
#gf_inline_time_LDFLAGS = -lgf_complete
gf_inline_time_LDADD = ../src/libgf_complete.la

