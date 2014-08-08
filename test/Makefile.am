# GF-Complete 'test' AM file

INCLUDES=-I./ -I../include
AM_CFLAGS = -O3 $(SIMD_FLAGS) -fPIC $(INCLUDES)

bin_PROGRAMS = gf_unit 

gf_unit_SOURCES = gf_unit.c
#gf_unit_LDFLAGS = -lgf_complete
gf_unit_LDADD = ../src/libgf_complete.la

