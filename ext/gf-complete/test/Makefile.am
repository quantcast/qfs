# GF-Complete 'test' AM file

AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
AM_CFLAGS = -O3 -fPIC

bin_PROGRAMS = gf_unit 

gf_unit_SOURCES = gf_unit.c
#gf_unit_LDFLAGS = -lgf_complete
gf_unit_LDADD = ../src/libgf_complete.la

