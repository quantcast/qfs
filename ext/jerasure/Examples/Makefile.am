# Jerasure AM file

AM_CPPFLAGS = -I$(top_srcdir)/include
AM_CFLAGS = $(SIMD_FLAGS)

bin_PROGRAMS = jerasure_01 \
               jerasure_02 \
               jerasure_03 \
               jerasure_04 \
               jerasure_05 \
               jerasure_06 \
               jerasure_07 \
               jerasure_08 \
               reed_sol_01 \
               reed_sol_02 \
               reed_sol_03 \
               reed_sol_04 \
               reed_sol_test_gf \
               reed_sol_time_gf \
               cauchy_01 \
               cauchy_02 \
               cauchy_03 \
               cauchy_04 \
               liberation_01 \
               encoder \
               decoder

TESTS=test_all_gfs.sh

dist_noinst_SCRIPTS = test_all_gfs.sh time_all_gfs_argv_init.sh

jerasure_01_SOURCES = jerasure_01.c
jerasure_02_SOURCES = jerasure_02.c
jerasure_03_SOURCES = jerasure_03.c
jerasure_04_SOURCES = jerasure_04.c
jerasure_05_SOURCES = jerasure_05.c
jerasure_06_SOURCES = jerasure_06.c
jerasure_07_SOURCES = jerasure_07.c
jerasure_08_SOURCES = jerasure_08.c

reed_sol_01_SOURCES = reed_sol_01.c
reed_sol_02_SOURCES = reed_sol_02.c
reed_sol_03_SOURCES = reed_sol_03.c
reed_sol_04_SOURCES = reed_sol_04.c

reed_sol_test_gf_SOURCES = reed_sol_test_gf.c
reed_sol_time_gf_SOURCES = reed_sol_time_gf.c

cauchy_01_SOURCES = cauchy_01.c
cauchy_02_SOURCES = cauchy_02.c
cauchy_03_SOURCES = cauchy_03.c
cauchy_04_SOURCES = cauchy_04.c

liberation_01_SOURCES = liberation_01.c

decoder_SOURCES = decoder.c
encoder_SOURCES = encoder.c

LDADD = ../src/libJerasure.la
decoder_LDADD = $(LDADD) ../src/libtiming.a
encoder_LDADD = $(LDADD) ../src/libtiming.a
reed_sol_time_gf_LDADD = $(LDADD) ../src/libtiming.a
