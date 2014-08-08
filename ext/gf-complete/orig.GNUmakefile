#
# GNUmakefile for Galois field library
#
# The default flags do *not* have the SSE instructions enabled.
# Please cd to flag_tester and run which_compile_flags.sh to see which SSE instructions
# your machine and compiler support, and which flags you should include below.

CFLAGS = -O3 
LDFLAGS = -O3 

SRCS = gf_w4.c gf_w8.c gf_w16.c gf_w32.c gf_w64.c gf_w128.c gf_wgen.c gf.c gf_unit.c \
       gf_time.c gf_mult.c gf_method.c gf_methods.c gf_div.c gf_rand.c gf_general.c \
       gf_poly.c gf_example_1.c gf_add.c gf_example_2.c gf_example_3.c gf_example_4.c \
       gf_inline_time.c gf_example_5.c gf_example_6.c gf_example_7.c

HDRS = gf_complete.h gf_int.h

EXECUTABLES = gf_mult gf_div gf_add gf_unit gf_time gf_methods gf_poly \
              gf_example_1 gf_example_2 gf_example_3 gf_example_4 gf_inline_time \
              gf_example_5 gf_example_6 gf_example_7

RM = /bin/rm -f

LIBOBJS = gf.o gf_method.o gf_wgen.o gf_w4.o gf_w8.o gf_w16.o gf_w32.o \
          gf_w64.o gf_w128.o gf_rand.o gf_general.o

OBJS = $(addsuffix .o, $(basename $(SRCS)))

DEFAULT = $(EXECUTABLES) gf_complete.a

default: $(DEFAULT)

all: $(OBJS)

gf_complete.a: $(LIBOBJS)
	ar ru gf_complete.a $(LIBOBJS)
	ranlib gf_complete.a

gf_methods: gf_methods.o gf_complete.a
gf_time: gf_time.o gf_complete.a
gf_inline_time: gf_inline_time.o gf_complete.a
gf_unit: gf_unit.o gf_complete.a
gf_example_1: gf_example_1.o gf_complete.a
gf_example_2: gf_example_2.o gf_complete.a
gf_example_3: gf_example_3.o gf_complete.a
gf_example_4: gf_example_4.o gf_complete.a
gf_example_5: gf_example_5.o gf_complete.a
gf_example_6: gf_example_6.o gf_complete.a
gf_example_7: gf_example_7.o gf_complete.a
gf_mult: gf_mult.o gf_complete.a
gf_div: gf_div.o gf_complete.a
gf_poly: gf_poly.o gf_complete.a
gf_add: gf_add.o

clean:
	$(RM) $(OBJS) gf_div.c

spotless: clean
	$(RM) *~ $(EXECUTABLES) which_compile_flags
	$(RM) gf_complete.a

gf_div.o: gf_complete.h gf_method.h
gf_methods.o: gf_complete.h gf_method.h
gf_time.o: gf_complete.h gf_method.h gf_rand.h gf_general.h
gf_inline_time.o: gf_complete.h gf_rand.h
gf_wgen.o: gf_int.h gf_complete.h
gf_w4.o: gf_int.h gf_complete.h
gf_w8.o: gf_int.h gf_complete.h
gf_w16.o: gf_int.h gf_complete.h
gf_w32.o: gf_int.h gf_complete.h
gf_w64.o: gf_int.h gf_complete.h
gf_unit.o: gf_complete.h gf_method.h gf_rand.h gf_general.h
gf_example_1.o: gf_complete.h gf_rand.h
gf_example_2.o: gf_complete.h gf_rand.h
gf_example_3.o: gf_complete.h gf_rand.h
gf_example_4.o: gf_complete.h gf_rand.h
gf_example_5.o: gf_complete.h gf_rand.h
gf_example_6.o: gf_complete.h gf_rand.h
gf_example_7.o: gf_complete.h gf_rand.h
gf_general.o: gf_complete.h gf_int.h gf_general.h gf_rand.h
gf_mult.o: gf_complete.h gf_method.h
gf.o: gf_complete.h gf_int.h
gf_method.o: gf_complete.h

gf_div.c: gf_mult.c
	sed -e 's/product/quotient/' -e 's/multiply/divide/g' -e 's/multiplication/division/' -e 's/mult/div/' gf_mult.c > gf_div.c
