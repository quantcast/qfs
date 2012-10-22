#
# $Id$
#
# Created 2010/07/24
# Author: Dan Adkins
#
# Copyright 2010 Quantcast Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#

all: librs.a

CC=gcc
ARCH=$(shell uname -m)
CFLAGS=-Wall -g -O3
ifeq ($(ARCH),i686)
   CFLAGS+=-msse2 -DLIBRS_USE_SSE2
else
   CFLAGS+=-mssse3
endif
LDFLAGS=
AR=ar
ARFLAGS=rsc

HFILES=\
	rs.h\
	rs_table.h\
	prim.h\

OFILES=\
	decode.o\
	encode.o\
	rs_table.o\

librs.a: $(OFILES)
	$(AR) $(ARFLAGS) librs.a $(OFILES)

rs_test: librs.a rs_test_main.o
	$(CC) -o $@ rs_test_main.o librs.a $(LDFLAGS)

clean:
	rm -f *.o librs.a rs_test

test: rs_test
	./rs_test 6 16384
	./rs_test 64 64
