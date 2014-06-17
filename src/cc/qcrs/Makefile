#
# $Id$
#
# Created 2010/07/24
# Author: Dan Adkins
# Edited by Chao Tian (AT&T) 2014/3/25 
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
CFLAGS=-g -O3
ifeq ($(ARCH),i686)
   CFLAGS+=-mmmx -msse -DINTEL_SSE -msse2 -DINTEL_SSE2
else
   CFLAGS+=-mmmx -msse -DINTEL_SSE -msse2 -DINTEL_SSE2 -msse3 -DINTEL_SSE3 -mssse3 -DINTEL_SSSE3 -msse4.1 -DINTEL_SSE41  
endif

ifeq ($(ARCH),i686)
   LDFLAGS=-mmmx -msse -DINTEL_SSE -msse2 -DINTEL_SSE2
else
  LDFLAGS=-mmmx -msse -DINTEL_SSE -msse2 -DINTEL_SSE2 -msse3 -DINTEL_SSE3 -mssse3 -DINTEL_SSSE3 -msse4.1 -DINTEL_SSE41
endif


AR=ar
ARFLAGS=rsc

HFILES=\
	rs.h\
	cauchy.h\
	galois.h\
	jerasure.h\
	gf_init.h\
	gf_complete.h\

OFILES=\
	cauchy.o\
	galois.o\
	jerasure.o\
	gf.o\
	gf_w8.o\
#	gf_general.o\
#	gf_w4.o\
#	gf_wgen.o\

librs.a: $(OFILES)
	$(AR) $(ARFLAGS) librs.a $(OFILES)

rs_test_new: librs.a rs_test_main_new.o
	$(CC) -o $@ rs_test_main_new.o librs.a $(LDFLAGS) 

clean:
	rm -f *.o librs.a rs_test rs_test_new

