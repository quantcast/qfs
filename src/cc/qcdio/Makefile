#
# $Id:  $
#
# Created 2008/11/22
# Author: Mike Ovsiannikov
#
# Copyright 2008,2009 Quantcast Corp.
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
#

.SUFFIXES :
.SUFFIXES : .c .cpp .o .d

MKDIR   = mkdir
CP      = cp
RM      = rm -f
CC      = gcc
ifndef OS_NAME
    OS_NAME := $(shell uname |  tr -s '[:lower:]' '[:upper:]')
endif

CFLAGS_COMMON = \
    -D_GNU_SOURCE \
    -D_LARGEFILE_SOURCE \
    -D_LARGEFILE64_SOURCE \
    -D_FILE_OFFSET_BITS=64 \
    -DQC_OS_NAME_${OS_NAME}

CXXFLAGS_COMMON = ${CFLAGS_COMMON}

SRC_unittest = \
    QCDiskQueue.cc \
    QCFdPoll.cc \
    QCIoBufferPool.cc \
    QCMutex.cc \
    QCThread.cc \
    QCUtils.cc \
    qcunittest_main.cc

ifeq (${OS_NAME}, DARWIN)
    LIBS_unittest := ''
else
    LIBS_unittest := \
        -pthread \
        -lrt
endif

INCLUDE_unittest  =
CXXFLAGS_unittest = -Wall -pthread
CXXFLAGS_unittest_debug = ${CXXFLAGS_unittest} -g3 -O0
LDFLAGS_unittest_debug = -g3

SRC_iovperf = iovperf.c
CFLAGS_iovperf_debug  = -Wall -g3 -O0
LDFLAGS_iovperf_debug = -Wall -g3

INCLUDE := ${INCLUDE_${PROGRAM}}
OBJ     := $(addsuffix .o, $(basename ${SRC_${PROGRAM}}))
LIBS    := ${LIBS_${PROGRAM}}
DEPEND  := ${OBJ:.o=.d}

TO_CLEAN = \
	unittest_debug \
	iovperf_debug

all:
	${MAKE} PROGRAM=unittest BLDTYPE=debug debug
	${MAKE} PROGRAM=iovperf  BLDTYPE=debug debug

${BLDTYPE}: blddirs
	cd build/$@ && \
	${MAKE} -f ../../Makefile \
		CXXFLAGS='${CXXFLAGS_${PROGRAM}_$@} -DQC_BUILD_TYPE_$@ ${CXXFLAGS_COMMON}' \
		CFLAGS='${CFLAGS_${PROGRAM}_$@} -DQC_BUILD_TYPE_$@ ${CFLAGS_COMMON}' \
		LDFLAGS='${LDFLAGS_${PROGRAM}_$@}' \
		VPATH=../../ \
		${PROGRAM}_$@
	${CP} build/$@/${PROGRAM}_$@ ${PROGRAM}_$@

blddirs:
	${MKDIR} -p build/${BLDTYPE}

${PROGRAM}_${BLDTYPE}: $(OBJ)
	@ echo Linking $@
	$(CXX) $(LDFLAGS) $(OBJ) $(LIBS) -o $@ 

%.o: %.cc
	$(COMPILE.cpp) $(INCLUDE) -MMD -o $@ $<

%.o: %.c
	$(COMPILE.c) $(INCLUDE) -MMD -o $@ $<

clean:
	${RM} -r build
	${RM} ${TO_CLEAN}

-include $(DEPEND)
