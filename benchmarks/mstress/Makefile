# $Id$
#
# Author: Thilee Subramaniam
#
# Copyright 2012 Quantcast Corp.
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


UNAME := $(shell uname)
CC = g++
KFS_BUILD_INCLUDE?=../../build/include
KFS_BUILD_STATLIB?=../../build/lib/static
INCLUDES = -I. -I$(KFS_BUILD_INCLUDE)/kfs
CCFLAGS = -g -O2 $(INCLUDES)
LIBS = -lkfs_tools -lkfs_client -lkfs_qcdio -lpthread -lkfs_io -lkfs_common -lkfs_qcdio -lpthread -lz -lcrypto -lkfs_qcrs -lboost_regex
ifneq ($(UNAME), Darwin)
LIBS += -lrt
endif
LDFLAGS = -L$(KFS_BUILD_STATLIB) -L$(BOOST_LIBRARY_DIR) $(LIBS)

javaC = javac
javaR = java
javaCP = -cp

default: ccclient javaclient

ccclient: mstress_client.cc
	$(CC) $(CCFLAGS) mstress_client.cc $(LDFLAGS) -o mstress_client

javaclient: MStress_Client.java
	$(javaC) $(javaCP) $(shell echo mstress_hdfs_client_jars/*.jar | sed 's/ /:/g') MStress_Client.java

run_ccclient: ccclient
	./mstress_client -h

run_javaclient:	javaclient
	$(javaR) $(javaCP) .:$(shell echo mstress_hdfs_client_jars/*.jar | sed 's/ /:/g') MStress_Client -h

clean:
	rm -f *.o mstress_client *.class
	rm -rf *.dSYM

