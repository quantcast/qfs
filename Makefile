# $Id$
#
# Created 2012/07/27
# Author: Mike Ovsiannikov
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

ifeq ($(origin BUILD_TYPE),undefined)
ifeq ($(DEBUG),true)
BUILD_TYPE := debug
CMAKE_OPTIONS :=
else
BUILD_TYPE := release
CMAKE_OPTIONS := -D CMAKE_BUILD_TYPE=RelWithDebInfo
endif
endif

V=@
ifeq ($(VERBOSE),true)
V=
endif

.PHONY: all
all: build java

.PHONY: build
build:
	$Vmkdir -p build/${BUILD_TYPE}
	$Vcd build/${BUILD_TYPE} && cmake ${CMAKE_OPTIONS} ../..
	$Vcd build/${BUILD_TYPE} && $(MAKE) --no-print-directory install

.PHONY: java
java: build
	$V./src/java/javabuild.sh clean
	$V./src/java/javabuild.sh

.PHONY: hadoop-jars
hadoop-jars: build
	$V./src/java/javabuild.sh clean
	$Vif test -x "`which mvn 2>/dev/null`"; then \
		./src/java/javabuild.sh clean   && \
		./src/java/javabuild.sh 0.23.4  && \
		./src/java/javabuild.sh 0.23.11 && \
		./src/java/javabuild.sh 1.0.4   && \
		./src/java/javabuild.sh 1.1.2   && \
		./src/java/javabuild.sh 2.5.1      \
	; fi

.PHONY: tarball
tarball: hadoop-jars
	$Vcd build && \
	myuname=`uname -s`; \
	if [ x"$$myuname" = x'Linux' -a -f /etc/issue ]; then \
	    myflavor=`head -n 1 /etc/issue | cut -d' ' -f1` ; \
	    if [ x"$$myflavor" = x'Ubuntu' ]; then \
		myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f2`" ; \
	    elif [ x"$$myflavor" = x ]; then \
		myflavor=$$myuname ; \
	    else \
		myflavor="$$myflavor-`head -n 1 /etc/issue | cut -d' ' -f3`" ; \
	    fi ; \
	else \
	    if echo "$$myuname" | grep CYGWIN > /dev/null; then \
		myflavor=cygwin ; \
	    else \
		myflavor=$$myuname ; \
	    fi ; \
	fi ; \
	qfsversion=`../src/cc/common/buildversgit.sh -v 2> /dev/null | head -1` ; \
	tarname="qfs-$$myflavor-$$qfsversion-`uname -m`" ;\
	tarname=`echo "$$tarname" | tr A-Z a-z` ; \
	{ test -d tmpreldir || mkdir tmpreldir; } && \
	rm -rf "tmpreldir/$$tarname" && \
	mkdir "tmpreldir/$$tarname" && \
	cp -r release/bin release/lib release/include ../scripts ../webui \
	     ../examples ../benchmarks "tmpreldir/$$tarname/" && \
	if ls -1 ./java/qfs-access/qfs-access-*.jar > /dev/null 2>&1; then \
	    cp ./java/qfs-access/qfs-access*.jar "tmpreldir/$$tarname/lib/"; fi && \
	if ls -1 ./java/hadoop-qfs/hadoop-*.jar > /dev/null 2>&1; then \
	    cp ./java/hadoop-qfs/hadoop-*.jar "tmpreldir/$$tarname/lib/"; fi && \
	tar cvfz "$$tarname".tgz -C ./tmpreldir "$$tarname" && \
	rm -rf tmpreldir

.PHONY: python
python: build
	$Vcd build/${BUILD_TYPE} && python ../../src/cc/access/kfs_setup.py build

.PHONY: test
test: build
	$Vcd build/${BUILD_TYPE} && ../../src/test-scripts/qfstest.sh

.PHONY: clean
clean:
	$Vrm -rf build
